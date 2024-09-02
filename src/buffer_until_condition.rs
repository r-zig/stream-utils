use core::pin::pin;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;
use tracing::debug;

#[pin_project]
pub struct BufferUntilCondition<S: Stream, F: Fn(&S::Item) -> bool> {
    #[pin]
    stream: S,
    condition: F,
    buffer: VecDeque<S::Item>,
}

impl<S, F> BufferUntilCondition<S, F>
where
    S: Stream + Unpin + Send + 'static,
    F: Fn(&S::Item) -> bool + Send + 'static,
{
    pub fn new(stream: S, condition: F) -> Self {
        BufferUntilCondition {
            stream,
            condition,
            buffer: VecDeque::new(),
        }
    }
}

impl<S, F> Stream for BufferUntilCondition<S, F>
where
    S: Stream + Unpin,
    F: Fn(&S::Item) -> bool,
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Project the pin to the inner fields
        let mut this = self.project();

        while let Poll::Ready(item) = Pin::new(&mut this.stream).poll_next(cx) {
            match item {
                Some(item) => {
                    debug!("poll from inner stream item");
                    if (this.condition)(&item) {
                        // Condition met, produce buffered items, if exits
                        if this.buffer.is_empty() {
                            debug!("condition met with 0 items in buffer. continue loop");
                            continue;
                        }
                        let result = this.buffer.drain(..).collect::<Vec<_>>();
                        this.buffer.push_back(item);

                        debug!("condition met! Poll::Ready: {:?} items", result.len());
                        return Poll::Ready(Some(result));
                    } else {
                        // Buffer the item
                        this.buffer.push_back(item);
                        debug!(
                            "buffer with length of: {:?} items after added current item",
                            this.buffer.len()
                        );
                    }
                }
                None => {
                    // Stream is exhausted, flush the remaining buffer
                    debug!(
                        "finish internal stream this.buffer length: {:?} items",
                        this.buffer.len()
                    );
                    if !this.buffer.is_empty() {
                        let result = this.buffer.drain(..).collect();
                        return Poll::Ready(Some(result));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::folder_scanner::RecursiveFileStream;

    use super::*;
    use tempfile::tempdir;
    use tokio::fs::{self, File};
    use tokio_stream::StreamExt;

    use tracing_subscriber;

    pub fn init_test_logging() {
        // Set RUST_LOG environment variable programmatically
        std::env::set_var("RUST_LOG", "debug"); // or "info", "warn", etc.

        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer() // Ensures output is captured by test framework
            .try_init();
    }

    #[tokio::test]
    async fn test_conditional_item() {
        let stream = tokio_stream::iter(vec![1, 2, 3, 4, 5]);
        let mut buffer_stream = BufferUntilCondition::new(stream, |&x| x > 3);

        let mut idx = 0;
        while let Some(item) = buffer_stream.next().await {
            idx += 1;
            if idx == 1 {
                assert_eq!(item, vec![1, 2, 3]);
            } else if idx == 2 {
                assert_eq!(item, vec![4]);
            } else if idx == 3 {
                assert_eq!(item, vec![5]);
            }
        }
        assert_eq!(idx, 3);
    }

    #[tokio::test]
    async fn test_multiple_sub_directories() {
        init_test_logging();

        let dir = tempdir().unwrap();

        // create multiple subdirectories with multiple files in each
        let max_dirs = 2;
        let max_files_per_dir = 2;
        for i in 0..max_dirs {
            let subdir = dir.path().join(format!("subdir{}", i));
            fs::create_dir(&subdir).await.unwrap();
            for j in 0..max_files_per_dir {
                let file = subdir.join(format!("file{}.txt", j));
                File::create(&file).await.unwrap();
            }
        }

        let expected_files: Vec<PathBuf> = (0..max_files_per_dir)
            .map(|j: usize| PathBuf::from(format!("file{}.txt", j)))
            .collect();
        let stream_files = RecursiveFileStream::new(&dir.path());
        // run the stream and group files into directories
        let mut files_groups =
            BufferUntilCondition::new(stream_files, |path| path.as_ref().unwrap().is_dir());

        while let Some(files) = files_groups.next().await {
            // assert that the number of files in each directory is less than or equal to max_files_per_dir
            assert!(
                files.len() <= max_files_per_dir + 1,
                "files len: {} > max_files_per_dir + 1 (the dir itself): {}",
                files.len(),
                max_files_per_dir
            );

            // assert that the files in the directory are the expected files
            files.iter().for_each(|file| {
                let file = file.as_ref().unwrap();
                if file.is_dir() {
                    return;
                }
                assert!(
                    expected_files
                        .iter()
                        .any(|expected| file.ends_with(expected)),
                    "file {:?} not found in expected files",
                    file
                );
            });
        }
    }
}
