use futures::stream::{Stream, StreamExt};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{self, ReadDir};
use tokio_stream::wrappers::ReadDirStream;

/// State of traversing a directory tree.
/// There are various state:
/// Next: The next directory to traverse it will try to pop the next directory from the directories queue and move to the next state Prepare
/// Prepare: Prepare the current stream that will be used to read the directory entries.
/// Reading: Actually reading the current directory using the current stream that obtained previously.
enum State {
    Next,
    Prepare(Pin<Box<dyn Future<Output = Result<ReadDir, std::io::Error>>>>),
    Reading(ReadDirStream),
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Next => write!(f, "NextDir"),
            State::Prepare(_) => write!(f, "PrepareDir"),
            State::Reading(_) => write!(f, "ReadingDir"),
        }
    }
}

pub struct RecursiveFileStream {
    dirs: VecDeque<PathBuf>,
    state: State,
}

impl RecursiveFileStream {
    pub fn new<P: AsRef<Path>>(root: &P) -> Self {
        let mut dirs = VecDeque::new();
        dirs.push_back(root.as_ref().to_path_buf());
        RecursiveFileStream {
            dirs,
            state: State::Next,
        }
    }
}

impl Stream for RecursiveFileStream {
    type Item = Result<PathBuf, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                State::Next => {
                    if let Some(dir) = this.dirs.pop_front() {
                        // Prepare the next directory to read
                        let read_dir = fs::read_dir(dir.clone());
                        this.state = State::Prepare(Box::pin(read_dir));
                        // produce the directory path, before producing inner files
                        return Poll::Ready(Some(Ok(dir)));
                    } else {
                        // No more directories to traverse
                        return Poll::Ready(None);
                    }
                }
                State::Prepare(read_dir) => match read_dir.as_mut().poll(cx) {
                    Poll::Ready(Ok(read_dir)) => {
                        this.state = State::Reading(ReadDirStream::new(read_dir));
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = State::Next;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                State::Reading(current_stream) => {
                    match futures::ready!(current_stream.poll_next_unpin(cx)) {
                        Some(Ok(entry)) => {
                            if entry.path().is_dir() {
                                // Push the directory to the queue if it is a directory
                                this.dirs.push_back(entry.path());
                            } else {
                                // Produce the file path if it is a file
                                return Poll::Ready(Some(Ok(entry.path())));
                            }
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // No more entries in the current directory
                            this.state = State::Next;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use tempfile::tempdir;
    use tokio::fs::{self, File};

    #[tokio::test]
    async fn test_empty_directory() {
        let dir = tempdir().unwrap();
        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream
            .filter(|f| {
                let result = f.as_ref().ok().unwrap().is_file();
                async move { result }
            })
            .collect()
            .await;

        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_single_file() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("file1.txt")).await.unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            1
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
    }

    /*
    test single directory without skipping producing directories
    */
    #[tokio::test]
    async fn test_single_directory() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).await.unwrap();
        File::create(subdir.join("file1.txt")).await.unwrap();
        File::create(subdir.join("file2.txt")).await.unwrap();

        let stream = RecursiveFileStream::new(&dir);
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            2
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("subdir")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file2.txt")));
    }

    #[tokio::test]
    async fn test_nested_directories() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).await.unwrap();
        File::create(subdir.join("file2.txt")).await.unwrap();
        File::create(subdir.join("file1.txt")).await.unwrap();

        let stream = RecursiveFileStream::new(&subdir);
        let files: Vec<_> = stream.collect().await;

        for file in &files {
            println!("file: {:?}", file.as_ref().unwrap().to_str());
        }
        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            2
        );

        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file2.txt")));
    }

    #[tokio::test]
    async fn test_multiple_directories() {
        let dir = tempdir().unwrap();
        let subdir1 = dir.path().join("subdir1");
        let subdir2 = dir.path().join("subdir2");
        fs::create_dir(&subdir1).await.unwrap();
        fs::create_dir(&subdir2).await.unwrap();
        File::create(subdir1.join("file1.txt")).await.unwrap();
        File::create(subdir2.join("file2.txt")).await.unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            2
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file2.txt")));
    }

    #[tokio::test]
    async fn test_multiple_sub_directories() {
        let dir = tempdir().unwrap();
        let subdir1 = dir.path().join("subdir1");
        let subdir2 = subdir1.join("subdir2");
        fs::create_dir(&subdir1).await.unwrap();
        fs::create_dir(&subdir2).await.unwrap();

        let mut stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<PathBuf> = vec!["file1.txt", "file2.txt"]
            .into_iter()
            .map(|f| PathBuf::from(f))
            .collect();
        let mut files_iter = files.iter();

        File::create(subdir1.join(files_iter.next().unwrap()))
            .await
            .unwrap();
        File::create(subdir2.join(files_iter.next().unwrap()))
            .await
            .unwrap();

        let mut files_iter = files.iter();
        while let Some(file) = stream.next().await {
            let file = file.unwrap();
            if file.is_dir() {
                continue;
            }
            let expected = files_iter.next().unwrap();
            assert!(file.ends_with(expected));
        }
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_directory_with_hidden_files() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("file1.txt")).await.unwrap();
        File::create(dir.path().join(".hidden_file")).await.unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            2
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
    }

    #[tokio::test]
    async fn test_directory_with_symlinks() {
        let dir = tempdir().unwrap();
        let symlink_path = dir.path().join("symlink");
        let target_path = dir.path().join("target.txt");
        File::create(&target_path).await.unwrap();
        std::os::unix::fs::symlink(&target_path, &symlink_path).unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            2
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("symlink")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("target.txt")));
    }

    #[tokio::test]
    async fn test_directory_with_nested_symlinks() {
        let dir = tempdir().unwrap();
        let symlink1_path = dir.path().join("symlink1");
        let symlink2_path = dir.path().join("symlink2");
        let target_path = dir.path().join("target.txt");
        File::create(&target_path).await.unwrap();
        std::os::unix::fs::symlink(&target_path, &symlink1_path).unwrap();
        std::os::unix::fs::symlink(&symlink1_path, &symlink2_path).unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream.collect().await;

        assert_eq!(
            files
                .iter()
                .filter(|f| f.as_ref().ok().unwrap().is_file())
                .count(),
            3
        );
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("symlink1")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("symlink2")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("target.txt")));
    }

    // test filter by file extension
    #[tokio::test]
    async fn test_filter_by_extension() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("file1.txt")).await.unwrap();
        File::create(dir.path().join("file2.jpg")).await.unwrap();
        File::create(dir.path().join("file3.txt")).await.unwrap();

        let stream = RecursiveFileStream::new(&dir.path());
        let files: Vec<_> = stream
            .filter(|f| {
                let f = f.as_ref().ok().unwrap();
                let result =
                    f.is_file() && f.extension().is_some() && f.extension().unwrap() == "txt";
                async move { result }
            })
            .collect()
            .await;

        assert_eq!(files.len(), 2);
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file1.txt")));
        assert!(files
            .iter()
            .any(|res| res.as_ref().unwrap().ends_with("file3.txt")));
    }
}
