use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

#[allow(dead_code)]
pub struct ChunkedWriter {
    file: tokio::fs::File,
    part_path: PathBuf,
    final_path: PathBuf,
    bytes_written: u64,
    progress_path: PathBuf,
}

#[allow(dead_code)]
impl ChunkedWriter {
    pub async fn create(path: &Path) -> Result<Self, std::io::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let part_path = path.with_extension(format!(
            "{}.part",
            path.extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        ));

        let progress_path = path.with_extension(format!(
            "{}.drift-progress",
            path.extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        ));

        let (file, bytes_written) = if part_path.exists() {
            let metadata = tokio::fs::metadata(&part_path).await?;
            let file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&part_path)
                .await?;
            (file, metadata.len())
        } else {
            let file = tokio::fs::File::create(&part_path).await?;
            (file, 0)
        };

        Ok(Self {
            file,
            part_path,
            final_path: path.to_path_buf(),
            bytes_written,
            progress_path,
        })
    }

    /// Create a writer that resumes from a confirmed offset.
    /// If the existing .part file is larger than the confirmed offset, it is truncated.
    pub async fn create_with_resume(path: &Path, confirmed_offset: u64) -> Result<Self, std::io::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let part_path = path.with_extension(format!(
            "{}.part",
            path.extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        ));

        let progress_path = path.with_extension(format!(
            "{}.drift-progress",
            path.extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        ));

        let (file, bytes_written) = if part_path.exists() && confirmed_offset > 0 {
            let metadata = tokio::fs::metadata(&part_path).await?;
            if metadata.len() > confirmed_offset {
                // Truncate to confirmed offset
                let f = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&part_path)
                    .await?;
                f.set_len(confirmed_offset).await?;
                drop(f);
            }
            let file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&part_path)
                .await?;
            (file, confirmed_offset)
        } else if confirmed_offset == 0 {
            // Fresh start
            let file = tokio::fs::File::create(&part_path).await?;
            (file, 0)
        } else {
            let file = tokio::fs::File::create(&part_path).await?;
            (file, 0)
        };

        Ok(Self {
            file,
            part_path,
            final_path: path.to_path_buf(),
            bytes_written,
            progress_path,
        })
    }

    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        self.file.write_all(data).await?;
        self.bytes_written += data.len() as u64;

        // Write progress metadata every 1MB
        if self.bytes_written % (1024 * 1024) < data.len() as u64 {
            self.write_progress().await.ok();
        }

        Ok(())
    }

    /// Persist current progress to the .drift-progress file.
    async fn write_progress(&self) -> Result<(), std::io::Error> {
        let content = format!("{}", self.bytes_written);
        tokio::fs::write(&self.progress_path, content.as_bytes()).await
    }

    pub async fn finalize(mut self) -> Result<(), std::io::Error> {
        self.file.flush().await?;
        drop(self.file);
        tokio::fs::rename(&self.part_path, &self.final_path).await?;
        // Clean up progress file on successful completion
        let _ = tokio::fs::remove_file(&self.progress_path).await;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Check how many bytes have already been written for resume support.
    /// Returns the size of the .part file if it exists.
    pub async fn resume_offset(path: &Path) -> u64 {
        let part_path = path.with_extension(format!(
            "{}.part",
            path.extension()
                .map(|e| e.to_string_lossy().to_string())
                .unwrap_or_default()
        ));
        tokio::fs::metadata(&part_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0)
    }
}
