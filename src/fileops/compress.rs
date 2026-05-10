use flate2::Compression;
use flate2::write::GzEncoder;
use std::path::{Path, PathBuf};
use tar::Builder;
use walkdir::WalkDir;

/// Extensions that are already compressed and gain nothing from gzip.
const INCOMPRESSIBLE_EXTENSIONS: &[&str] = &[
    "gz", "zip", "tar", "bz2", "xz", "zst", "7z", "rar", "jpg", "jpeg", "png", "gif", "webp",
    "mp4", "mkv", "avi", "mp3", "flac", "dmg", "iso", "whl", "deb", "rpm",
];

/// Check if a file is already compressed based on its extension.
pub fn should_compress(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    !INCOMPRESSIBLE_EXTENSIONS.contains(&ext.as_str())
}

/// Determine whether a directory should use gzip compression.
/// Returns false if >80% of total file size consists of incompressible files.
fn directory_needs_gzip(source: &Path) -> bool {
    let mut total_size: u64 = 0;
    let mut incompressible_size: u64 = 0;

    for entry in WalkDir::new(source).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            total_size += size;
            if !should_compress(entry.path()) {
                incompressible_size += size;
            }
        }
    }

    if total_size == 0 {
        return true;
    }

    // If >80% is already compressed, skip gzip
    let ratio = incompressible_size as f64 / total_size as f64;
    ratio <= 0.8
}

/// Compress a directory into a .tar.gz file inside the .drift temp directory.
/// Returns (archive_path, archive_size).
pub fn compress_directory(
    root_dir: &Path,
    relative_path: &str,
) -> Result<(PathBuf, u64), CompressError> {
    compress_directory_with_mode(root_dir, relative_path, None)
}

/// Compress a directory with an explicit compression mode.
/// `compression`: None = auto-detect, Some("none") = tar only, Some("gzip") = tar.gz
pub fn compress_directory_with_mode(
    root_dir: &Path,
    relative_path: &str,
    compression: Option<&str>,
) -> Result<(PathBuf, u64), CompressError> {
    let source = root_dir.join(relative_path);
    let source = source
        .canonicalize()
        .map_err(|e| CompressError::Io(format!("Failed to resolve path: {}", e)))?;

    if !source.is_dir() {
        return Err(CompressError::NotADirectory);
    }

    // Decide whether to use gzip
    let use_gzip = match compression {
        Some("none") => false,
        Some("gzip") => true,
        _ => directory_needs_gzip(&source), // auto-detect
    };

    // Create .drift temp directory
    let drift_dir = root_dir.join(".drift");
    std::fs::create_dir_all(&drift_dir)
        .map_err(|e| CompressError::Io(format!("Failed to create .drift dir: {}", e)))?;

    let dir_name = source
        .file_name()
        .ok_or_else(|| CompressError::Io("Invalid directory path".to_string()))?;

    let (archive_path, size) = if use_gzip {
        let archive_name = format!("{}.tar.gz", relative_path.replace('/', "_"));
        let archive_path = drift_dir.join(&archive_name);

        let file = std::fs::File::create(&archive_path)
            .map_err(|e| CompressError::Io(format!("Failed to create archive: {}", e)))?;

        let encoder = GzEncoder::new(file, Compression::fast());
        let mut archive = Builder::new(encoder);

        archive
            .append_dir_all(dir_name, &source)
            .map_err(|e| CompressError::Io(format!("Failed to archive directory: {}", e)))?;

        let encoder = archive
            .into_inner()
            .map_err(|e| CompressError::Io(format!("Failed to finalize archive: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| CompressError::Io(format!("Failed to finish compression: {}", e)))?;

        let size = std::fs::metadata(&archive_path)
            .map_err(|e| CompressError::Io(format!("Failed to read archive size: {}", e)))?
            .len();

        (archive_path, size)
    } else {
        // Tar only, no gzip
        let archive_name = format!("{}.tar", relative_path.replace('/', "_"));
        let archive_path = drift_dir.join(&archive_name);

        let file = std::fs::File::create(&archive_path)
            .map_err(|e| CompressError::Io(format!("Failed to create archive: {}", e)))?;

        let mut archive = Builder::new(file);

        archive
            .append_dir_all(dir_name, &source)
            .map_err(|e| CompressError::Io(format!("Failed to archive directory: {}", e)))?;

        archive
            .into_inner()
            .map_err(|e| CompressError::Io(format!("Failed to finalize archive: {}", e)))?;

        let size = std::fs::metadata(&archive_path)
            .map_err(|e| CompressError::Io(format!("Failed to read archive size: {}", e)))?
            .len();

        (archive_path, size)
    };

    tracing::info!(
        "Compressed {} -> {} ({} bytes, gzip={})",
        relative_path,
        archive_path.display(),
        size,
        use_gzip,
    );

    Ok((archive_path, size))
}

/// Clean up a temp archive file
pub fn cleanup_archive(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        tracing::warn!("Failed to clean up archive {}: {}", path.display(), e);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error("not a directory")]
    NotADirectory,
    #[error("IO error: {0}")]
    Io(String),
}
