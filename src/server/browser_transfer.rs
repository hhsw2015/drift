use axum::extract::ws::Message;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::fileops::compress;
use crate::fileops::reader::ChunkedReader;
use crate::protocol::codec::{encode_control_frame, encode_data_frame_v1, encode_data_frame_v2};
use crate::protocol::messages::{ControlMessage, Direction, TransferEntry};
use crate::server::{AppState, FrameChannel};

/// Maximum number of concurrent file readers for parallel transfer.
const MAX_PARALLEL_READERS: usize = 4;

/// Validate that a relative path resolves to a location within root_dir.
/// Returns the validated path on success, or an error message on failure.
fn validate_path(root_dir: &Path, relative_path: &str) -> Result<PathBuf, String> {
    let target = root_dir.join(relative_path);
    match target.canonicalize() {
        Ok(canonical) => {
            let root_canonical = root_dir
                .canonicalize()
                .map_err(|e| format!("Invalid root: {}", e))?;
            if canonical.starts_with(&root_canonical) {
                Ok(canonical)
            } else {
                Err("Path traversal attempt blocked".to_string())
            }
        }
        // Path doesn't exist yet (for writes) - validate parent
        Err(_) => {
            if let Some(parent) = target.parent() {
                if parent.exists() {
                    let parent_canonical = parent
                        .canonicalize()
                        .map_err(|e| format!("Invalid parent: {}", e))?;
                    let root_canonical = root_dir
                        .canonicalize()
                        .map_err(|e| format!("Invalid root: {}", e))?;
                    if parent_canonical.starts_with(&root_canonical) {
                        return Ok(target);
                    }
                }
            }
            Err("Invalid path".to_string())
        }
    }
}

#[allow(dead_code)]
pub async fn handle_browser_transfer(
    state: Arc<AppState>,
    id: Uuid,
    entries: Vec<TransferEntry>,
    direction: Direction,
    destination_path: String,
    ws_tx: mpsc::UnboundedSender<Message>,
) {
    handle_browser_transfer_with_resume(
        state,
        id,
        entries,
        direction,
        destination_path,
        HashMap::new(),
        ws_tx,
    )
    .await;
}

pub async fn handle_browser_transfer_with_resume(
    state: Arc<AppState>,
    id: Uuid,
    entries: Vec<TransferEntry>,
    direction: Direction,
    destination_path: String,
    resume_hints: HashMap<String, u64>,
    ws_tx: mpsc::UnboundedSender<Message>,
) {
    tracing::info!(
        "Browser transfer request: id={}, entries={}, direction={:?}, dest={}, resume_hints={}",
        id,
        entries.len(),
        direction,
        destination_path,
        resume_hints.len(),
    );

    let remote = state.remote.read().await;
    if remote.is_none() {
        send_error(&ws_tx, id, "No remote connection");
        return;
    }

    // For Pull: register the local receiver BEFORE forwarding the request.
    // Binary frames from the remote can arrive before TransferAccepted is processed,
    // so the receiver must be ready or chunks would be silently dropped (and lost).
    let pull_done_rx = if direction == Direction::Pull {
        match state
            .transfer_receiver
            .start_transfer_with_notify(id, entries.clone(), destination_path.clone())
            .await
        {
            Ok(rx) => Some(rx),
            Err(e) => {
                send_error(&ws_tx, id, &e);
                return;
            }
        }
    } else {
        None
    };

    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let request_msg = ControlMessage::TransferRequest {
        id,
        entries: entries.clone(),
        direction: direction.clone(),
        destination_path,
        resume_hints: resume_hints.clone(),
    };

    let remote_conn = remote.as_ref().expect("checked above");
    crate::server::register_pending_response(
        &remote_conn.pending_requests,
        &remote_conn.pending_request_order,
        id,
        response_tx,
        remote_conn.peer_version,
        false,
    )
    .await;
    if remote_conn.tx.send(request_msg).is_err() {
        let _ = crate::server::remove_pending_response(
            &remote_conn.pending_requests,
            &remote_conn.pending_request_order,
            id,
        )
        .await;
        send_error(&ws_tx, id, "Failed to send to remote");
        return;
    }
    let (pending_requests, pending_request_order) = (
        remote_conn.pending_requests.clone(),
        remote_conn.pending_request_order.clone(),
    );
    drop(remote);

    // 60s timeout: TransferAccepted may be delayed if data from a prior transfer
    // is still draining through the shared frame channel.
    match tokio::time::timeout(std::time::Duration::from_secs(60), response_rx).await {
        Ok(Ok(ControlMessage::TransferAccepted { .. })) => {
            tracing::info!("Remote accepted transfer, starting file send");

            let _ = ws_tx.send(Message::Text(
                serde_json::to_string(&ControlMessage::TransferAccepted {
                    id,
                    resume_offsets: std::collections::HashMap::new(),
                })
                .unwrap()
                .into(),
            ));

            match direction {
                Direction::Push => {
                    if !entries.is_empty() {
                        push_entries(&state, id, &entries, &ws_tx).await;
                    }
                }
                Direction::Pull => {
                    let done_rx = pull_done_rx.expect("pull_done_rx set above for Pull");

                    let wait_result = tokio::time::timeout(std::time::Duration::from_secs(1800), done_rx).await;
                    match wait_result {
                        Err(_) => send_error(&ws_tx, id, "Pull transfer timed out"),
                        Ok(Err(_recv_err)) => {
                            send_error(&ws_tx, id, "Pull transfer channel closed unexpectedly")
                        }
                        Ok(Ok(Err(error))) => {
                            send_error(&ws_tx, id, &error);
                        }
                        Ok(Ok(Ok(total_bytes))) => {
                            tracing::info!("Pull transfer complete: {}", id);
                            let _ = ws_tx.send(Message::Text(
                                serde_json::to_string(&ControlMessage::TransferComplete {
                                    id,
                                    total_bytes,
                                })
                                .unwrap()
                                .into(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(Ok(ControlMessage::TransferError { error, .. })) => send_error(&ws_tx, id, &error),
        Ok(Ok(_)) => send_error(&ws_tx, id, "Unexpected response from remote"),
        Ok(Err(_)) => send_error(&ws_tx, id, "Remote response channel closed"),
        Err(_) => {
            let _ = crate::server::remove_pending_response(
                &pending_requests,
                &pending_request_order,
                id,
            )
            .await;
            send_error(&ws_tx, id, "Remote response timeout");
        }
    }
}

/// Read files from `root_dir` and stream them to the remote via the unified frame channel.
/// Used by both Push (local -> remote) and Pull (remote reads and sends back to requester).
#[allow(dead_code)]
pub async fn send_entries(
    root_dir: &std::path::Path,
    id: Uuid,
    entries: &[TransferEntry],
    frame_tx: &FrameChannel,
    peer_version: Option<u32>,
) {
    send_entries_with_resume(root_dir, id, entries, frame_tx, peer_version, &HashMap::new()).await;
}

/// Like `send_entries` but supports resume offsets from a prior partial transfer.
pub async fn send_entries_with_resume(
    root_dir: &std::path::Path,
    id: Uuid,
    entries: &[TransferEntry],
    frame_tx: &FrameChannel,
    peer_version: Option<u32>,
    resume_offsets: &HashMap<String, u64>,
) {
    let mut files_to_send: Vec<(String, PathBuf, u64, Option<PathBuf>, u64)> = Vec::new();

    for entry in entries {
        // Validate path before any file operations
        if let Err(e) = validate_path(root_dir, &entry.relative_path) {
            send_control(
                frame_tx,
                &ControlMessage::TransferError {
                    id,
                    error: format!("Invalid path {}: {}", entry.relative_path, e),
                },
            );
            cleanup_archives_v2(&files_to_send);
            return;
        }

        let resume_offset = resume_offsets
            .get(&entry.relative_path)
            .copied()
            .unwrap_or(0);

        if entry.is_dir {
            // Use adaptive compression based on the entry's compression field
            let compression_mode = entry.compression.as_deref();
            match compress::compress_directory_with_mode(root_dir, &entry.relative_path, compression_mode) {
                Ok((archive_path, archive_size)) => {
                    files_to_send.push((
                        entry.relative_path.clone(),
                        archive_path.clone(),
                        archive_size,
                        Some(archive_path),
                        resume_offset,
                    ));
                }
                Err(e) => {
                    send_control(
                        frame_tx,
                        &ControlMessage::TransferError {
                            id,
                            error: format!("Failed to compress {}: {}", entry.relative_path, e),
                        },
                    );
                    cleanup_archives_v2(&files_to_send);
                    return;
                }
            }
        } else {
            let file_path = root_dir.join(&entry.relative_path);
            files_to_send.push((
                entry.relative_path.clone(),
                file_path,
                entry.size,
                None,
                resume_offset,
            ));
        }
    }

    let use_v2 = peer_version >= Some(crate::protocol::messages::MIN_MULTI_FILE_VERSION);

    // Use parallel sending when V2 protocol is available and there are multiple files
    if use_v2 && files_to_send.len() > 1 {
        send_files_parallel(id, &files_to_send, frame_tx).await;
    } else {
        send_files_sequential(id, &files_to_send, frame_tx, use_v2).await;
    }

    cleanup_archives_v2(&files_to_send);
}

/// Send files sequentially (V1 fallback or single file).
async fn send_files_sequential(
    id: Uuid,
    files: &[(String, PathBuf, u64, Option<PathBuf>, u64)],
    frame_tx: &FrameChannel,
    use_v2: bool,
) {
    let mut total_sent: u64 = 0;

    for (file_idx, (display_name, file_path, _file_size, _cleanup, resume_offset)) in
        files.iter().enumerate()
    {
        match ChunkedReader::open(file_path, *resume_offset).await {
            Ok(mut reader) => {
                tracing::info!(
                    "Sending: {} ({} bytes, resume_offset={})",
                    display_name,
                    reader.total_size(),
                    resume_offset,
                );

                while let Ok(Some((_offset, chunk))) = reader.read_chunk().await {
                    let frame = if use_v2 {
                        encode_data_frame_v2(id, file_idx as u32, total_sent, &chunk)
                    } else {
                        encode_data_frame_v1(id, total_sent, &chunk)
                    };
                    if frame_tx.send(frame).is_err() {
                        send_control(
                            frame_tx,
                            &ControlMessage::TransferError {
                                id,
                                error: "Connection lost while sending".to_string(),
                            },
                        );
                        return;
                    }
                    total_sent += chunk.len() as u64;
                }
            }
            Err(e) => {
                send_control(
                    frame_tx,
                    &ControlMessage::TransferError {
                        id,
                        error: format!("Failed to open {}: {}", display_name, e),
                    },
                );
                return;
            }
        }
    }

    send_control(
        frame_tx,
        &ControlMessage::TransferComplete {
            id,
            total_bytes: total_sent,
        },
    );
    tracing::info!("send_entries complete: {} ({} bytes)", id, total_sent);
}

/// Send multiple files in parallel using up to MAX_PARALLEL_READERS concurrent readers.
/// Each reader tags its chunks with the correct file_index, all multiplexed through
/// a single frame channel.
async fn send_files_parallel(
    id: Uuid,
    files: &[(String, PathBuf, u64, Option<PathBuf>, u64)],
    frame_tx: &FrameChannel,
) {
    use tokio::sync::Semaphore;

    let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_READERS));
    let frame_tx = frame_tx.clone();
    let total_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let error_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut handles = Vec::new();

    for (file_idx, (display_name, file_path, _file_size, _cleanup, resume_offset)) in
        files.iter().enumerate()
    {
        let sem = semaphore.clone();
        let tx = frame_tx.clone();
        let total = total_sent.clone();
        let err_flag = error_flag.clone();
        let path = file_path.clone();
        let name = display_name.clone();
        let offset = *resume_offset;

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            if err_flag.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            match ChunkedReader::open(&path, offset).await {
                Ok(mut reader) => {
                    tracing::info!(
                        "Parallel sending: {} ({} bytes, resume_offset={})",
                        name,
                        reader.total_size(),
                        offset,
                    );

                    while let Ok(Some((_chunk_offset, chunk))) = reader.read_chunk().await {
                        if err_flag.load(std::sync::atomic::Ordering::Relaxed) {
                            return;
                        }
                        let frame_offset = total.fetch_add(
                            chunk.len() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        let frame =
                            encode_data_frame_v2(id, file_idx as u32, frame_offset, &chunk);
                        if tx.send(frame).is_err() {
                            err_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to open {}: {}", name, e);
                    err_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all readers to complete
    for handle in handles {
        let _ = handle.await;
    }

    if error_flag.load(std::sync::atomic::Ordering::Relaxed) {
        send_control(
            &frame_tx,
            &ControlMessage::TransferError {
                id,
                error: "Connection lost or read error during parallel transfer".to_string(),
            },
        );
        return;
    }

    let final_total = total_sent.load(std::sync::atomic::Ordering::Relaxed);
    send_control(
        &frame_tx,
        &ControlMessage::TransferComplete {
            id,
            total_bytes: final_total,
        },
    );
    tracing::info!(
        "send_entries parallel complete: {} ({} bytes)",
        id,
        final_total
    );
}

/// Push entries to remote -- reads local files and streams through the unified frame channel.
/// Waits for TransferFinalized acknowledgment from the remote before notifying the browser.
async fn push_entries(
    state: &AppState,
    id: Uuid,
    entries: &[TransferEntry],
    ws_tx: &mpsc::UnboundedSender<Message>,
) {
    let (frame_tx, peer_version) = {
        let remote = state.remote.read().await;
        remote
            .as_ref()
            .map(|r| (r.frame_tx.clone(), r.peer_version))
            .unzip()
    };
    let peer_version = peer_version.flatten();
    tracing::debug!(
        "push_entries: peer_version = {:?}, use_v2 = {}",
        peer_version,
        peer_version >= Some(crate::protocol::messages::MIN_MULTI_FILE_VERSION)
    );

    let Some(frame_tx) = frame_tx else {
        send_error(ws_tx, id, "Remote connection lost");
        return;
    };

    let mut files_to_send: Vec<(String, PathBuf, u64, Option<PathBuf>, u64)> = Vec::new();

    for entry in entries {
        // Validate path before any file operations
        if let Err(e) = validate_path(&state.config.root_dir, &entry.relative_path) {
            send_error(
                ws_tx,
                id,
                &format!("Invalid path {}: {}", entry.relative_path, e),
            );
            cleanup_archives_v2(&files_to_send);
            return;
        }

        if entry.is_dir {
            let compression_mode = entry.compression.as_deref();
            match compress::compress_directory_with_mode(
                &state.config.root_dir,
                &entry.relative_path,
                compression_mode,
            ) {
                Ok((archive_path, archive_size)) => {
                    files_to_send.push((
                        entry.relative_path.clone(),
                        archive_path.clone(),
                        archive_size,
                        Some(archive_path),
                        0,
                    ));
                }
                Err(e) => {
                    send_error(
                        ws_tx,
                        id,
                        &format!("Failed to compress {}: {}", entry.relative_path, e),
                    );
                    cleanup_archives_v2(&files_to_send);
                    return;
                }
            }
        } else {
            let file_path = state.config.root_dir.join(&entry.relative_path);
            files_to_send.push((entry.relative_path.clone(), file_path, entry.size, None, 0));
        }
    }

    let total_size: u64 = files_to_send.iter().map(|(_, _, s, _, _)| s).sum();
    let use_v2 = peer_version >= Some(crate::protocol::messages::MIN_MULTI_FILE_VERSION);
    let mut total_sent: u64 = 0;

    // Register completion waiter BEFORE sending any data, so TransferFinalized
    // cannot arrive and be missed between sending TransferComplete and registering.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    state.pending_completions.lock().await.insert(id, done_tx);

    for (file_idx, (display_name, file_path, _file_size, _cleanup, resume_offset)) in
        files_to_send.iter().enumerate()
    {
        match ChunkedReader::open(file_path, *resume_offset).await {
            Ok(mut reader) => {
                tracing::info!("Sending: {} ({} bytes)", display_name, reader.total_size());

                while let Ok(Some((_offset, chunk))) = reader.read_chunk().await {
                    let frame = if use_v2 {
                        encode_data_frame_v2(id, file_idx as u32, total_sent, &chunk)
                    } else {
                        encode_data_frame_v1(id, total_sent, &chunk)
                    };
                    if frame_tx.send(frame).is_err() {
                        state.pending_completions.lock().await.remove(&id);
                        send_error(ws_tx, id, "Connection to remote lost");
                        cleanup_archives_v2(&files_to_send);
                        return;
                    }
                    total_sent += chunk.len() as u64;

                    let _ = ws_tx.send(Message::Text(
                        serde_json::to_string(&ControlMessage::TransferProgress {
                            id,
                            path: display_name.clone(),
                            bytes_done: total_sent,
                            bytes_total: total_size,
                        })
                        .unwrap()
                        .into(),
                    ));
                }
            }
            Err(e) => {
                state.pending_completions.lock().await.remove(&id);
                send_error(
                    ws_tx,
                    id,
                    &format!("Failed to open {}: {}", display_name, e),
                );
                cleanup_archives_v2(&files_to_send);
                return;
            }
        }
    }

    // Tell the remote we're done sending (with byte count for verification)
    send_control(
        &frame_tx,
        &ControlMessage::TransferComplete {
            id,
            total_bytes: total_sent,
        },
    );

    // Wait for the remote to confirm receipt before telling the browser
    let wait_result = tokio::time::timeout(std::time::Duration::from_secs(300), done_rx).await;
    match wait_result {
        Err(_) => {
            state.pending_completions.lock().await.remove(&id);
            send_error(
                ws_tx,
                id,
                "Remote did not confirm transfer within 5 minutes",
            );
        }
        Ok(Err(_recv_err)) => {
            send_error(ws_tx, id, "Remote completion channel closed unexpectedly");
        }
        Ok(Ok(Err(error))) => {
            send_error(ws_tx, id, &error);
        }
        Ok(Ok(Ok(()))) => {
            tracing::info!("Push verified complete: {} ({} bytes)", id, total_sent);
            let _ = ws_tx.send(Message::Text(
                serde_json::to_string(&ControlMessage::TransferComplete {
                    id,
                    total_bytes: total_sent,
                })
                .unwrap()
                .into(),
            ));
        }
    }

    cleanup_archives_v2(&files_to_send);
}

fn send_control(frame_tx: &FrameChannel, msg: &ControlMessage) {
    let json = serde_json::to_string(msg).unwrap();
    let _ = frame_tx.send(encode_control_frame(json.as_bytes()));
}

fn send_error(ws_tx: &mpsc::UnboundedSender<Message>, id: Uuid, error: &str) {
    let _ = ws_tx.send(Message::Text(
        serde_json::to_string(&ControlMessage::TransferError {
            id,
            error: error.to_string(),
        })
        .unwrap()
        .into(),
    ));
}

fn cleanup_archives_v2(files: &[(String, PathBuf, u64, Option<PathBuf>, u64)]) {
    for (_, _, _, cleanup, _) in files {
        if let Some(path) = cleanup {
            compress::cleanup_archive(path);
        }
    }
}
