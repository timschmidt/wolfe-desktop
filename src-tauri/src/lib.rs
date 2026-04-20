use arrow_array::{Array, StringArray};
use base64::{engine::general_purpose, Engine as _};
use directories::ProjectDirs;
use lance::dataset::Dataset;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::OsStr,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{AppHandle, Emitter, Manager, State};
use uuid::Uuid;

const DEFAULT_IMAGE: &str = "localhost/wolfe-podman:latest";
const FALLBACK_IMAGE: &str = "localhost/wolfe-podman:latest";
const PODMAN_REPO: &str = "https://github.com/timschmidt/wolfe-podman.git";
const DEFAULT_CDI_DEVICE: &str = "nvidia.com/gpu=all";
const DEFAULT_CACHE_VOLUME: &str = "wolfe-cache";

#[derive(Default)]
struct AppState {
    jobs: Arc<Mutex<HashMap<String, JobControl>>>,
}

#[derive(Clone)]
struct JobControl {
    cancel: Arc<Mutex<bool>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexRequest {
    root_path: String,
    db_path: String,
    image: Option<String>,
    device: Option<String>,
    low_memory: bool,
    translate: bool,
    batch_by_subfolder: bool,
    ignores: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexJob {
    id: String,
    batches: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BatchProgress {
    job_id: String,
    batch_index: usize,
    batch_count: usize,
    path: String,
    status: String,
    message: String,
    ingest_current: Option<usize>,
    ingest_total: Option<usize>,
    ingest_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchRequest {
    query: String,
    db_path: String,
    image: Option<String>,
    limit: usize,
    device: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResult {
    score: Option<f64>,
    path: Option<String>,
    source_path: Option<String>,
    record_type: Option<String>,
    text: Option<String>,
    raw: Value,
    preview: Option<Preview>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Preview {
    kind: String,
    mime: String,
    data_url: Option<String>,
    file_url: Option<String>,
    label: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreviewRequest {
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseStatusRequest {
    db_path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseStatus {
    exists: bool,
    size_bytes: u64,
    entry_count: Option<usize>,
    unique_file_count: Option<usize>,
    table_name: String,
    table_path: String,
    message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PodmanStatus {
    available: bool,
    version: Option<String>,
    image: String,
}

#[tauri::command]
fn default_paths() -> Result<HashMap<String, String>, String> {
    let dirs = ProjectDirs::from("com", "wolfe", "desktop")
        .ok_or_else(|| "Could not resolve platform data directory".to_string())?;
    let data_dir = dirs.data_dir();
    fs::create_dir_all(data_dir).map_err(|err| err.to_string())?;

    let mut paths = HashMap::new();
    paths.insert(
        "dbPath".to_string(),
        data_dir.join("wolfe.lance").display().to_string(),
    );
    paths.insert("image".to_string(), DEFAULT_IMAGE.to_string());
    Ok(paths)
}

#[tauri::command]
fn podman_status(image: Option<String>) -> PodmanStatus {
    let version = Command::new("podman")
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        });

    PodmanStatus {
        available: version.is_some(),
        version,
        image: image.unwrap_or_else(|| DEFAULT_IMAGE.to_string()),
    }
}

#[tauri::command]
fn build_wolfe_image(app: AppHandle, image: Option<String>) -> Result<(), String> {
    let target = image.unwrap_or_else(|| FALLBACK_IMAGE.to_string());
    let output = Command::new("podman")
        .args(["build", "-t", &target, PODMAN_REPO])
        .output()
        .map_err(|err| format!("Failed to start podman build: {err}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let message = format!("{stdout}{stderr}");
    let _ = app.emit("wolfe://image-build", message.clone());

    if output.status.success() {
        Ok(())
    } else {
        Err(message)
    }
}

#[tauri::command]
fn start_index(
    app: AppHandle,
    state: State<'_, AppState>,
    request: IndexRequest,
) -> Result<IndexJob, String> {
    let root = PathBuf::from(&request.root_path);
    let db = PathBuf::from(&request.db_path);
    validate_existing_path(&root)?;
    ensure_db_parent(&db)?;

    let batches = build_batches(&root, request.batch_by_subfolder)?;
    let job_id = Uuid::new_v4().to_string();
    let cancel = Arc::new(Mutex::new(false));
    state.jobs.lock().map_err(|err| err.to_string())?.insert(
        job_id.clone(),
        JobControl {
            cancel: cancel.clone(),
        },
    );

    let batch_labels = batches
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>();
    let thread_job_id = job_id.clone();
    let jobs = state.jobs.clone();

    thread::spawn(move || {
        run_index_job(app, thread_job_id.clone(), request, batches, cancel);
        if let Ok(mut jobs) = jobs.lock() {
            jobs.remove(&thread_job_id);
        }
    });

    Ok(IndexJob {
        id: job_id,
        batches: batch_labels,
    })
}

#[tauri::command]
fn cancel_index(state: State<'_, AppState>, job_id: String) -> Result<(), String> {
    let jobs = state.jobs.lock().map_err(|err| err.to_string())?;
    let job = jobs
        .get(&job_id)
        .ok_or_else(|| format!("No active job with id {job_id}"))?;
    *job.cancel.lock().map_err(|err| err.to_string())? = true;
    Ok(())
}

#[tauri::command]
fn search(request: SearchRequest) -> Result<Vec<SearchResult>, String> {
    let db = PathBuf::from(&request.db_path);
    let limit = request.limit.clamp(1, 100).to_string();
    let image = request.image.unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let mounts = Mounts::for_search(&db)?;

    let device = normalized_device(request.device);
    let mut args = base_podman_args(
        &image,
        &mounts,
        should_enable_nvidia_cdi(device.as_deref())?,
    );
    args.extend(["--search".to_string(), request.query]);
    args.extend(["--db".to_string(), mounts.container_db_path()]);
    args.extend(["--limit".to_string(), limit]);
    args.push("--json".to_string());
    if let Some(device) = device {
        args.extend(["--device".to_string(), device]);
    }

    let output = run_podman(args)?;
    let parsed: Value = serde_json::from_str(&output)
        .map_err(|err| format!("Wolfe did not return valid JSON: {err}\n{output}"))?;
    let rows = parsed.as_array().cloned().unwrap_or_default();

    Ok(rows
        .into_iter()
        .map(|raw| {
            let path = string_field(&raw, &["path", "file", "source", "source_path"]);
            let source_path = path
                .as_deref()
                .and_then(|p| mounts.host_path_from_container(p))
                .map(|p| p.display().to_string());
            let preview = source_path
                .as_deref()
                .and_then(|p| make_preview(Path::new(p)).ok());

            SearchResult {
                score: number_field(&raw, &["score", "distance", "_distance"]),
                path,
                source_path,
                record_type: string_field(&raw, &["kind", "type", "record_type", "modality"]),
                text: string_field(&raw, &["text", "chunk", "content", "summary"]),
                raw,
                preview,
            }
        })
        .collect())
}

#[tauri::command]
fn preview_file(request: PreviewRequest) -> Result<Preview, String> {
    make_preview(Path::new(&request.path))
}

#[tauri::command]
fn reveal_path(path: String) -> Result<(), String> {
    open::that(path).map_err(|err| err.to_string())
}

#[tauri::command]
fn delete_database(request: DatabaseStatusRequest) -> Result<(), String> {
    let target = resolve_table_target(Path::new(&request.db_path))?;
    if target.table_path.exists() {
        let metadata = fs::symlink_metadata(&target.table_path).map_err(|err| err.to_string())?;
        if metadata.is_dir() {
            fs::remove_dir_all(&target.table_path).map_err(|err| err.to_string())?;
        } else {
            fs::remove_file(&target.table_path).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

#[tauri::command]
async fn database_status(request: DatabaseStatusRequest) -> Result<DatabaseStatus, String> {
    let target = resolve_table_target(Path::new(&request.db_path))?;
    let exists = target.table_path.exists();
    let size_path = if exists {
        target.table_path.as_path()
    } else {
        target.db_root.as_path()
    };
    let size_bytes = path_size(size_path);

    let (entry_count, unique_file_count, message) = if exists {
        match Dataset::open(&target.table_path.to_string_lossy()).await {
            Ok(dataset) => {
                let mut messages = Vec::new();
                let entry_count = match dataset.count_rows(None).await {
                    Ok(count) => Some(count),
                    Err(err) => {
                        messages.push(format!("Could not count rows: {err}"));
                        None
                    }
                };
                let unique_file_count = match count_unique_files(&dataset).await {
                    Ok(count) => Some(count),
                    Err(err) => {
                        messages.push(format!("Could not count files: {err}"));
                        None
                    }
                };
                let message = if messages.is_empty() {
                    None
                } else {
                    Some(messages.join("; "))
                };
                (entry_count, unique_file_count, message)
            }
            Err(err) => (
                None,
                None,
                Some(format!("Could not open Lance table: {err}")),
            ),
        }
    } else {
        (None, None, Some("Database table not found".to_string()))
    };

    Ok(DatabaseStatus {
        exists,
        size_bytes,
        entry_count,
        unique_file_count,
        table_name: target.table_name,
        table_path: target.table_path.display().to_string(),
        message,
    })
}

fn run_index_job(
    app: AppHandle,
    job_id: String,
    request: IndexRequest,
    batches: Vec<PathBuf>,
    cancel: Arc<Mutex<bool>>,
) {
    let batch_count = batches.len();
    for (idx, batch) in batches.into_iter().enumerate() {
        if cancel.lock().map(|c| *c).unwrap_or(false) {
            emit_progress(
                &app,
                &job_id,
                idx,
                batch_count,
                &batch,
                "cancelled",
                "Index cancelled",
            );
            break;
        }

        emit_progress(
            &app,
            &job_id,
            idx,
            batch_count,
            &batch,
            "running",
            "Starting batch",
        );
        let progress_target = ProgressTarget {
            app: app.clone(),
            job_id: job_id.clone(),
            batch_index: idx,
            batch_count,
            batch_path: batch.clone(),
        };
        let result = run_index_batch(&request, &batch, progress_target);
        match result {
            Ok(message) => emit_progress(&app, &job_id, idx, batch_count, &batch, "done", &message),
            Err(message) => {
                emit_progress(&app, &job_id, idx, batch_count, &batch, "error", &message)
            }
        }
    }

    let _ = app.emit(
        "wolfe://index-finished",
        serde_json::json!({ "jobId": job_id, "finishedAt": unix_time() }),
    );
}

#[derive(Clone)]
struct ProgressTarget {
    app: AppHandle,
    job_id: String,
    batch_index: usize,
    batch_count: usize,
    batch_path: PathBuf,
}

fn run_index_batch(
    request: &IndexRequest,
    batch: &Path,
    progress: ProgressTarget,
) -> Result<String, String> {
    let db = PathBuf::from(&request.db_path);
    let image = request
        .image
        .clone()
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let mounts = Mounts::for_index(batch, &db)?;

    let device = normalized_device(request.device.clone());
    let mut args = base_podman_args(
        &image,
        &mounts,
        should_enable_nvidia_cdi(device.as_deref())?,
    );
    args.extend(["--path".to_string(), batch.display().to_string()]);
    args.extend(["--db".to_string(), mounts.container_db_path()]);
    if request.low_memory {
        args.push("--low-memory".to_string());
    }
    if request.translate {
        args.push("--translate".to_string());
    }
    if let Some(device) = device {
        args.extend(["--device".to_string(), device]);
    }
    for ignore in &request.ignores {
        if !ignore.trim().is_empty() {
            args.extend(["--ignore".to_string(), ignore.trim().to_string()]);
        }
    }

    run_podman_streaming_index(args, progress)
}

fn base_podman_args(image: &str, mounts: &Mounts, enable_nvidia_cdi: bool) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--security-opt".to_string(),
        "label=disable".to_string(),
        "--ipc=host".to_string(),
        "--user".to_string(),
        "0:0".to_string(),
    ];
    if enable_nvidia_cdi {
        args.extend(["--device".to_string(), cdi_device_name()]);
    }
    for mount in &mounts.args {
        args.extend(["-v".to_string(), mount.clone()]);
    }
    args.extend([
        "--mount".to_string(),
        format!("type=volume,src={},dst=/cache", cache_volume_name()),
    ]);
    args.push(image.to_string());
    args
}

fn run_podman(args: Vec<String>) -> Result<String, String> {
    let output = Command::new("podman")
        .args(args)
        .output()
        .map_err(|err| format!("Failed to start podman: {err}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        Ok(if stdout.trim().is_empty() {
            stderr
        } else {
            stdout
        })
    } else {
        Err(format!("{stdout}{stderr}"))
    }
}

fn run_podman_streaming_index(
    args: Vec<String>,
    progress: ProgressTarget,
) -> Result<String, String> {
    let mut child = Command::new("podman")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Failed to start podman: {err}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture podman stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Failed to capture podman stderr".to_string())?;

    let stdout_text = Arc::new(Mutex::new(String::new()));
    let stdout_target = stdout_text.clone();
    let stdout_thread = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if let Ok(mut output) = stdout_target.lock() {
                output.push_str(&line);
                output.push('\n');
            }
        }
    });

    let mut stderr_text = String::new();
    let reader = BufReader::new(stderr);
    for line in reader.lines().map_while(Result::ok) {
        stderr_text.push_str(&line);
        stderr_text.push('\n');
        if let Some((current, total, path)) = parse_ingest_line(&line) {
            emit_progress_with_ingest(
                &progress.app,
                &progress.job_id,
                progress.batch_index,
                progress.batch_count,
                &progress.batch_path,
                "running",
                &format!("Ingesting {current}/{total}: {}", display_short_path(&path)),
                Some(current),
                Some(total),
                Some(path),
            );
        }
    }

    let status = child
        .wait()
        .map_err(|err| format!("Failed while waiting for podman: {err}"))?;
    let _ = stdout_thread.join();
    let stdout = stdout_text.lock().map(|s| s.clone()).unwrap_or_default();
    let combined = format!("{stdout}{stderr_text}");

    if status.success() {
        if summary_has_only_failed_records(&stdout) {
            Err(combined)
        } else {
            Ok(if combined.trim().is_empty() {
                "Index batch finished.".to_string()
            } else {
                combined
            })
        }
    } else {
        Err(combined)
    }
}

fn summary_has_only_failed_records(stdout: &str) -> bool {
    let Ok(summary) = serde_json::from_str::<Value>(stdout.trim()) else {
        return false;
    };
    let stored = summary
        .get("stored")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let errors = summary
        .get("errors")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    stored == 0 && errors > 0
}

fn parse_ingest_line(line: &str) -> Option<(usize, usize, String)> {
    let trimmed = line.trim();
    let rest = trimmed
        .strip_prefix("ingest ")
        .or_else(|| trimmed.strip_prefix("ingest: "))?;
    let (counter, path) = rest.split_once(':')?;
    let (current, total) = counter.trim().split_once('/')?;
    let current = current.trim().parse().ok()?;
    let total = total.trim().parse().ok()?;
    Some((current, total, path.trim().to_string()))
}

fn display_short_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or(path)
        .to_string()
}

#[derive(Debug)]
struct Mounts {
    args: Vec<String>,
    db: PathBuf,
}

impl Mounts {
    fn for_index(input: &Path, db: &Path) -> Result<Self, String> {
        let db_parent = db
            .parent()
            .ok_or_else(|| "Database path needs a parent directory".to_string())?;
        Ok(Self {
            args: vec![
                format!("{}:{}:ro", input.display(), input.display()),
                format!("{}:{}", db_parent.display(), db_parent.display()),
            ],
            db: db.to_path_buf(),
        })
    }

    fn for_search(db: &Path) -> Result<Self, String> {
        let db_parent = db
            .parent()
            .ok_or_else(|| "Database path needs a parent directory".to_string())?;
        Ok(Self {
            args: vec![format!("{}:{}", db_parent.display(), db_parent.display())],
            db: db.to_path_buf(),
        })
    }

    fn container_db_path(&self) -> String {
        self.db.display().to_string()
    }

    fn host_path_from_container(&self, raw: &str) -> Option<PathBuf> {
        let path = PathBuf::from(raw.split("!/").next().unwrap_or(raw));
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }
}

fn build_batches(root: &Path, by_subfolder: bool) -> Result<Vec<PathBuf>, String> {
    if !by_subfolder || root.is_file() {
        return Ok(vec![root.to_path_buf()]);
    }

    let mut batches = fs::read_dir(root)
        .map_err(|err| err.to_string())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    batches.sort();

    if batches.is_empty() {
        Ok(vec![root.to_path_buf()])
    } else {
        Ok(batches)
    }
}

fn validate_existing_path(path: &Path) -> Result<(), String> {
    if path.exists() {
        Ok(())
    } else {
        Err(format!("Path does not exist: {}", path.display()))
    }
}

fn ensure_db_parent(db: &Path) -> Result<(), String> {
    let parent = db
        .parent()
        .ok_or_else(|| "Database path needs a parent directory".to_string())?;
    fs::create_dir_all(parent).map_err(|err| err.to_string())
}

struct TableTarget {
    db_root: PathBuf,
    table_name: String,
    table_path: PathBuf,
}

fn resolve_table_target(db: &Path) -> Result<TableTarget, String> {
    if db.extension().and_then(OsStr::to_str) == Some("lance") {
        let db_root = db
            .parent()
            .ok_or_else(|| "Database path needs a parent directory".to_string())?
            .to_path_buf();
        let table_name = db
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(|| "Database path needs a table name".to_string())?
            .to_string();
        return Ok(TableTarget {
            db_root,
            table_name,
            table_path: db.to_path_buf(),
        });
    }

    Ok(TableTarget {
        db_root: db.to_path_buf(),
        table_name: "embeddings".to_string(),
        table_path: db.join("embeddings.lance"),
    })
}

fn path_size(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }

    fs::read_dir(path)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(Result::ok))
        .map(|entry| path_size(&entry.path()))
        .sum()
}

async fn count_unique_files(dataset: &Dataset) -> Result<usize, String> {
    let mut scanner = dataset.scan();
    scanner
        .project(&["path"])
        .map_err(|err| format!("Could not read path column: {err}"))?;
    let batch = scanner
        .try_into_batch()
        .await
        .map_err(|err| err.to_string())?;
    let paths = batch
        .column_by_name("path")
        .ok_or_else(|| "path column is missing".to_string())?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "path column is not Utf8".to_string())?;

    let mut unique = HashSet::new();
    for index in 0..paths.len() {
        if !paths.is_null(index) {
            unique.insert(paths.value(index).to_string());
        }
    }
    Ok(unique.len())
}

fn normalized_device(device: Option<String>) -> Option<String> {
    let device = device?;
    let device = device.trim();
    match device {
        "auto" | "cpu" | "cuda" | "mps" => Some(device.to_string()),
        _ => None,
    }
}

fn should_enable_nvidia_cdi(device: Option<&str>) -> Result<bool, String> {
    match device {
        Some("cpu" | "mps") => Ok(false),
        Some("cuda") if !has_nvidia_cdi_spec() => Err(format!(
            "CUDA was selected, but no NVIDIA CDI specification was found. Generate one with `sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml`, then verify `podman run --rm --device {} ubuntu:24.04 nvidia-smi`.",
            cdi_device_name()
        )),
        Some("cuda") => Ok(true),
        Some("auto") | None => Ok(has_nvidia_cdi_spec()),
        Some(_) => Ok(false),
    }
}

fn has_nvidia_cdi_spec() -> bool {
    Path::new("/etc/cdi/nvidia.yaml").exists() || Path::new("/var/run/cdi/nvidia.yaml").exists()
}

fn cdi_device_name() -> String {
    env::var("WOLFE_CDI_DEVICE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CDI_DEVICE.to_string())
}

fn cache_volume_name() -> String {
    env::var("WOLFE_CACHE_VOLUME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CACHE_VOLUME.to_string())
}

fn emit_progress(
    app: &AppHandle,
    job_id: &str,
    batch_index: usize,
    batch_count: usize,
    path: &Path,
    status: &str,
    message: &str,
) {
    let _ = app.emit(
        "wolfe://index-progress",
        BatchProgress {
            job_id: job_id.to_string(),
            batch_index,
            batch_count,
            path: path.display().to_string(),
            status: status.to_string(),
            message: message.to_string(),
            ingest_current: None,
            ingest_total: None,
            ingest_path: None,
        },
    );
}

fn emit_progress_with_ingest(
    app: &AppHandle,
    job_id: &str,
    batch_index: usize,
    batch_count: usize,
    path: &Path,
    status: &str,
    message: &str,
    ingest_current: Option<usize>,
    ingest_total: Option<usize>,
    ingest_path: Option<String>,
) {
    let _ = app.emit(
        "wolfe://index-progress",
        BatchProgress {
            job_id: job_id.to_string(),
            batch_index,
            batch_count,
            path: path.display().to_string(),
            status: status.to_string(),
            message: message.to_string(),
            ingest_current,
            ingest_total,
            ingest_path,
        },
    );
}

fn make_preview(path: &Path) -> Result<Preview, String> {
    let ext = path
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let label = path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("file")
        .to_string();

    if is_image(&ext) {
        let bytes = fs::read(path).map_err(|err| err.to_string())?;
        let mime = image_mime(&ext);
        return Ok(Preview {
            kind: "image".to_string(),
            mime: mime.to_string(),
            data_url: Some(format!(
                "data:{mime};base64,{}",
                general_purpose::STANDARD.encode(bytes)
            )),
            file_url: None,
            label,
        });
    }

    if is_audio(&ext) {
        return Ok(file_preview("audio", audio_mime(&ext), path, label));
    }

    if is_video(&ext) {
        return Ok(file_preview("video", video_mime(&ext), path, label));
    }

    if is_document(&ext) {
        return Ok(Preview {
            kind: "document".to_string(),
            mime: document_mime(&ext).to_string(),
            data_url: None,
            file_url: Some(path.display().to_string()),
            label,
        });
    }

    Ok(Preview {
        kind: "file".to_string(),
        mime: "application/octet-stream".to_string(),
        data_url: None,
        file_url: Some(path.display().to_string()),
        label,
    })
}

fn file_preview(kind: &str, mime: &str, path: &Path, label: String) -> Preview {
    Preview {
        kind: kind.to_string(),
        mime: mime.to_string(),
        data_url: None,
        file_url: Some(path.display().to_string()),
        label,
    }
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(ToString::to_string)
    })
}

fn number_field(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
}

fn is_image(ext: &str) -> bool {
    matches!(
        ext,
        "avif" | "bmp" | "gif" | "heic" | "heif" | "jpeg" | "jpg" | "png" | "tif" | "tiff" | "webp"
    )
}

fn is_audio(ext: &str) -> bool {
    matches!(
        ext,
        "aac" | "aif" | "aiff" | "au" | "flac" | "m4a" | "mp3" | "ogg" | "opus" | "wav" | "webm"
    )
}

fn is_video(ext: &str) -> bool {
    matches!(
        ext,
        "3gp" | "avi" | "m2ts" | "m4v" | "mkv" | "mov" | "mp4" | "mpeg" | "mpg" | "ts" | "webm"
    )
}

fn is_document(ext: &str) -> bool {
    matches!(
        ext,
        "csv"
            | "djvu"
            | "doc"
            | "docx"
            | "epub"
            | "html"
            | "odg"
            | "odp"
            | "ods"
            | "odt"
            | "pdf"
            | "ppt"
            | "pptx"
            | "ps"
            | "rtf"
            | "svg"
            | "txt"
            | "xls"
            | "xlsx"
            | "xml"
    )
}

fn image_mime(ext: &str) -> &'static str {
    match ext {
        "avif" => "image/avif",
        "bmp" => "image/bmp",
        "gif" => "image/gif",
        "png" => "image/png",
        "tif" | "tiff" => "image/tiff",
        "webp" => "image/webp",
        _ => "image/jpeg",
    }
}

fn audio_mime(ext: &str) -> &'static str {
    match ext {
        "flac" => "audio/flac",
        "m4a" => "audio/mp4",
        "ogg" => "audio/ogg",
        "opus" => "audio/opus",
        "wav" => "audio/wav",
        "webm" => "audio/webm",
        _ => "audio/mpeg",
    }
}

fn video_mime(ext: &str) -> &'static str {
    match ext {
        "mov" => "video/quicktime",
        "mpeg" | "mpg" => "video/mpeg",
        "webm" => "video/webm",
        _ => "video/mp4",
    }
}

fn document_mime(ext: &str) -> &'static str {
    match ext {
        "pdf" => "application/pdf",
        "svg" => "image/svg+xml",
        "txt" => "text/plain",
        "html" => "text/html",
        _ => "application/octet-stream",
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            build_wolfe_image,
            cancel_index,
            database_status,
            delete_database,
            default_paths,
            podman_status,
            preview_file,
            reveal_path,
            search,
            start_index
        ])
        .setup(|app| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.set_title("Wolfe Desktop");
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running Tauri application");
}
