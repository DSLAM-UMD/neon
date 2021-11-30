//! A synchronization logic for the [`RemoteStorage`] and the state to ensure the correct synchronizations.
//!
//! The synchronization does not aim to be immediate, instead
//! doing all the job in a separate thread asynchronously, attempting to fully replicate the
//! pageserver timeline workdir data on the remote storage in a custom format, beneficial for storing.
//!
//! [`SYNC_QUEUE`] is a deque to hold [`SyncTask`] for image upload/download.
//! The queue gets emptied by a single thread with the loop, that polls the tasks in batches (size configurable).
//! Every task in a batch processed concurrently, which is possible due to incremental nature of the timelines.
//!
//! During the loop startup, an initial loop state is constructed from all remote storage entries.
//! It's enough to poll the remote state once on startup only, due to agreement that the pageserver has
//! an exclusive write access to the remote storage: new files appear in the storage only after the same
//! pageserver writes them.
//!
//! The list construction is currently the only place where the storage sync can return an [`Err`] to the user.
//! New upload tasks are accepted via [`schedule_timeline_checkpoint_upload`] function disregarding of the corresponding loop startup,
//! it's up to the caller to avoid uploading of the new file, if that caller did not enable the loop.
//! After the initial state is loaded into memory and the loop starts, any further [`Err`] results do not stop the loop, but rather
//! reschedules the same task, with possibly less files to sync in it.
//!
//! The synchronization unit is an archive: a set of timeline files (or relishes) and a special metadata file, all compressed into a blob.
//! An archive contains set of files of a certain timeline, added during checkpoint(s) and the timeline metadata at that moment.
//! The archive contains that metadata's `disk_consistent_lsn` in its name, to be able to restore partial index information from just a remote storage file list.
//! The index is created at startup (possible due to exclusive ownership over the remote storage by the pageserver) and keeps track of which files were stored
//! in what remote archives.
//! Among other tasks, the index is used to prevent invalid uploads and non-existing downloads on demand.
//! Refer to [`compression`] and [`index`] for more details on the archives and index respectively.
//!
//! After pageserver parforms a succesful image checkpoint and produces new local files, it schedules an upload with
//! the list of the files and its metadata file contents at the moment of checkpointing.
//! Pageserver needs both the file list and metadata to load the timeline, so both are mandatory for the upload, that's why the uploads happen after checkpointing.
//! Not every upload of the same timeline gets processed: if the checkpoint with the same `disk_consistent_lsn` was already uploaded, no reuploads happen, as checkpoints
//! are considered to be immutable. The order of `lsn` during uploads is allowed to be arbitrary and not required to be ascending.
//!
//! Current uploads are per-checkpoint and don't accumulate any data with optimal size for storing on S3.
//! The upload is atomic and gets rescheduled entirely, if fails along the way.
//! The downloads are per-timeline and download all missing timeline files.
//! The archives get processed sequentially, from smaller `disk_consistent_lsn` to larger, with metadata files being added as last.
//! If any of the archive processing fails along the way, all the remaining archives are rescheduled for the next attempt.
//! There's a reschedule threshold that evicts tasks that fail too much and stops the corresponding timeline so it does not diverge from the state on the remote storage.
//! The archive unpacking is designed to unpack metadata as the last file, so the risk of leaving the corrupt timeline due to uncompression error is small (while not eliminated entirely and that should be improved).
//!
//! Synchronization never removes any local from pageserver workdir or remote files from the remote storage, yet there could be overwrites of the same files (metadata file updates; archive redownloads).
//! NOTE: No real contents or checksum check happens right now and is a subject to improve later.
//!
//! After the whole timeline is downloaded, [`crate::tenant_mgr::register_timeline_download`] function is used to register the image in pageserver.
//!
//! When pageserver signals shutdown, current sync task gets finished and the loop exists.
//!
//! Currently there's no other way to download a remote relish if it was not downloaded after initial remote storage files check.
//! This is a subject to change in the near future.

mod compression;
mod download;
pub mod index;
mod upload;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    thread,
};

use anyhow::{bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use tokio::{
    sync::mpsc::{self, UnboundedReceiver},
    time::Instant,
};
use tracing::*;

use self::{
    compression::ArchiveHeader,
    download::download_timeline,
    index::{ArchiveDescription, ArchiveId, RemoteTimeline, RemoteTimelineIndex},
    upload::upload_timeline_checkpoint,
};
use super::{RemoteStorage, TimelineSyncId};
use crate::{
    layered_repository::metadata::TimelineMetadata,
    remote_storage::storage_sync::{compression::read_archive_header, index::IndexEntry},
    repository::TimelineState,
    tenant_mgr::set_timeline_states,
    PageServerConf,
};

use zenith_metrics::{register_histogram_vec, register_int_gauge, HistogramVec, IntGauge};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

lazy_static! {
    static ref REMAINING_SYNC_ITEMS: IntGauge = register_int_gauge!(
        "pageserver_remote_storage_remaining_sync_items",
        "Number of storage sync items left in the queue"
    )
    .expect("failed to register pageserver remote storage remaining sync items int gauge");
    static ref IMAGE_SYNC_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_remote_storage_image_sync_time",
        "Time took to synchronize (download or upload) a whole pageserver image. \
        Grouped by `operation_kind` (upload|download) and `status` (success|failure)",
        &["operation_kind", "status"],
        vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0,
            8.0, 9.0, 10.0, 12.5, 15.0, 17.5, 20.0
        ]
    )
    .expect("failed to register pageserver image sync time histogram vec");
}

/// Wraps mpsc channel bits around into a queue interface.
/// mpsc approach was picked to allow blocking the sync loop if no tasks are present, to avoud meaningless spinning.
mod sync_queue {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::anyhow;
    use once_cell::sync::OnceCell;
    use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};
    use tracing::{debug, warn};

    use super::SyncTask;

    static SENDER: OnceCell<UnboundedSender<SyncTask>> = OnceCell::new();
    static LENGTH: AtomicUsize = AtomicUsize::new(0);

    pub fn init(sender: UnboundedSender<SyncTask>) -> anyhow::Result<()> {
        SENDER
            .set(sender)
            .map_err(|_| anyhow!("sync queue was already initialized"))?;
        Ok(())
    }

    pub fn push(new_task: SyncTask) -> bool {
        if let Some(sender) = SENDER.get() {
            match sender.send(new_task) {
                Err(e) => {
                    warn!(
                        "Failed to enqueue a sync task: the receiver is dropped: {}",
                        e
                    );
                    false
                }
                Ok(()) => {
                    LENGTH.fetch_add(1, Ordering::Relaxed);
                    true
                }
            }
        } else {
            warn!("Failed to enqueue a sync task: the receiver is not initialized");
            false
        }
    }

    pub async fn next_task(receiver: &mut UnboundedReceiver<SyncTask>) -> Option<SyncTask> {
        let task = receiver.recv().await;
        LENGTH.fetch_sub(1, Ordering::Relaxed);
        task
    }

    pub async fn next_task_batch(
        receiver: &mut UnboundedReceiver<SyncTask>,
        mut max_batch_size: usize,
    ) -> Vec<SyncTask> {
        let mut tasks = Vec::with_capacity(max_batch_size);

        if max_batch_size == 0 {
            return tasks;
        }

        loop {
            match receiver.try_recv() {
                Ok(new_task) => {
                    max_batch_size -= 1;
                    LENGTH.fetch_sub(1, Ordering::Relaxed);
                    tasks.push(new_task);
                    if max_batch_size == 0 {
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("Sender disconnected, batch collection aborted");
                    break;
                }
                Err(TryRecvError::Empty) => {
                    debug!("No more data in the sync queue, task batch is not full");
                    break;
                }
            }
        }

        tasks
    }

    pub fn len() -> usize {
        LENGTH.load(Ordering::Relaxed)
    }
}

/// A task to run in the async download/upload loop.
/// Limited by the number of retries, after certain threshold the failing task gets evicted and the timeline disabled.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SyncTask {
    sync_id: TimelineSyncId,
    retries: u32,
    kind: SyncKind,
}

impl SyncTask {
    fn new(sync_id: TimelineSyncId, retries: u32, kind: SyncKind) -> Self {
        Self {
            sync_id,
            retries,
            kind,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum SyncKind {
    /// A certain amount of images (archive files) to download.
    Download(TimelineDownload),
    /// A checkpoint outcome with possible local file updates that need actualization in the remote storage.
    /// Not necessary more fresh than the one already uploaded.
    Upload(NewCheckpoint),
}

/// Local timeline files for upload, appeared after the new checkpoint.
/// Current checkpoint design assumes new files are added only, no deletions or amendment happens.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NewCheckpoint {
    /// Relish file paths in the pageserver workdir, that were added for the corresponding checkpoint.
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
}

/// Info about the remote image files.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TimelineDownload {
    files_to_skip: Arc<HashSet<PathBuf>>,
    archives_to_skip: BTreeSet<ArchiveId>,
}

/// Adds the new checkpoint files as an upload sync task to the queue.
/// Ensure that the loop is started otherwise the task is never processed.
pub fn schedule_timeline_checkpoint_upload(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    layers: Vec<PathBuf>,
    metadata: TimelineMetadata,
) {
    if layers.is_empty() {
        debug!("Skipping empty layers upload task");
        return;
    }

    if !sync_queue::push(SyncTask::new(
        TimelineSyncId(tenant_id, timeline_id),
        0,
        SyncKind::Upload(NewCheckpoint { layers, metadata }),
    )) {
        warn!(
            "Could not send an upload task for tenant {}, timeline {}",
            tenant_id, timeline_id
        )
    } else {
        warn!(
            "Could not send an upload task for tenant {}, timeline {}: the sync queue is not initialized",
            tenant_id, timeline_id
        )
    }
}

pub fn schedule_timeline_download(tenant_id: ZTenantId, timeline_id: ZTimelineId) {
    sync_queue::push(SyncTask::new(
        TimelineSyncId(tenant_id, timeline_id),
        0,
        SyncKind::Download(TimelineDownload {
            files_to_skip: Arc::new(HashSet::new()),
            archives_to_skip: BTreeSet::new(),
        }),
    ));
}

/// Uses a remote storage given to start the storage sync loop.
/// See module docs for loop step description.
pub(super) fn spawn_storage_sync_thread<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    local_timeline_files: HashMap<TimelineSyncId, (TimelineMetadata, Vec<PathBuf>)>,
    remote_storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<(
    HashMap<ZTenantId, HashMap<ZTimelineId, TimelineState>>,
    thread::JoinHandle<anyhow::Result<()>>,
)> {
    let (sender, receiver) = mpsc::unbounded_channel();
    sync_queue::init(sender)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to create storage sync runtime")?;

    let remote_index = RemoteTimelineIndex::new(runtime
        .block_on(collect_timeline_descriptions(&remote_storage))
        .context(
            "Failed to list remote storage contents and collect the timeline archive description",
        )?);

    let initial_timeline_states = schedule_first_sync_tasks(&remote_index, local_timeline_files);

    let handle = thread::Builder::new()
        .name("Remote storage sync thread".to_string())
        .spawn(move || {
            storage_sync_loop(
                runtime,
                config,
                receiver,
                remote_index,
                remote_storage,
                max_concurrent_sync,
                max_sync_errors,
            )
        })
        .context("Failed to spawn remote storage sync thread")?;
    Ok((initial_timeline_states, handle))
}

async fn collect_timeline_descriptions<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
) -> anyhow::Result<HashMap<TimelineSyncId, BTreeMap<ArchiveId, ArchiveDescription>>> {
    let mut description = HashMap::<TimelineSyncId, BTreeMap<ArchiveId, ArchiveDescription>>::new();
    for (local_path, remote_path) in storage
        .list()
        .await
        .context("Failed to list remote storage files")?
        .into_iter()
        .map(|remote_path| (storage.local_path(&remote_path), remote_path))
    {
        match local_path.and_then(|local_path| index::parse_archive_description(local_path)) {
            Ok((sync_id, archive_id, archive_description)) => {
                description
                    .entry(sync_id)
                    .or_default()
                    .insert(archive_id, archive_description);
            }
            Err(e) => error!(
                "Failed to parse archive description from path '{:?}', reason: {:#}",
                remote_path, e
            ),
        }
    }
    Ok(description)
}

fn storage_sync_loop<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    runtime: tokio::runtime::Runtime,
    config: &'static PageServerConf,
    mut receiver: UnboundedReceiver<SyncTask>,
    index: RemoteTimelineIndex,
    remote_storage: S,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> anyhow::Result<()> {
    let index = Arc::new(RwLock::new(index));
    let remote_storage = Arc::new(remote_storage);
    while !crate::tenant_mgr::shutdown_requested() {
        let new_timeline_states = runtime.block_on(loop_step(
            config,
            &mut receiver,
            // TODO kb return it back under a single Arc?
            Arc::clone(&remote_storage),
            Arc::clone(&index),
            max_concurrent_sync,
            max_sync_errors,
        ));
        // Batch timeline download registration to ensure that the external registration code won't block any running tasks before.
        set_timeline_states(config, new_timeline_states);
    }

    debug!("Shutdown requested, stopping");
    Ok(())
}

async fn loop_step<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    receiver: &mut UnboundedReceiver<SyncTask>,
    remote_storage: Arc<S>,
    index: Arc<RwLock<RemoteTimelineIndex>>,
    max_concurrent_sync: NonZeroUsize,
    max_sync_errors: NonZeroU32,
) -> HashMap<ZTenantId, HashMap<ZTimelineId, TimelineState>> {
    let max_concurrent_sync = max_concurrent_sync.get();
    let mut next_tasks = Vec::with_capacity(max_concurrent_sync);

    // request the first task in blocking fashion to do less meaningless work
    if let Some(first_task) = sync_queue::next_task(receiver).await {
        next_tasks.push(first_task);
    } else {
        debug!("Shutdown requested, stopping");
        return HashMap::new();
    };
    next_tasks.extend(
        sync_queue::next_task_batch(receiver, max_concurrent_sync - 1)
            .await
            .into_iter(),
    );

    let remaining_queue_length = sync_queue::len();
    debug!(
        "Processing {} tasks in batch, more tasks left to process: {}",
        next_tasks.len(),
        remaining_queue_length
    );
    REMAINING_SYNC_ITEMS.set(remaining_queue_length as i64);

    let mut task_batch = next_tasks
        .into_iter()
        .map(|task| async {
            let sync_id = task.sync_id;
            let extra_step = match tokio::spawn(process_task(
                config,
                Arc::clone(&remote_storage),
                task,
                max_sync_errors,
                Arc::clone(&index),
            ))
            .await
            {
                Ok(extra_step) => extra_step,
                Err(e) => {
                    error!(
                        "Failed to process storage sync task for tenant {}, timeline {}: {:#}",
                        sync_id.0, sync_id.1, e
                    );
                    None
                }
            };
            (sync_id, extra_step)
        })
        .collect::<FuturesUnordered<_>>();

    let mut new_timeline_states: HashMap<ZTenantId, HashMap<ZTimelineId, TimelineState>> =
        HashMap::with_capacity(max_concurrent_sync);
    while let Some((sync_id, state_update)) = task_batch.next().await {
        debug!("Finished storage sync task for sync id {}", sync_id);
        if let Some(state_update) = state_update {
            let TimelineSyncId(tenant_id, timeline_id) = sync_id;
            new_timeline_states
                .entry(tenant_id)
                .or_default()
                .insert(timeline_id, state_update);
        }
    }

    new_timeline_states
}

async fn process_task<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_storage: Arc<S>,
    task: SyncTask,
    max_sync_errors: NonZeroU32,
    index: Arc<RwLock<RemoteTimelineIndex>>,
) -> Option<TimelineState> {
    if task.retries > max_sync_errors.get() {
        error!(
            "Evicting task {:?} that failed {} times, exceeding the error theshold",
            task.kind, task.retries
        );
        return Some(TimelineState::Evicted);
    }

    if task.retries > 0 {
        let seconds_to_wait = 2.0_f64.powf(task.retries as f64 - 1.0).min(30.0);
        debug!(
            "Waiting {} seconds before starting the task",
            seconds_to_wait
        );
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(seconds_to_wait)).await;
    }

    let sync_start = Instant::now();
    match task.kind {
        SyncKind::Download(download_data) => {
            let sync_status = download_timeline(
                config,
                index,
                remote_storage,
                task.sync_id,
                download_data,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, "download", sync_status);
            Some(TimelineState::AwaitsDownload)
        }
        SyncKind::Upload(layer_upload) => {
            let sync_status = upload_timeline_checkpoint(
                config,
                index,
                remote_storage,
                task.sync_id,
                layer_upload,
                task.retries + 1,
            )
            .await;
            register_sync_status(sync_start, "upload", sync_status);
            None
        }
    }
}

fn schedule_first_sync_tasks(
    index: &RemoteTimelineIndex,
    local_timeline_files: HashMap<TimelineSyncId, (TimelineMetadata, Vec<PathBuf>)>,
) -> HashMap<ZTenantId, HashMap<ZTimelineId, TimelineState>> {
    let mut initial_timeline_statuses: HashMap<ZTenantId, HashMap<ZTimelineId, TimelineState>> =
        HashMap::new();

    let mut new_sync_tasks =
        VecDeque::with_capacity(local_timeline_files.len().max(local_timeline_files.len()));

    for (sync_id, (local_metadata, local_files)) in local_timeline_files {
        let TimelineSyncId(tenant_id, timeline_id) = sync_id;
        match index.entry(&sync_id) {
            Some(index_entry) => {
                let timeline_status = compare_local_and_remote_timeline(
                    &mut new_sync_tasks,
                    sync_id,
                    local_metadata,
                    local_files,
                    index_entry,
                );
                initial_timeline_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, timeline_status);
            }
            None => {
                new_sync_tasks.push_back(SyncTask::new(
                    sync_id,
                    0,
                    SyncKind::Upload(NewCheckpoint {
                        layers: local_files,
                        metadata: local_metadata,
                    }),
                ));
                initial_timeline_statuses
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, TimelineState::Ready);
            }
        }
    }

    for TimelineSyncId(cloud_only_tenant_id, cloud_only_timeline_id) in index
        .all_ids()
        .filter(|remote_id| {
            initial_timeline_statuses
                .get(&remote_id.0)
                .and_then(|timelines| timelines.get(&remote_id.1))
                .is_none()
        })
        .collect::<Vec<_>>()
    {
        initial_timeline_statuses
            .entry(cloud_only_tenant_id)
            .or_default()
            .insert(cloud_only_timeline_id, TimelineState::CloudOnly);
    }

    new_sync_tasks.into_iter().for_each(|task| {
        sync_queue::push(task);
    });
    initial_timeline_statuses
}

fn compare_local_and_remote_timeline(
    new_sync_tasks: &mut VecDeque<SyncTask>,
    sync_id: TimelineSyncId,
    local_metadata: TimelineMetadata,
    local_files: Vec<PathBuf>,
    remote_entry: &IndexEntry,
) -> TimelineState {
    let local_lsn = local_metadata.ancestor_lsn();
    let uploads = remote_entry.uploaded_checkpoints();

    if !uploads.contains(&local_lsn) {
        new_sync_tasks.push_back(SyncTask::new(
            sync_id,
            0,
            SyncKind::Upload(NewCheckpoint {
                layers: local_files.clone(),
                metadata: local_metadata,
            }),
        ));
    }

    let uploads_count = uploads.len();
    let archives_to_skip: BTreeSet<ArchiveId> = uploads
        .into_iter()
        .filter(|upload_lsn| upload_lsn <= &local_lsn)
        .map(ArchiveId)
        .collect();
    if archives_to_skip.len() != uploads_count {
        new_sync_tasks.push_back(SyncTask::new(
            sync_id,
            0,
            SyncKind::Download(TimelineDownload {
                files_to_skip: Arc::new(local_files.into_iter().collect()),
                archives_to_skip,
            }),
        ));
        TimelineState::AwaitsDownload
    } else {
        TimelineState::Ready
    }
}

fn register_sync_status(sync_start: Instant, sync_name: &str, sync_status: Option<bool>) {
    let secs_elapsed = sync_start.elapsed().as_secs_f64();
    debug!("Processed a sync task in {} seconds", secs_elapsed);
    match sync_status {
        Some(true) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "success"]),
        Some(false) => IMAGE_SYNC_TIME.with_label_values(&[sync_name, "failure"]),
        None => return,
    }
    .observe(secs_elapsed)
}

async fn update_index_description<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
    index: &RwLock<RemoteTimelineIndex>,
    id: TimelineSyncId,
) -> anyhow::Result<RemoteTimeline> {
    let mut index_write = index.write().await;
    let full_index = match index_write.entry(&id) {
        None => bail!("Timeline not found for sync id {}", id),
        Some(IndexEntry::Full(_)) => bail!("Index is already populated for sync id {}", id),
        Some(IndexEntry::Description(description)) => {
            let mut archive_header_downloads = FuturesUnordered::new();
            for (&archive_id, description) in description {
                archive_header_downloads.push(async move {
                    let header = download_archive_header(storage, description)
                        .await
                        .map_err(|e| (e, archive_id))?;
                    Ok((archive_id, description.header_size, header))
                });
            }

            let mut full_index = RemoteTimeline::empty();
            while let Some(header_data) = archive_header_downloads.next().await {
                match header_data {
                        Ok((archive_id, header_size, header)) => full_index.update_archive_contents(archive_id.0, header, header_size),
                        Err((e, archive_id)) => bail!(
                            "Failed to download archive header for tenant {}, timeline {}, archive for Lsn {}: {}",
                            id.0, id.1, archive_id.0,
                            e
                        ),
                    }
            }
            full_index
        }
    };
    index_write.set_entry(id, IndexEntry::Full(full_index.clone()));
    Ok(full_index)
}

async fn download_archive_header<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
    description: &ArchiveDescription,
) -> anyhow::Result<ArchiveHeader> {
    let mut header_buf = std::io::Cursor::new(Vec::new());
    let remote_path = storage.storage_path(&description.download_path)?;
    storage
        .download_range(
            &remote_path,
            0,
            Some(description.header_size),
            &mut header_buf,
        )
        .await?;
    let header_buf = header_buf.into_inner();
    let header = read_archive_header(&description.archive_name, &mut header_buf.as_slice()).await?;
    Ok(header)
}

#[cfg(test)]
mod test_utils {
    use std::{collections::BTreeSet, fs};

    use super::*;
    use crate::{
        layered_repository::metadata::metadata_path, remote_storage::local_fs::LocalFs,
        repository::repo_harness::RepoHarness,
    };
    use zenith_utils::lsn::Lsn;

    #[track_caller]
    pub async fn ensure_correct_timeline_upload(
        harness: &RepoHarness,
        index: Arc<RwLock<RemoteTimelineIndex>>,
        remote_storage: Arc<LocalFs>,
        timeline_id: ZTimelineId,
        new_upload: NewCheckpoint,
    ) {
        let sync_id = TimelineSyncId(harness.tenant_id, timeline_id);
        upload_timeline_checkpoint(
            harness.conf,
            Arc::clone(&index),
            Arc::clone(&remote_storage),
            sync_id,
            new_upload.clone(),
            0,
        )
        .await;
        assert_index_descriptions(
            &index,
            collect_timeline_descriptions(remote_storage.as_ref())
                .await
                .unwrap(),
        )
        .await;

        let new_remote_timeline = expect_timeline(index.as_ref(), sync_id).await;
        let new_remote_lsn = new_remote_timeline
            .checkpoints()
            .max()
            .expect("Remote timeline should have an lsn after reupload");
        let upload_lsn = new_upload.metadata.disk_consistent_lsn();
        assert!(
            new_remote_lsn >= upload_lsn,
            "Remote timeline after upload should have the biggest Lsn out of all uploads"
        );
        assert!(
            new_remote_timeline.contains_archive(upload_lsn),
            "Should contain upload lsn among the remote ones"
        );

        let remote_files_after_upload = new_remote_timeline
            .stored_files(&harness.conf.timeline_path(&timeline_id, &harness.tenant_id));
        for new_uploaded_layer in &new_upload.layers {
            assert!(
                remote_files_after_upload.contains(new_uploaded_layer),
                "Remote files do not contain layer that should be uploaded: '{}'",
                new_uploaded_layer.display()
            );
        }

        assert_timeline_files_match(harness, timeline_id, new_remote_timeline);
    }

    pub async fn expect_timeline(
        index: &RwLock<RemoteTimelineIndex>,
        sync_id: TimelineSyncId,
    ) -> RemoteTimeline {
        if let Some(IndexEntry::Full(remote_timeline)) = index.read().await.entry(&sync_id) {
            remote_timeline.clone()
        } else {
            panic!(
                "Expect to have a full remote timeline in the index for sync id {}",
                sync_id
            )
        }
    }

    #[track_caller]
    pub async fn assert_index_descriptions(
        index: &RwLock<RemoteTimelineIndex>,
        expected_descriptions: HashMap<TimelineSyncId, BTreeMap<ArchiveId, ArchiveDescription>>,
    ) {
        let actual_entries_count = index.read().await.all_ids().count();
        assert_eq!(actual_entries_count, expected_descriptions.len());

        for (expected_sync_id, timeline_descriptions) in expected_descriptions {
            let index_read = index.read().await;
            let actual_timeline = index_read.entry(&expected_sync_id).expect(&format!(
                "Failed to find an expected timeline with id {} in the index",
                expected_sync_id
            ));
            let expected_lsns = timeline_descriptions
                .values()
                .map(|description| description.disk_consistent_lsn)
                .collect::<BTreeSet<_>>();
            assert_eq!(
                actual_timeline.uploaded_checkpoints(),
                expected_lsns,
                "Timline {} should have the same checkpoints uploaded",
                expected_sync_id,
            )
        }
    }

    pub fn assert_timeline_files_match(
        harness: &RepoHarness,
        remote_timeline_id: ZTimelineId,
        remote_timeline: RemoteTimeline,
    ) {
        let local_timeline_dir = harness.timeline_path(&remote_timeline_id);
        let local_paths = fs::read_dir(&local_timeline_dir)
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();
        let mut reported_remote_files = remote_timeline.stored_files(&local_timeline_dir);
        let local_metadata_path =
            metadata_path(harness.conf, remote_timeline_id, harness.tenant_id);
        let local_metadata = TimelineMetadata::from_bytes(
            &fs::read(&local_metadata_path)
                .expect("Failed to read metadata file when comparing remote and local image files"),
        )
        .expect(
            "Failed to parse metadata file contents when comparing remote and local image files",
        );
        assert!(
            remote_timeline.contains_archive(local_metadata.disk_consistent_lsn()),
            "Should contain local lsn among the remote ones after the upload"
        );
        reported_remote_files.insert(local_metadata_path);

        assert_eq!(
            local_paths, reported_remote_files,
            "Remote image files and local image files are different, missing locally: {:?}, missing remotely: {:?}",
            reported_remote_files.difference(&local_paths).collect::<Vec<_>>(),
            local_paths.difference(&reported_remote_files).collect::<Vec<_>>(),
        );

        if let Some(remote_file) = reported_remote_files.iter().next() {
            let actual_remote_paths = fs::read_dir(
                remote_file
                    .parent()
                    .expect("Remote files are expected to have their timeline dir as parent"),
            )
            .unwrap()
            .map(|dir| dir.unwrap().path())
            .collect::<BTreeSet<_>>();

            let unreported_remote_files = actual_remote_paths
                .difference(&reported_remote_files)
                .collect::<Vec<_>>();
            assert!(
                unreported_remote_files.is_empty(),
                "Unexpected extra remote files that were not listed: {:?}",
                unreported_remote_files
            )
        }
    }

    pub fn create_local_timeline(
        harness: &RepoHarness,
        timeline_id: ZTimelineId,
        filenames: &[&str],
        metadata: TimelineMetadata,
    ) -> anyhow::Result<NewCheckpoint> {
        let timeline_path = harness.timeline_path(&timeline_id);
        fs::create_dir_all(&timeline_path)?;

        let mut layers = Vec::with_capacity(filenames.len());
        for &file in filenames {
            let file_path = timeline_path.join(file);
            fs::write(&file_path, dummy_contents(file).into_bytes())?;
            layers.push(file_path);
        }

        fs::write(
            metadata_path(harness.conf, timeline_id, harness.tenant_id),
            metadata.to_bytes()?,
        )?;

        Ok(NewCheckpoint { layers, metadata })
    }

    fn dummy_contents(name: &str) -> String {
        format!("contents for {}", name)
    }

    pub fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        TimelineMetadata::new(disk_consistent_lsn, None, None, Lsn(0), Lsn(0), Lsn(0))
    }
}
