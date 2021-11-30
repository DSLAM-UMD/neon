use std::{borrow::Cow, collections::HashSet, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context};
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::{
    remote_storage::{
        storage_sync::{
            compression, index::IndexEntry, sync_queue, update_index_description, SyncKind,
            SyncTask,
        },
        RemoteStorage, TimelineSyncId,
    },
    PageServerConf,
};

use super::{
    index::{ArchiveId, RemoteTimeline, RemoteTimelineIndex},
    TimelineDownload,
};

pub(super) async fn download_timeline<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_assets: Arc<(S, RwLock<RemoteTimelineIndex>)>,
    sync_id: TimelineSyncId,
    mut download: TimelineDownload,
    retries: u32,
) -> Option<bool> {
    debug!("Downloading layers for sync id {}", sync_id);

    let index_read = remote_assets.1.read().await;
    let remote_timeline = match index_read.entry(&sync_id) {
        None => {
            error!("Cannot download: no timeline is present in the index for given ids");
            return None;
        }
        Some(IndexEntry::Full(remote_timeline)) => Cow::Borrowed(remote_timeline),
        Some(IndexEntry::Description(_)) => {
            drop(index_read);
            debug!("Found timeline description for the given ids, downloading the full index");
            match update_index_description(remote_assets.as_ref(), sync_id).await {
                Ok(remote_timeline) => Cow::Owned(remote_timeline),
                Err(e) => {
                    error!("Failed to download full timeline index: {:#}", e);
                    sync_queue::push(SyncTask::new(
                        sync_id,
                        retries,
                        SyncKind::Download(download),
                    ));
                    return Some(false);
                }
            }
        }
    };

    let mut archives_to_download = remote_timeline
        .checkpoints()
        .map(ArchiveId)
        .filter(|remote_archive| !download.archives_to_skip.contains(remote_archive))
        .collect::<Vec<_>>();

    let archives_total = archives_to_download.len();
    debug!("Downloading {} archives of a timeline", archives_total);

    let TimelineSyncId(tenant_id, timeline_id) = sync_id;
    while let Some(archive_id) = archives_to_download.pop() {
        match try_download_archive(
            Arc::clone(&remote_assets),
            config.timeline_path(&timeline_id, &tenant_id),
            remote_timeline.as_ref(),
            archive_id,
            Arc::clone(&download.files_to_skip),
        )
        .await
        {
            Err(e) => {
                let archives_left = archives_to_download.len();
                error!(
                    "Failed to download archive {:?} for tenant {} timeline {} : {:#}, requeueing the download ({} archives left out of {})",
                    archive_id, tenant_id, timeline_id, e, archives_left, archives_total
                );
                sync_queue::push(SyncTask::new(
                    sync_id,
                    retries,
                    SyncKind::Download(download),
                ));
                return Some(false);
            }
            Ok(()) => {
                debug!("Successfully downloaded archive {:?}", archive_id);
                download.archives_to_skip.insert(archive_id);
            }
        }
    }

    debug!("Finished downloading all timeline's archives");
    Some(true)
}

async fn try_download_archive<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    remote_assets: Arc<(S, RwLock<RemoteTimelineIndex>)>,
    timeline_dir: PathBuf,
    remote_timeline: &RemoteTimeline,
    archive_id: ArchiveId,
    files_to_skip: Arc<HashSet<PathBuf>>,
) -> anyhow::Result<()> {
    debug!("Downloading archive {:?}", archive_id);
    let remote_archive = remote_timeline
        .archive_data(archive_id)
        .ok_or_else(|| anyhow!("Archive {:?} not found in remote storage", archive_id))?;
    let (archive_header, header_size) = remote_timeline
        .restore_header(archive_id)
        .context("Failed to restore header when downloading an archive")?;

    compression::uncompress_file_stream_with_index(
        timeline_dir.clone(),
        files_to_skip,
        remote_archive.disk_consistent_lsn(),
        archive_header,
        header_size,
        move |mut archive_target, archive_name| async move {
            let archive_local_path = timeline_dir.join(&archive_name);
            let remote_storage = &remote_assets.0;
            remote_storage
                .download_range(
                    &remote_storage.storage_path(&archive_local_path)?,
                    header_size,
                    None,
                    &mut archive_target,
                )
                .await
        },
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::tempdir;
    use tokio::fs;
    use zenith_utils::lsn::Lsn;

    use crate::{
        remote_storage::{
            local_fs::LocalFs,
            storage_sync::{
                collect_timeline_descriptions,
                test_utils::{
                    assert_index_descriptions, assert_timeline_files_match, create_local_timeline,
                    dummy_metadata, ensure_correct_timeline_upload, expect_timeline,
                },
            },
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[tokio::test]
    async fn test_download_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_download_timeline")?;
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let index = RwLock::new(RemoteTimelineIndex::new(
            collect_timeline_descriptions(&storage).await.unwrap(),
        ));
        let remote_assets = Arc::new((storage, index));
        let storage = &remote_assets.0;
        let index = &remote_assets.1;

        let regular_timeline_path = repo_harness.timeline_path(&TIMELINE_ID);
        let regular_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            dummy_metadata(Lsn(0x30)),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            regular_timeline,
        )
        .await;
        fs::remove_dir_all(&regular_timeline_path).await?;
        let remote_regular_timeline = expect_timeline(index, sync_id).await;

        download_timeline(
            repo_harness.conf,
            Arc::clone(&remote_assets),
            sync_id,
            TimelineDownload {
                files_to_skip: Arc::new(HashSet::new()),
                archives_to_skip: BTreeSet::new(),
            },
            0,
        )
        .await;
        assert_index_descriptions(index, collect_timeline_descriptions(storage).await.unwrap())
            .await;
        assert_timeline_files_match(&repo_harness, TIMELINE_ID, remote_regular_timeline);

        Ok(())
    }
}
