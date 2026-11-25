use std::cmp::{Ordering, max};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::mem;
use std::num::{NonZeroU8, NonZeroU64};
use std::path::{Path, PathBuf};

use axum::BoxError;
use chrono::{DateTime, Local, Utc};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use reliquary_archiver::export::fribbels::Export;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use tracing::info;

use crate::pull_history::GachaPull;

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportedState {
    source: String,
    build: String,
    version: u32,
    format_version: u32,
    timestamp: DateTime<Utc>,
    consider_compression: bool,
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub(crate) exports: HashMap<u32, GlorifiedExport>,
    #[serde(skip)]
    pub dirty: bool,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GlorifiedExport {
    pub(crate) export: Option<Export>,
    pub(crate) pulls_export: PullsExport,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PullsExport {
    pulls: Vec<GachaPull>,
    last_token: Option<(String, DateTime<Utc>)>,
    last_fetched: Option<DateTime<Utc>>,
}

impl PartialEq for GachaPull {
    fn eq(&self, other: &Self) -> bool {
        other.id == self.id
    }
}

impl PartialOrd for GachaPull {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for GachaPull {}
impl Ord for GachaPull {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl ExportedState {
    pub fn new(source: String, build: String, version: u32, timestamp: DateTime<Utc>, consider_compression: bool) -> Self {
        Self {
            source,
            build,
            version,
            format_version: 0,
            timestamp,
            consider_compression,
            exports: Default::default(),
            dirty: false,
        }
    }

    pub fn read_from_file(path: &Path) -> Result<Self, BoxError> {
        if !path.exists() {
            Err(format!("File {} does not exist", path.display()).into())
        } else if path.is_file() {
            if path.extension().is_some_and(|ext| ext == "json") {
                let compression_advised = std::fs::metadata(path)?.len() > 500_000;
                let mut state: ExportedState =
                    serde_json::from_reader(std::fs::File::open(path).map_err(BoxError::from)?).map_err(BoxError::from)?;
                state.consider_compression = compression_advised;
                Ok(state)
            } else if path
                .file_name()
                .is_some_and(|file_name| file_name.to_string_lossy().ends_with(".json.gz"))
            {
                serde_json::from_reader(std::fs::File::open(path).map_err(BoxError::from).map(GzDecoder::new)?).map_err(BoxError::from)
            } else {
                Err(format!("File {} is not a json file", path.display()).into())
            }
        } else {
            Err(format!("File {} is not a json file", path.display()).into())
        }
    }

    pub fn write_to_file(&self, path: &Path) -> Option<BoxError> {
        let file = match std::fs::File::create(path).map_err(BoxError::from) {
            Ok(file) => file,
            Err(err) => return Some(err),
        };
        if self.consider_compression {
            serde_json::to_writer(GzEncoder::new(file, Compression::default()), self)
        } else {
            serde_json::to_writer(file, self)
        }
        .map_err(BoxError::from)
        .err()
    }

    pub fn read_from_dir(path: &Path) -> Result<Option<Self>, BoxError> {
        if !path.exists() {
            Err(format!("File {} does not exist", path.display()).into())
        } else if path.is_dir() {
            let file = Self::list_state_files(path)?.max_by(|a, b| {
                a.file_name()
                    .expect("the entry should have a filename")
                    .cmp(b.file_name().expect("the entry should have a filename"))
            });
            if let Some(path) = file {
                Self::read_from_file(path.as_path()).map(Some)
            } else {
                Ok(None)
            }
        } else if path.is_file() {
            Self::read_from_file(path).map(Some)
        } else {
            unreachable!("Technically should not happen")
        }
    }

    fn list_state_files(path: &Path) -> Result<impl Iterator<Item = PathBuf>, BoxError> {
        Ok(std::fs::read_dir(path)
            .map_err(BoxError::from)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                if !(p.exists() && p.is_file()) {
                    return false;
                }
                let Some(name) = p.file_name().map(OsStr::to_string_lossy) else {
                    info!("File {} has no name", p.display());
                    return false;
                };
                (name.ends_with(".json.gz") || name.ends_with(".json")) && name.starts_with("reliquary-state-")
            }))
    }

    #[must_use]
    pub fn write_to_dir(&self, dir_path: &Path, max_files: u8) -> Option<BoxError> {
        let ext = if self.consider_compression { "json.gz" } else { "json" };
        let path = dir_path.to_path_buf().join(format!(
            "reliquary-state-{}.{ext}",
            Local::now().to_utc().format("%Y-%m-%dT%H-%M-%S")
        ));

        let err = self.write_to_file(&path);

        if err.is_none() {
            if max_files == 0 {
                return None;
            }
            let mut files = match Self::list_state_files(dir_path) {
                Ok(iter) => iter.collect::<Vec<PathBuf>>(),
                Err(err) => return Some(err),
            };
            files.sort_by(|a, b| {
                a.file_name()
                    .expect("the entry should have a filename")
                    .cmp(b.file_name().expect("the entry should have a filename"))
            });
            for file in files.iter().take(files.len().saturating_sub(max_files as usize)) {
                if let Err(err) = std::fs::remove_file(file) {
                    return Some(err.into());
                }
            }
            None
        } else {
            err
        }
    }

    pub fn get_or_create_export(&mut self, uid: u32) -> &mut GlorifiedExport {
        self.dirty = true;
        self.exports.entry(uid).or_insert_with(|| GlorifiedExport {
            export: None,
            pulls_export: Default::default(),
        })
    }

    pub fn set_export(&mut self, uid: u32, export: Export) {
        self.get_or_create_export(uid).export = Some(export);
        self.dirty = true;
    }

    pub fn stage_pulls(&mut self, uid: u32, pulls: Vec<GachaPull>) {
        self.get_or_create_export(uid).pulls_export.pulls.extend(pulls);
        self.dirty = true;
    }
    pub fn commit_pulls(&mut self, uid: u32, pulls: Vec<GachaPull>) {
        self.dirty = true;
        let export = self.get_or_create_export(uid);

        let stored_pulls = mem::take(&mut export.pulls_export.pulls);
        if stored_pulls.is_empty() {
            export.pulls_export.pulls = pulls;
            return;
        }

        let (mut commited, staged): (Vec<_>, Vec<_>) = stored_pulls.into_iter().partition(|i| i.id.is_some());

        let max_pull_time = pulls.iter().max().map(|c| c.time).expect("commiting pulls shouldn't be empty");
        let min_pull_time = pulls.iter().min().map(|c| c.time).expect("commiting pulls shouldn't be empty");

        let commited_len = commited.len();
        commited.extend(pulls);
        commited.sort();
        commited.dedup();
        info!(
            "Added {} pulls to commited pulls, now amounting to {} commited pulls",
            commited.len() - commited_len,
            commited.len()
        );

        let (future_pulls, unresolved_old_pulls): (Vec<_>, Vec<_>) = staged
            .into_iter()
            .filter(|i| i.time < min_pull_time || max_pull_time < i.time)
            .partition(|p| max_pull_time < p.time);
        if !unresolved_old_pulls.is_empty() {
            info!(
                "There are {} unresolved pulls, generating sequential ids for them.",
                unresolved_old_pulls.len()
            );

            commited.extend(unresolved_old_pulls);
            commited.sort_by(|a, b| a.time.cmp(&b.time).then(a.id.cmp(&b.id)));
            if let Some(f) = commited.first_mut().filter(|f| f.id.is_none()) {
                f.id = Some(NonZeroU64::new(1).unwrap())
            }
            for i in 1..commited.len() {
                let previous_id = commited[i - 1].id.expect("each previous id should exist already");
                if let Some(f) = commited.get_mut(i).filter(|f| f.id.is_none()) {
                    f.id = Some(previous_id.checked_add(1).unwrap())
                }
            }
        }
        commited.extend(future_pulls);
        export.pulls_export.pulls = commited;
    }
}
