// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Maika Namuo

#[cfg(target_os = "linux")]
use rustix::fs::inotify::{self, CreateFlags, WatchFlags as W};
#[cfg(target_os = "linux")]
use rustix::io::Errno;
use std::convert::Infallible;
#[cfg(target_os = "linux")]
use std::ffi::{OsStr, OsString};
use std::hash::{Hash, Hasher};
use std::iter;
#[cfg(target_os = "linux")]
use std::mem::MaybeUninit;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io, thread};

// so that several steps can finish before a reload
const SETTLE: Duration = Duration::from_millis(100);
#[cfg(target_os = "macos")]
const POLL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct SettingsWatcher {
    dirs: Arc<[PathBuf]>,
    sender: async_channel::Sender<()>,
    receiver: async_channel::Receiver<()>,
}

impl SettingsWatcher {
    pub(super) fn new(dirs: [PathBuf; 2]) -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        Self {
            dirs: Arc::new(dirs),
            sender,
            receiver,
        }
    }

    pub(super) fn wake(&self) {
        let _ = self.sender.try_send(());
    }

    pub(super) fn start(&self) {
        let (dirs, sender) = (Arc::clone(&self.dirs), self.sender.clone());
        if let Err(err) = thread::Builder::new()
            .name("openmeters-settings-watch".into())
            .spawn(move || {
                let Err(err) = watch_loop(&dirs, &sender);
                tracing::warn!("[settings] live reload unavailable: {err}");
            })
        {
            tracing::warn!("[settings] failed to start watcher: {err}");
        }
    }
}

impl Hash for SettingsWatcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dirs.hash(state);
    }
}

/// the stream for `Subscription::run_with`.
pub fn changes(watcher: &SettingsWatcher) -> async_channel::Receiver<()> {
    watcher.receiver.clone()
}

// Runs for the life of the app, since the settings keep the channel open.
#[cfg(target_os = "linux")]
fn watch_loop(dirs: &[PathBuf], sender: &async_channel::Sender<()>) -> io::Result<Infallible> {
    // Reads shouldn't wake the watcher
    let mask = W::CLOSE_WRITE | W::MOVED_TO | W::MOVED_FROM | W::CREATE | W::DELETE | W::ONLYDIR;
    let mut buf = [MaybeUninit::uninit(); 4096];
    let mut warned = false;
    loop {
        let inotify = inotify::init(CreateFlags::CLOEXEC)?;
        let mut watched = Vec::new();
        let mut watch_dir = |dir: &Path, entry: Option<&OsStr>| -> Result<(), Errno> {
            let wd = inotify::add_watch(&inotify, dir, mask)?;
            watched.push((wd, entry.map(OsStr::to_owned)));
            Ok(())
        };
        for dir in dirs {
            let mut entry = None;
            let added = dir.ancestors().find_map(|path| {
                let added = watch_dir(path, entry);
                entry = path.file_name();
                (added != Err(Errno::NOENT)).then_some(added)
            });
            if let Some(Err(err)) = added
                && !std::mem::replace(&mut warned, true)
            {
                tracing::warn!("[settings] cannot watch {dir:?}: {err}");
            }
            for link in link_entries(dir) {
                let Some(parent) = link.parent() else {
                    continue;
                };
                if let Err(err) = watch_dir(parent, link.file_name()) {
                    tracing::debug!("[settings] cannot watch {link:?}: {err}");
                }
            }
        }
        // A full channel already holds a wake.
        let _ = sender.try_send(());
        let mut events = inotify::Reader::new(&inotify, &mut buf);
        loop {
            match events.next() {
                Ok(event) if concerns(&watched, &event) => break,
                Ok(_) | Err(Errno::INTR) => {}
                Err(err) => return Err(err.into()),
            }
        }
        thread::sleep(SETTLE);
    }
}

#[cfg(target_os = "macos")]
fn watch_loop(dirs: &[PathBuf], sender: &async_channel::Sender<()>) -> io::Result<Infallible> {
    let _ = sender.try_send(());
    let mut previous = snapshot(dirs);
    loop {
        thread::sleep(POLL);
        let current = snapshot(dirs);
        if current != previous {
            thread::sleep(SETTLE);
            previous = snapshot(dirs);
            let _ = sender.try_send(());
        }
    }
}

#[cfg(target_os = "macos")]
fn snapshot(dirs: &[PathBuf]) -> Vec<(PathBuf, Option<(std::time::SystemTime, u64, bool, bool)>)> {
    use std::collections::BTreeSet;

    let mut paths = BTreeSet::new();
    for dir in dirs {
        paths.extend(dir.ancestors().map(Path::to_owned));
        if let Ok(entries) = fs::read_dir(dir) {
            paths.extend(entries.flatten().map(|entry| entry.path()));
        }
        for linked in link_entries(dir) {
            paths.insert(linked.clone());
            if let Ok(entries) = fs::read_dir(&linked) {
                paths.extend(entries.flatten().map(|entry| entry.path()));
            }
        }
    }
    paths
        .into_iter()
        .map(|path| {
            let metadata = fs::symlink_metadata(&path).ok().map(|metadata| {
                (
                    metadata
                        .modified()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                    metadata.len(),
                    metadata.is_dir(),
                    metadata.file_type().is_symlink(),
                )
            });
            (path, metadata)
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn concerns(watched: &[(i32, Option<OsString>)], event: &inotify::Event<'_>) -> bool {
    event.file_name().is_none_or(|name| {
        watched.iter().any(|(wd, entry)| {
            *wd == event.wd()
                && (entry.as_deref()).is_none_or(|entry| entry.as_bytes() == name.to_bytes())
        })
    })
}

fn link_entries(dir: &Path) -> Vec<PathBuf> {
    let entries = fs::read_dir(dir).into_iter().flatten().flatten();
    iter::once(dir.to_owned())
        .chain(entries.map(|entry| entry.path()))
        .flat_map(|path| {
            let (links, target) = super::resolve_links(&path);
            let linked = !links.is_empty();
            links.into_iter().chain(linked.then_some(target))
        })
        .collect()
}
