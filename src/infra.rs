// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Maika Namuo

pub(crate) mod audio_transport;

#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(target_os = "linux")]
pub mod pipewire;

#[cfg(target_os = "macos")]
pub use macos::{AudioBackend, CaptureControl};
#[cfg(target_os = "linux")]
pub use pipewire::{AudioBackend, CaptureControl};

pub use audio_transport::{AudioReader, CapturedSpan};
pub(crate) use audio_transport::{AudioWake, audio_wake};

use crate::domain::routing::StreamIdentity;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationView {
    pub identity: StreamIdentity,
    pub label: Arc<str>,
    pub active: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CaptureView {
    pub applications: Arc<[ApplicationView]>,
    pub devices: Arc<[Arc<str>]>,
    pub default_sink: Arc<str>,
    pub selected_device: Option<Arc<str>>,
}

#[cfg(test)]
pub(crate) fn test_audio_reader() -> AudioReader {
    audio_transport::channel().1
}
