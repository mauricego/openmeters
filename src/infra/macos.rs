// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Maika Namuo

//! Process audio capture through Apple's Core Audio process taps.

use crate::domain::routing::{CaptureConfig, CaptureMode, StreamIdentity};
use crate::dsp::{AudioFormat, SampleRate};
use crate::infra::audio_transport::{CaptureWriter, StreamStatus};
use crate::infra::{ApplicationView, CaptureView};
use crate::util::unpoison;
use coreaudio::{
    AppTapAggregateDevice, AudioObject, DEVICE_NAME, DEVICE_UID, Device, IOProc, OBJECT_NAME,
    PROCESS_BUNDLE_ID, PROCESS_IS_RUNNING_OUTPUT, PROCESS_PID, Process, ProcessTap, Scope, System,
    TAP_FORMAT, TapMute,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const REFRESH_INTERVAL: Duration = Duration::from_millis(750);
const RETRY_INTERVAL: Duration = Duration::from_secs(3);

enum Command {
    Configure(CaptureConfig),
    Shutdown,
}

#[derive(Default)]
struct PublicState {
    alive: AtomicBool,
    view: RwLock<Arc<CaptureView>>,
}

impl PublicState {
    fn publish(&self, view: CaptureView) {
        let mut current = unpoison(self.view.write());
        if **current != view {
            *current = Arc::new(view);
        }
    }

    fn view(&self) -> Arc<CaptureView> {
        unpoison(self.view.read()).clone()
    }
}

#[derive(Clone)]
pub struct CaptureControl {
    commands: mpsc::Sender<Command>,
    public: Arc<PublicState>,
}

impl CaptureControl {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self {
            commands: mpsc::channel().0,
            public: Arc::default(),
        }
    }

    pub fn configure(&self, config: CaptureConfig) -> bool {
        self.commands.send(Command::Configure(config)).is_ok()
    }

    pub fn view(&self) -> Arc<CaptureView> {
        self.public.view()
    }

    pub fn is_alive(&self) -> bool {
        self.public.alive.load(Ordering::Acquire)
    }
}

pub struct AudioBackend {
    commands: mpsc::Sender<Command>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AudioBackend {
    pub fn start(
        config: CaptureConfig,
    ) -> io::Result<(Self, CaptureControl, crate::infra::AudioReader)> {
        let (writer, audio) = crate::infra::audio_transport::channel();
        let writer = Arc::new(Mutex::new(writer));
        let (commands, receiver) = mpsc::channel();
        let public = Arc::new(PublicState::default());
        let control = CaptureControl {
            commands: commands.clone(),
            public: Arc::clone(&public),
        };
        let thread_writer = Arc::clone(&writer);
        let thread_public = Arc::clone(&public);
        let thread = thread::Builder::new()
            .name("openmeters-coreaudio".into())
            .spawn(move || run(receiver, config, thread_writer, thread_public))?;
        Ok((
            Self {
                commands,
                thread: Some(thread),
            },
            control,
            audio,
        ))
    }

    pub fn shutdown(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        let _ = self.commands.send(Command::Shutdown);
        if thread.join().is_err() {
            error!("[capture] Core Audio backend thread panicked");
        }
        info!("[capture] Core Audio backend stopped");
    }
}

impl Drop for AudioBackend {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct ProcessInfo {
    object: AudioObject<Process>,
    identity: StreamIdentity,
    label: Arc<str>,
    active: bool,
}

struct DeviceInfo {
    object: AudioObject<Device>,
    name: Arc<str>,
    uid: String,
}

#[derive(PartialEq, Eq)]
struct RouteKey {
    mode: CaptureMode,
    process_ids: Vec<u32>,
    device_uid: Option<String>,
}

fn run(
    receiver: mpsc::Receiver<Command>,
    mut config: CaptureConfig,
    writer: Arc<Mutex<CaptureWriter>>,
    public: Arc<PublicState>,
) {
    public.alive.store(true, Ordering::Release);
    let system = AudioObject::<System>::default();
    let mut session: Option<CaptureSession> = None;
    let mut route: Option<RouteKey> = None;
    let mut last_attempt = Instant::now();
    let mut stopping = false;

    while !stopping {
        match receiver.recv_timeout(REFRESH_INTERVAL) {
            Ok(Command::Configure(next)) => {
                config = next;
                route = None;
            }
            Ok(Command::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                stopping = true;
                continue;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        let (processes, devices, default_device) = match enumerate(&system) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!("[capture] Core Audio enumeration failed: {err}");
                continue;
            }
        };
        public.publish(capture_view(
            &config,
            &processes,
            &devices,
            default_device.as_ref(),
        ));

        let device = selected_device(&config, &devices, default_device.as_ref());
        let selected: Vec<_> = processes
            .iter()
            .filter(|process| process.active)
            .filter(|process| {
                config.mode == CaptureMode::Device
                    || !config.disabled_streams.contains(&process.identity)
            })
            .collect();
        let process_ids = selected
            .iter()
            .map(|process| process.object.id())
            .collect::<Vec<_>>();
        let next_route = RouteKey {
            mode: config.mode,
            process_ids,
            device_uid: (config.mode == CaptureMode::Device)
                .then(|| device.map(|device| device.uid.clone()))
                .flatten(),
        };
        let changed = route.as_ref() != Some(&next_route);
        let retry = session.is_none()
            && !next_route.process_ids.is_empty()
            && last_attempt.elapsed() >= RETRY_INTERVAL;
        if !changed && !retry {
            continue;
        }

        route = Some(next_route);
        drop(session.take());
        let mut capture_writer = unpoison(writer.lock());
        capture_writer.clear_format();
        capture_writer.set_status(if selected.is_empty() || device_missing(&config, device) {
            StreamStatus::Paused
        } else {
            StreamStatus::Starting
        });
        drop(capture_writer);

        if selected.is_empty() || device_missing(&config, device) {
            continue;
        }
        last_attempt = Instant::now();
        match CaptureSession::new(&config, &selected, device, Arc::clone(&writer)) {
            Ok(next) => {
                session = Some(next);
                let mut capture_writer = unpoison(writer.lock());
                capture_writer.mark_reconnect();
                capture_writer.set_status(StreamStatus::Streaming);
            }
            Err(err) => {
                warn!("[capture] unable to start Core Audio process tap: {err}");
                unpoison(writer.lock()).disconnect();
            }
        }
    }

    drop(session.take());
    let mut capture_writer = unpoison(writer.lock());
    capture_writer.set_status(StreamStatus::Stopped);
    public.alive.store(false, Ordering::Release);
}

fn enumerate(
    system: &AudioObject<System>,
) -> Result<(Vec<ProcessInfo>, Vec<DeviceInfo>, Option<DeviceInfo>), coreaudio::CoreAudioError> {
    let mut processes = Vec::new();
    for object in system.processes()? {
        let active = object
            .get_property(PROCESS_IS_RUNNING_OUTPUT)
            .unwrap_or(false);
        let pid = object.get_property(PROCESS_PID).unwrap_or_default();
        let bundle = object
            .get_property(PROCESS_BUNDLE_ID)
            .ok()
            .filter(|id| !id.is_empty());
        if !active && bundle.is_none() {
            continue;
        }
        let (identity, label): (String, Arc<str>) = match bundle {
            Some(bundle) => {
                let label = object
                    .get_property(OBJECT_NAME)
                    .ok()
                    .filter(|name: &String| !name.is_empty())
                    .unwrap_or_else(|| bundle.clone());
                (format!("macos:bundle:{bundle}"), Arc::from(label))
            }
            None => {
                let label = format!("Process {pid}");
                (format!("macos:pid:{pid}"), Arc::from(label))
            }
        };
        processes.push(ProcessInfo {
            object,
            identity: StreamIdentity(identity.into()),
            label,
            active,
        });
    }
    processes.sort_by(|left, right| left.identity.cmp(&right.identity));

    let devices = system
        .devices_with_scope(Scope::Output)?
        .into_iter()
        .filter_map(|object| {
            let name = object.get_property(DEVICE_NAME).ok()?;
            let uid = object.get_property(DEVICE_UID).ok()?;
            Some(DeviceInfo {
                object,
                name: Arc::from(name),
                uid,
            })
        })
        .collect::<Vec<_>>();
    let default_uid = system
        .current_device(Scope::Output)
        .ok()
        .and_then(|device| device.get_property(DEVICE_UID).ok());
    let default_device =
        default_uid.and_then(|uid| devices.iter().find(|device| device.uid == uid));
    let default_device = default_device.map(|device| DeviceInfo {
        object: device.object,
        name: Arc::clone(&device.name),
        uid: device.uid.clone(),
    });
    Ok((processes, devices, default_device))
}

fn capture_view(
    config: &CaptureConfig,
    processes: &[ProcessInfo],
    devices: &[DeviceInfo],
    default_device: Option<&DeviceInfo>,
) -> CaptureView {
    let mut applications = BTreeMap::new();
    for process in processes {
        applications
            .entry(process.identity.clone())
            .and_modify(|application: &mut ApplicationView| {
                application.active |= process.active;
            })
            .or_insert_with(|| ApplicationView {
                identity: process.identity.clone(),
                label: Arc::clone(&process.label),
                active: process.active,
            });
    }
    let selected = selected_device(config, devices, default_device);
    let selected_device = selected.map(|device| Arc::clone(&device.name));
    let devices = devices
        .iter()
        .map(|device| Arc::clone(&device.name))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    CaptureView {
        applications: applications.into_values().collect::<Vec<_>>().into(),
        devices: devices.into(),
        default_sink: default_device
            .map(|device| Arc::clone(&device.name))
            .unwrap_or_else(|| Arc::from("Unavailable")),
        selected_device,
    }
}

// Device choices use their display name in persisted settings; this also keeps existing
// settings portable when a user's Core Audio device UID changes after a hardware update.
fn selected_device<'a>(
    config: &CaptureConfig,
    devices: &'a [DeviceInfo],
    default_device: Option<&'a DeviceInfo>,
) -> Option<&'a DeviceInfo> {
    match config.device.as_deref() {
        Some(name) => devices.iter().find(|device| device.name.as_ref() == name),
        None => default_device,
    }
}

fn device_missing(config: &CaptureConfig, device: Option<&DeviceInfo>) -> bool {
    config.mode == CaptureMode::Device && device.is_none()
}

static NEXT_TAP_ID: AtomicU64 = AtomicU64::new(1);

struct CaptureSession {
    // Field drop order is significant: stop callbacks before destroying the aggregate and tap.
    _io_proc: IOProc,
    _aggregate: AppTapAggregateDevice,
    _tap: ProcessTap,
}

impl CaptureSession {
    fn new(
        config: &CaptureConfig,
        processes: &[&ProcessInfo],
        device: Option<&DeviceInfo>,
        writer: Arc<Mutex<CaptureWriter>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let objects = processes
            .iter()
            .map(|process| process.object)
            .collect::<Vec<_>>();
        let id = NEXT_TAP_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!("OpenMeters {id}");
        let tap = if config.mode == CaptureMode::Applications {
            ProcessTap::stereo_mixdown(&objects, &name, TapMute::Unmuted)?
        } else {
            let device =
                device.ok_or_else(|| io::Error::other("selected output device is unavailable"))?;
            ProcessTap::device_stream(&objects, &device.uid, 0, &name, TapMute::Unmuted)?
        };
        let tap_format = tap.tap().get_property(TAP_FORMAT)?;
        let channels = (tap_format.channels_per_frame() as usize).clamp(1, 8);
        let rate = tap_format.sample_rate().round() as u32;
        let sample_rate = SampleRate::new(rate).unwrap_or(SampleRate::DEFAULT);
        let format = AudioFormat::fallback(channels, sample_rate);
        {
            let mut writer = unpoison(writer.lock());
            writer.set_format(format);
        }

        let aggregate = AppTapAggregateDevice::with_tap(
            &name,
            &format!("org.openmeters.capture.{id}"),
            tap.uid(),
        )?;
        let device = aggregate.device();
        let callback_writer = Arc::clone(&writer);
        let mut interleaved = Vec::new();
        let mut io_proc = device.add_io_proc(Scope::Input, move |buffers| {
            if buffers.len() == 1 && buffers[0].get_channels() as usize == channels {
                unpoison(callback_writer.lock()).push_f32(&buffers[0].data);
                return;
            }
            let frames = buffers
                .iter()
                .map(|buffer| buffer.get_frame_count() as usize)
                .min()
                .unwrap_or(0);
            if frames == 0 {
                return;
            }
            interleaved.clear();
            interleaved.reserve(frames * channels);
            for frame in 0..frames {
                let frame_start = interleaved.len();
                for buffer in buffers.iter() {
                    let buffer_channels = buffer.get_channels() as usize;
                    let start = frame * buffer_channels;
                    let end = (start + buffer_channels).min(buffer.data.len());
                    let available = channels.saturating_sub(interleaved.len() - frame_start);
                    interleaved.extend_from_slice(&buffer.data[start..end.min(start + available)]);
                }
                if interleaved.len() - frame_start < channels {
                    interleaved.resize(frame_start + channels, 0.0);
                }
            }
            unpoison(callback_writer.lock()).push_f32(&interleaved);
        })?;
        io_proc.play()?;
        Ok(Self {
            _io_proc: io_proc,
            _aggregate: aggregate,
            _tap: tap,
        })
    }
}
