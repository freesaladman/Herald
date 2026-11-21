use std::f32::consts::TAU;
use std::fs::File;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use rodio::buffer::SamplesBuffer;
use rodio::source::Source;
use rodio::{Decoder, DeviceSinkBuilder, DeviceSinkError, MixerDeviceSink, Player};

const SAMPLE_RATE: u32 = 44_100;
const CHIME_TONES: [(f32, u32); 2] = [(880.0, 120), (1320.0, 130)];
const FADE_MS: u32 = 8;
const CHIME_GAIN: f32 = 0.5;
const MAX_FILE_PLAYBACK: Duration = Duration::from_secs(5);

/// One request to play the alert sound.
#[derive(Debug, Clone, PartialEq)]
pub struct PlayRequest {
    pub file: Option<PathBuf>,
    pub volume: f32,
}

/// Plays alert sounds on a dedicated thread, because rodio's output stream is not `Send`.
pub struct SoundPlayer {
    tx: Sender<PlayRequest>,
}

impl SoundPlayer {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        if let Err(e) = std::thread::Builder::new()
            .name("sound".into())
            .spawn(move || run(&rx))
        {
            tracing::warn!(error = %e, "could not start sound thread");
        }
        Self { tx }
    }

    /// Never blocks; the request is dropped when the sound thread is gone.
    pub fn play(&self, file: Option<PathBuf>, volume: f32) {
        let _ = self.tx.send(PlayRequest { file, volume });
    }
}

fn run(rx: &Receiver<PlayRequest>) {
    let mut sink: Option<MixerDeviceSink> = None;
    let mut current: Option<Player> = None;
    let failed = Arc::new(AtomicBool::new(false));
    let mut warned = false;
    while let Ok(request) = rx.recv() {
        if failed.swap(false, Ordering::Relaxed) {
            // The device went away or errored; reopen on this request.
            current = None;
            sink = None;
        }
        if sink.is_none() {
            match open_sink(&failed) {
                Ok(s) => sink = Some(s),
                Err(e) => {
                    if !warned {
                        tracing::warn!(error = %e, "no audio output device; alert sounds dropped");
                        warned = true;
                    }
                    continue;
                }
            }
        }
        if let Some(s) = &sink {
            if let Some(previous) = current.take() {
                previous.stop();
            }
            current = Some(play_on(s, &request));
        }
    }
}

fn open_sink(failed: &Arc<AtomicBool>) -> Result<MixerDeviceSink, DeviceSinkError> {
    let flag = Arc::clone(failed);
    let mut sink = DeviceSinkBuilder::from_default_device()?
        .with_error_callback(move |e| {
            tracing::warn!(error = %e, "audio stream error; will reopen the device");
            flag.store(true, Ordering::Relaxed);
        })
        .open_stream()?;
    sink.log_on_drop(false);
    Ok(sink)
}

fn play_on(sink: &MixerDeviceSink, request: &PlayRequest) -> Player {
    let player = Player::connect_new(sink.mixer());
    player.set_volume(clamp_volume(request.volume));
    let decoded = request.file.as_ref().and_then(|path| {
        File::open(path)
            .map_err(|e| tracing::warn!(path = %path.display(), error = %e, "sound file unreadable"))
            .ok()
            .and_then(|f| {
                Decoder::try_from(f)
                    .map_err(|e| {
                        tracing::warn!(path = %path.display(), error = %e, "sound file undecodable");
                    })
                    .ok()
            })
    });
    match decoded {
        Some(source) => player.append(source.take_duration(MAX_FILE_PLAYBACK)),
        None => player.append(chime_source()),
    }
    player
}

fn chime_source() -> SamplesBuffer {
    SamplesBuffer::new(
        NonZero::<u16>::MIN,
        NonZero::<u32>::new(SAMPLE_RATE).unwrap_or(NonZero::<u32>::MIN),
        chime_samples(SAMPLE_RATE),
    )
}

fn clamp_volume(volume: f32) -> f32 {
    if volume.is_nan() {
        0.0
    } else {
        volume.clamp(0.0, 1.0)
    }
}

/// Mono two-tone chime with a short fade in and out on each tone to avoid clicks.
fn chime_samples(sample_rate: u32) -> Vec<f32> {
    let rate = sample_rate as f32;
    let fade = (sample_rate * FADE_MS / 1000) as usize;
    let mut out = Vec::new();
    for (freq, ms) in CHIME_TONES {
        let len = (sample_rate * ms / 1000) as usize;
        for i in 0..len {
            let envelope = (i.min(len - 1 - i) as f32 / fade as f32).min(1.0);
            out.push((TAU * freq * i as f32 / rate).sin() * envelope * CHIME_GAIN);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chime_is_about_250ms_bounded_and_fades_at_both_ends() {
        let samples = chime_samples(SAMPLE_RATE);
        assert_eq!(samples.len(), (SAMPLE_RATE * 250 / 1000) as usize);
        let peak = samples.iter().fold(0.0_f32, |m, s| m.max(s.abs()));
        assert!(peak > 0.3 && peak <= CHIME_GAIN, "peak {peak}");
        assert!(samples[0].abs() < 0.01);
        assert!(samples[samples.len() - 1].abs() < 0.05);
    }

    #[test]
    fn clamp_volume_limits_to_unit_range() {
        assert_eq!(clamp_volume(-1.0), 0.0);
        assert_eq!(clamp_volume(3.0), 1.0);
        assert_eq!(clamp_volume(f32::NAN), 0.0);
        assert_eq!(clamp_volume(0.4), 0.4);
    }
}
