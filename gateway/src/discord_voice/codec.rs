use anyhow::{Context as _, Result, ensure};
use opus2::{Application, Channels, Decoder, Encoder};
use std::{collections::VecDeque, time::Duration};
use tokio::time::Instant;

pub(super) const SILENCE: &[u8; 3] = &[0xf8, 0xff, 0xfe];
const SAMPLES: usize = 480;
const FRAME: usize = SAMPLES * 2;
const MAX_PLAYBACK: usize = 24_000 * 2 * 20;
const TAIL: Duration = Duration::from_millis(40);

pub(super) struct Audio {
    decoder: Decoder,
    encoder: Encoder,
    queue: VecDeque<u8>,
    silence: u8,
    last_audio: Instant,
    last_speech: Option<Instant>,
    previous: Option<i16>,
}

impl Audio {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            decoder: Decoder::new(24_000, Channels::Mono)
                .context("Discord voice decoder unavailable")?,
            encoder: Encoder::new(48_000, Channels::Stereo, Application::Voip)
                .context("Discord voice encoder unavailable")?,
            queue: VecDeque::new(),
            silence: 0,
            last_audio: Instant::now(),
            last_speech: None,
            previous: None,
        })
    }

    pub(super) fn decode(&mut self, packet: &[u8]) -> Result<Option<Vec<u8>>> {
        if packet == SILENCE {
            self.last_speech = None;
            return Ok(None);
        }
        // Opus permits up to 120 ms; the decoder resamples and downmixes to mono directly.
        let mut samples = [0i16; SAMPLES * 6];
        let count = self
            .decoder
            .decode(packet, &mut samples, false)
            .context("Discord voice Opus frame could not be decoded")?;
        let pcm: Vec<u8> = samples[..count]
            .iter()
            .flat_map(|sample| sample.to_le_bytes())
            .collect();
        let now = Instant::now();
        if self
            .last_speech
            .is_none_or(|last| now.duration_since(last) >= Duration::from_millis(100))
        {
            self.queue.clear();
            self.previous = None;
            self.encoder
                .reset_state()
                .context("Discord voice encoder reset failed")?;
        }
        self.last_speech = Some(now);
        Ok(Some(pcm))
    }

    pub(super) fn push(&mut self, pcm: &[u8]) -> Result<()> {
        ensure!(
            pcm.len() <= MAX_PLAYBACK.saturating_sub(self.queue.len()),
            "Discord voice playback exceeded its 20-second buffer"
        );
        if !pcm.is_empty() {
            self.queue.extend(pcm);
            self.last_audio = Instant::now();
        }
        Ok(())
    }

    pub(super) fn playback(&mut self, buf: &mut [u8]) -> Result<Option<(usize, bool)>> {
        let available = FRAME.min(self.queue.len() / 2 * 2);
        if available == FRAME || (available > 0 && self.last_audio.elapsed() >= TAIL) {
            let mut input = [0i16; 4 * SAMPLES];
            let mut bytes = self.queue.drain(..available);
            for frames in input.chunks_exact_mut(4) {
                let current = match (bytes.next(), bytes.next()) {
                    (Some(lo), Some(hi)) => i16::from_le_bytes([lo, hi]),
                    _ => 0,
                };
                // The sum is guaranteed in [-65536,65534], and rounded average fits i16.
                let average =
                    (i32::from(self.previous.unwrap_or(current)) + i32::from(current) + 1)
                        .div_euclid(2);
                let interpolated =
                    i16::try_from(average).context("Discord voice resampling overflow")?;
                frames.copy_from_slice(&[interpolated, interpolated, current, current]);
                self.previous = Some(current);
            }
            self.silence = 5;
            let size = self
                .encoder
                .encode(&input, buf)
                .context("Discord voice PCM encoding failed")?;
            return Ok(Some((size, true)));
        }
        if self.silence != 0 {
            self.silence -= 1;
            buf[..SILENCE.len()].copy_from_slice(SILENCE);
            return Ok(Some((SILENCE.len(), self.silence != 0)));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_provider_pcm_becomes_real_stereo_opus_and_owner_audio_decodes_to_mono() {
        let tone: Vec<u8> = (0..SAMPLES)
            .flat_map(|i| (if i % 48 < 24 { 6_000i16 } else { -6_000i16 }).to_le_bytes())
            .collect();
        let mut bridge = Audio::new().unwrap();
        bridge.push(&tone[..471]).unwrap();
        assert!(bridge.playback(&mut [0; 2048]).unwrap().is_none());
        bridge.push(&tone[471..]).unwrap();
        let mut packet = [0; 2048];
        let (size, speaking) = bridge.playback(&mut packet).unwrap().unwrap();
        assert!(speaking);
        let mut decoder = Decoder::new(48_000, Channels::Stereo).unwrap();
        let mut decoded = [0; SAMPLES * 4];
        assert_eq!(
            decoder
                .decode(&packet[..size], &mut decoded, false)
                .unwrap(),
            960
        );
        assert!(decoded.iter().any(|sample| sample.unsigned_abs() > 1000));
        let owner = bridge.decode(&packet[..size]).unwrap().unwrap();
        assert_eq!(owner.len(), 960);
        assert!(
            owner
                .chunks_exact(2)
                .any(|s| i16::from_le_bytes([s[0], s[1]]).unsigned_abs() > 1000)
        );
    }
}
