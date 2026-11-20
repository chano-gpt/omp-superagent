use aes_gcm::{
    Aes256Gcm,
    aead::{Aead, KeyInit, Payload},
};
use anyhow::{Context as _, Result, bail, ensure};
use chacha20poly1305::XChaCha20Poly1305;
use std::net::{IpAddr, SocketAddr};
use tokio::net::UdpSocket;
use uuid::Uuid;

const AES: &str = "aead_aes256_gcm_rtpsize";
const XCHACHA: &str = "aead_xchacha20_poly1305_rtpsize";
const RTP_HEADER: usize = 12;
const OPUS: u8 = 120;
const NONCE: usize = 4;
const TAG: usize = 16;
const DISCOVERY: usize = 74;

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Aes,
    XChaCha,
}

impl Mode {
    pub(super) fn choose(modes: &[String]) -> Result<Self> {
        if modes.iter().any(|mode| mode == AES) {
            return Ok(Self::Aes);
        }
        if modes.iter().any(|mode| mode == XCHACHA) {
            return Ok(Self::XChaCha);
        }
        bail!("Discord voice did not offer a supported transport cipher")
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Aes => AES,
            Self::XChaCha => XCHACHA,
        }
    }
}

enum Cipher {
    Aes(Box<Aes256Gcm>),
    XChaCha(XChaCha20Poly1305),
}

pub(super) struct Rtp {
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    nonce: u32,
    cipher: Cipher,
}

impl Rtp {
    pub(super) fn new(ssrc: u32, mode: Mode, key: [u8; 32]) -> Self {
        let random = Uuid::new_v4();
        let &[a, b, c, d, e, f, ..] = random.as_bytes();
        let cipher = match mode {
            Mode::Aes => Cipher::Aes(Box::new(Aes256Gcm::new(&key.into()))),
            Mode::XChaCha => Cipher::XChaCha(XChaCha20Poly1305::new(&key.into())),
        };
        Self {
            ssrc,
            sequence: u16::from_be_bytes([a, b]),
            timestamp: u32::from_be_bytes([c, d, e, f]),
            nonce: 0,
            cipher,
        }
    }

    pub(super) fn seal(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        self.nonce = self
            .nonce
            .checked_add(1)
            .context("Discord voice transport nonce exhausted")?;
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(960);
        let mut header = [0; RTP_HEADER];
        header[0] = 0x80;
        header[1] = OPUS;
        header[2..4].copy_from_slice(&self.sequence.to_be_bytes());
        header[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        header[8..12].copy_from_slice(&self.ssrc.to_be_bytes());
        let short = self.nonce.to_be_bytes();
        let payload = Payload {
            msg: data,
            aad: &header,
        };
        let ciphertext = match &self.cipher {
            Cipher::Aes(cipher) => cipher.encrypt(&nonce::<12>(short).into(), payload),
            Cipher::XChaCha(cipher) => cipher.encrypt(&nonce::<24>(short).into(), payload),
        }
        .map_err(|_| anyhow::anyhow!("Discord voice RTP encryption failed"))?;
        let mut packet = Vec::with_capacity(header.len() + ciphertext.len() + NONCE);
        packet.extend_from_slice(&header);
        packet.extend_from_slice(&ciphertext);
        packet.extend_from_slice(&short);
        Ok(packet)
    }

    pub(super) fn open(&self, packet: &[u8], owner_ssrc: u32) -> Option<Vec<u8>> {
        let [flags, payload_type, ..] = *packet else {
            return None;
        };
        if flags >> 6 != 2
            || payload_type & 0x7f != OPUS
            || packet.get(8..12)? != owner_ssrc.to_be_bytes()
        {
            return None;
        }
        let base = RTP_HEADER + 4 * usize::from(flags & 0x0f);
        let (header, extension) = if flags & 0x10 == 0 {
            (base, 0)
        } else {
            let words = u16::from_be_bytes(packet.get(base + 2..base + 4)?.try_into().ok()?);
            (base + 4, 4 * usize::from(words))
        };
        let tail = packet.len().checked_sub(NONCE)?;
        if tail < header + TAG {
            return None;
        }
        let short: [u8; NONCE] = packet[tail..].try_into().ok()?;
        let payload = Payload {
            msg: &packet[header..tail],
            aad: &packet[..header],
        };
        let mut data = match &self.cipher {
            Cipher::Aes(cipher) => cipher.decrypt(&nonce::<12>(short).into(), payload),
            Cipher::XChaCha(cipher) => cipher.decrypt(&nonce::<24>(short).into(), payload),
        }
        .ok()?;
        if extension >= data.len() {
            return None;
        }
        let end = if flags & 0x20 == 0 {
            data.len()
        } else {
            let padding = usize::from(*data.last()?);
            if padding == 0 {
                return None;
            }
            data.len().checked_sub(padding)?
        };
        if end <= extension {
            return None;
        }
        data.truncate(end);
        if extension != 0 {
            data.drain(..extension);
        }
        Some(data)
    }
}

fn nonce<const N: usize>(short: [u8; NONCE]) -> [u8; N] {
    const { assert!(N >= NONCE) };
    let mut nonce = [0; N];
    nonce[..NONCE].copy_from_slice(&short);
    nonce
}

pub(super) async fn discover(remote: SocketAddr, ssrc: u32) -> Result<(UdpSocket, SocketAddr)> {
    let bind = match remote.ip() {
        IpAddr::V4(_) => "0.0.0.0:0",
        IpAddr::V6(_) => "[::]:0",
    };
    let socket = UdpSocket::bind(bind)
        .await
        .context("Could not allocate Discord voice UDP")?;
    socket
        .connect(remote)
        .await
        .context("Discord voice UDP connection failed")?;
    let mut request = [0; DISCOVERY];
    request[..2].copy_from_slice(&1u16.to_be_bytes());
    request[2..4].copy_from_slice(&70u16.to_be_bytes());
    request[4..8].copy_from_slice(&ssrc.to_be_bytes());
    socket
        .send(&request)
        .await
        .context("Discord voice UDP discovery send failed")?;
    let mut response = [0; DISCOVERY + 1];
    loop {
        let size = socket
            .recv(&mut response)
            .await
            .context("Discord voice UDP discovery failed")?;
        if size != DISCOVERY
            || response[..4] != [0, 2, 0, 70]
            || response[4..8] != ssrc.to_be_bytes()
        {
            continue;
        }
        let addr = &response[8..72];
        let end = addr
            .iter()
            .position(|&b| b == 0)
            .context("Invalid Discord UDP discovery address")?;
        let ip: IpAddr = std::str::from_utf8(&addr[..end])?.parse()?;
        let port = u16::from_be_bytes([response[72], response[73]]);
        ensure!(port != 0, "Invalid Discord UDP discovery port");
        return Ok((socket, SocketAddr::new(ip, port)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticates_csrc_extension_and_payload_and_refuses_other_senders() {
        let header = [
            0xB1, 0xf8, 0, 1, 0, 0, 0, 1, 0, 0, 0, 42, 0, 0, 0, 91, 0xbe, 0xde, 0, 1,
        ];
        let short = [0, 1, 2, 3];
        let data = b"\x00\x00\x00\x00voice\x00\x02";
        let key = [0x5a; 32];
        for mode in [Mode::XChaCha, Mode::Aes] {
            let payload = Payload {
                msg: data,
                aad: &header,
            };
            let cipher = match mode {
                Mode::Aes => {
                    Aes256Gcm::new(&key.into()).encrypt(&nonce::<12>(short).into(), payload)
                }
                Mode::XChaCha => {
                    XChaCha20Poly1305::new(&key.into()).encrypt(&nonce::<24>(short).into(), payload)
                }
            }
            .unwrap();
            let mut packet = [&header[..], &cipher, &short].concat();
            let transport = Rtp::new(1, mode, key);
            assert_eq!(transport.open(&packet, 42).as_deref(), Some(&b"voice"[..]));
            assert!(transport.open(&packet, 91).is_none());
            packet[13] ^= 1;
            assert!(transport.open(&packet, 42).is_none());
        }
    }
}
