//! Reset inherited TCP connections through the same SOCK_DIAG interface as ss -K.
//! Call only while the fork lifecycle and host network barriers remain closed.
use std::{io, os::fd::AsRawFd};

use netlink_sys::{Socket, SocketAddr};

const TCP_DIAG: u16 = 18;
const DIAG: u16 = 20;
const DESTROY: u16 = 21;
// `ss -K -t state connected`: all TCP states except CLOSE (7) and LISTEN (10).
const CONNECTED: u32 = ((1 << 14) - 1) & !((1 << 7) | (1 << 10));

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn request(
    socket: &Socket,
    seq: u32,
    kind: u16,
    family: u8,
    id: Option<&[u8]>,
    states: u32,
) -> io::Result<Vec<(u8, Vec<u8>)>> {
    // The kernel's TCPDIAG_GETSOCK compatibility dump explicitly uses
    // AF_UNSPEC. Walk the shared TCP hash table once for both IP families;
    // use modern SOCK_DESTROY with each returned family and socket cookie.
    let mut req = vec![0u8; if kind == TCP_DIAG { 76 } else { 72 }];
    let length = req.len() as u32;
    req[..4].copy_from_slice(&length.to_ne_bytes());
    req[4..6].copy_from_slice(&kind.to_ne_bytes());
    let flags: u16 = if kind == TCP_DIAG { 0x301 } else { 5 };
    req[6..8].copy_from_slice(&flags.to_ne_bytes());
    req[8..12].copy_from_slice(&seq.to_ne_bytes());
    req[16] = family;
    if let Some(id) = id {
        req[17] = libc::IPPROTO_TCP as u8;
        req[24..72].copy_from_slice(id);
    } else {
        req[68..72].copy_from_slice(&states.to_ne_bytes());
    }
    if socket.send(&req, 0)? != req.len() {
        return Err(invalid("short SOCK_DIAG request"));
    }
    let mut records = Vec::new();
    loop {
        let mut buf = [0u8; 65536];
        let (n, sender) = socket.recv_from(&mut buf, libc::MSG_TRUNC)?;
        if n > buf.len() || sender.port_number() != 0 {
            return Err(invalid("invalid SOCK_DIAG sender or truncated reply"));
        }
        let mut offset = 0;
        while offset < n {
            let b = &buf[offset..n];
            if b.len() < 16 {
                return Err(invalid("short SOCK_DIAG header"));
            }
            let len = u32::from_ne_bytes(b[..4].try_into().unwrap()) as usize;
            let kind = u16::from_ne_bytes(b[4..6].try_into().unwrap());
            let flags = u16::from_ne_bytes(b[6..8].try_into().unwrap());
            if len < 16
                || len > b.len()
                || u32::from_ne_bytes(b[8..12].try_into().unwrap()) != seq
                || flags & 0x10 != 0
            {
                return Err(invalid("invalid or interrupted SOCK_DIAG reply"));
            }
            let payload = &b[16..len];
            match kind {
                2 | 3 => {
                    if payload.len() < 4 {
                        return Err(invalid("short SOCK_DIAG completion"));
                    }
                    let error = i32::from_ne_bytes(payload[..4].try_into().unwrap());
                    if error != 0 {
                        return Err(io::Error::from_raw_os_error(-error));
                    }
                    return Ok(records);
                }
                TCP_DIAG | DIAG => {
                    if payload.len() < 72 || !matches!(payload[0], 2 | 10) {
                        return Err(invalid("short SOCK_DIAG record"));
                    }
                    records.push((payload[0], payload[4..52].to_vec()));
                }
                _ => return Err(invalid("unexpected SOCK_DIAG reply")),
            }
            offset += (len + 3) & !3;
        }
    }
}

fn open_diag() -> io::Result<Socket> {
    let mut socket = Socket::new(libc::NETLINK_SOCK_DIAG as isize)?;
    socket.bind_auto()?;
    socket.connect(&SocketAddr::new(0, 0))?;
    let timeout = libc::timeval {
        tv_sec: 5,
        tv_usec: 0,
    };
    // SAFETY: a live socket fd and correctly sized, initialized timeval pointer.
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout as *const _ as *const libc::c_void,
            std::mem::size_of_val(&timeout) as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(socket)
}

pub fn empty_all_states() -> io::Result<bool> {
    let socket = open_diag()?;
    Ok(request(&socket, 1, TCP_DIAG, libc::AF_UNSPEC as u8, None, u32::MAX)?.is_empty())
}

pub fn reset() -> io::Result<()> {
    let socket = open_diag()?;
    let mut seq = 1;
    let sockets = request(&socket, seq, TCP_DIAG, libc::AF_UNSPEC as u8, None, CONNECTED)?;
    for (family, id) in sockets {
        seq += 1;
        match request(&socket, seq, DESTROY, family, Some(&id), CONNECTED) {
            Ok(_) => (),
            // Same tolerances as iproute2: disappeared or unkillable socket.
            Err(e) if matches!(e.raw_os_error(), Some(libc::ENOENT | libc::EOPNOTSUPP)) => (),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        time::Duration,
    };

    use super::*;
    #[test]
    #[ignore = "destroys TCP connections; run only under unshare --net"]
    fn reset_connections_preserves_listeners() {
        assert_ne!(
            std::fs::read_link("/proc/self/ns/net").unwrap(),
            std::fs::read_link("/proc/1/ns/net").unwrap(),
            "this destructive test requires a private network namespace"
        );
        std::process::Command::new("ip")
            .args(["link", "set", "lo", "up"])
            .status()
            .unwrap();
        assert!(empty_all_states().unwrap());
        let listeners: Vec<_> = ["127.0.0.1:0", "[::1]:0"]
            .into_iter()
            .map(|a| TcpListener::bind(a).unwrap())
            .collect();
        assert!(!empty_all_states().unwrap(), "listeners invalidate the certificate");
        let mut pairs: Vec<_> = listeners
            .iter()
            .map(|l| {
                let client = TcpStream::connect(l.local_addr().unwrap()).unwrap();
                let (server, _) = l.accept().unwrap();
                (client, server)
            })
            .collect();
        reset().unwrap();
        for (a, b) in &mut pairs {
            for s in [a, b] {
                s.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
                match s.read(&mut [0]) {
                    Ok(0) => (),
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        ()
                    }
                    result => panic!("connection survived reset: {:?}", result),
                }
            }
        }
        for l in listeners {
            let _client = TcpStream::connect(l.local_addr().unwrap()).unwrap();
            let _server = l.accept().unwrap();
        }
        reset().unwrap();
        let before = std::fs::read_dir("/proc/self/fd").unwrap().count();
        for _ in 0..10 {
            reset().unwrap();
        }
        assert_eq!(std::fs::read_dir("/proc/self/fd").unwrap().count(), before);
    }
}
