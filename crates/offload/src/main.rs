//! Nomad-side compute offload for streams composed in Atlas OBS.
//!
//! Atlas OBS sends one SRT contribution feed. This supervisor runs ffmpeg to
//! turn it into a landscape program and a 9:16 portrait program (hardware
//! encoded) for the two local InstantClone relays, and serves the OBS dock
//! that shows the whole chain and edits the portrait framing live.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{ChildStdin, Command, Stdio};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DOCK_HTML: &str = include_str!("dock.html");
/// Portrait program geometry. Split mode stacks a 16:9 camera band above
/// the gameplay band.
const PORTRAIT: (u32, u32) = (1080, 1920);
const CAM_BAND: u32 = 608;

/// A region of the source frame, as fractions of its width and height.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
struct Rect {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

impl Rect {
    fn valid(&self) -> bool {
        let unit = |v: f64| v.is_finite() && (0.0..=1.0).contains(&v);
        [self.x, self.y, self.w, self.h].into_iter().all(unit)
            && self.w >= 0.05
            && self.h >= 0.05
            && self.x + self.w <= 1.0 + 1e-6
            && self.y + self.h <= 1.0 + 1e-6
    }

    /// Ordered `(option, expression)` pairs for ffmpeg's crop filter.
    fn crop(&self) -> [(&'static str, String); 4] {
        [
            ("w", format!("iw*{:.6}", self.w)),
            ("h", format!("ih*{:.6}", self.h)),
            ("x", format!("iw*{:.6}", self.x)),
            ("y", format!("ih*{:.6}", self.y)),
        ]
    }

    fn filter(&self, name: &str) -> String {
        let opts: Vec<String> = self
            .crop()
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        format!("crop@{name}={}", opts.join(":"))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Full,
    Split,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Layout {
    mode: Mode,
    game: Rect,
    cam: Rect,
}

impl Default for Layout {
    /// Centre 9:16 column of a 16:9 source; camera in the top-right corner.
    fn default() -> Self {
        let w = (9.0 / 16.0) / (16.0 / 9.0);
        Layout {
            mode: Mode::Full,
            game: Rect {
                x: (1.0 - w) / 2.0,
                y: 0.0,
                w,
                h: 1.0,
            },
            cam: Rect {
                x: 0.72,
                y: 0.03,
                w: 0.25,
                h: 0.25,
            },
        }
    }
}

// Keep only the latest desired layout. Return one command at a time so a
// slow or stalled FFmpeg never builds up a queue of obsolete positions.
fn next_crop_command(applied: &mut Layout, desired: &Layout) -> Option<String> {
    for (name, old, new) in [
        ("game", &mut applied.game, desired.game),
        ("cam", &mut applied.cam, desired.cam),
    ] {
        if name == "cam" && desired.mode != Mode::Split {
            break;
        }
        for ((old, new), (key, expression)) in [
            (&mut old.w, new.w),
            (&mut old.h, new.h),
            (&mut old.x, new.x),
            (&mut old.y, new.y),
        ]
        .into_iter()
        .zip(new.crop())
        {
            if *old != new {
                *old = new;
                return Some(format!("ccrop@{name} -1 {key} {expression}\n"));
            }
        }
    }
    None
}

struct Program {
    url: String,
    relay: Option<String>,
    kbps: u32,
}

struct Config {
    listen: String,
    srt_port: u16,
    ffmpeg: String,
    hwaccel: Option<String>,
    encoder: String,
    video_opts: Vec<String>,
    landscape: Option<Program>,
    portrait: Option<Program>,
    state_dir: PathBuf,
    runtime_dir: PathBuf,
}

impl Config {
    fn from_args() -> Result<Config, String> {
        let mut args: HashMap<String, String> = HashMap::new();
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            let key = flag
                .strip_prefix("--")
                .ok_or(format!("unexpected argument {flag}"))?;
            let value = it.next().ok_or(format!("{flag} needs a value"))?;
            args.insert(key.to_string(), value);
        }
        let mut take = |k: &str| args.remove(k);
        let xdg = |var: &str, fallback: &str| {
            std::env::var_os(var)
                .map(PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(fallback)))
                .unwrap_or_else(|| PathBuf::from("."))
                .join("stream-offload")
        };
        let kbps = |v: Option<String>| -> Result<u32, String> {
            v.map_or(Ok(6000), |v| {
                v.parse().map_err(|_| format!("bad bitrate {v}"))
            })
        };
        let landscape = match take("landscape-url") {
            Some(url) => Some(Program {
                url,
                relay: take("landscape-relay"),
                kbps: kbps(take("landscape-kbps"))?,
            }),
            None => None,
        };
        let portrait = match take("portrait-url") {
            Some(url) => Some(Program {
                url,
                relay: take("portrait-relay"),
                kbps: kbps(take("portrait-kbps"))?,
            }),
            None => None,
        };
        let cfg = Config {
            listen: take("listen").unwrap_or_else(|| "127.0.0.1:8890".into()),
            srt_port: take("srt-port")
                .map_or(Ok(8888), |v| v.parse().map_err(|_| format!("bad port {v}")))?,
            ffmpeg: take("ffmpeg").unwrap_or_else(|| "ffmpeg".into()),
            hwaccel: take("hwaccel"),
            encoder: take("encoder").unwrap_or_else(|| "h264_nvenc".into()),
            video_opts: take("video-opts")
                .unwrap_or_else(|| "-preset p5 -rc cbr".into())
                .split_whitespace()
                .map(String::from)
                .collect(),
            landscape,
            portrait,
            state_dir: take("state-dir")
                .map(PathBuf::from)
                .unwrap_or_else(|| xdg("XDG_STATE_HOME", ".local/state")),
            runtime_dir: take("runtime-dir")
                .map(PathBuf::from)
                .unwrap_or_else(|| xdg("XDG_RUNTIME_DIR", ".cache")),
        };
        if let Some(k) = args.keys().next() {
            return Err(format!("unknown flag --{k}"));
        }
        if cfg.landscape.is_none() && cfg.portrait.is_none() {
            return Err("configure --landscape-url and/or --portrait-url".into());
        }
        Ok(cfg)
    }
}

fn filter_graph(layout: &Layout, landscape: bool, portrait: bool) -> String {
    let mut taps = vec!["[src_t]"];
    if landscape {
        taps.push("[src_h]");
    }
    if portrait {
        taps.push("[src_p]");
    }
    let mut graph = vec![format!("[0:v]split={}{}", taps.len(), taps.concat())];
    graph.push("[src_t]fps=1,scale=640:-2[thumb_src]".into());
    if landscape {
        graph.push(
            "[src_h]scale=1920:1080:force_original_aspect_ratio=decrease,\
             pad=1920:1080:-1:-1,setsar=1,format=yuv420p[landscape]"
                .into(),
        );
    }
    if portrait {
        let (w, h) = PORTRAIT;
        let stacked = match layout.mode {
            Mode::Full => format!(
                "[src_p]{},scale={w}:{h},setsar=1",
                layout.game.filter("game")
            ),
            Mode::Split => format!(
                "[src_p]split=2[pc][pg];[pc]{},scale={w}:{CAM_BAND},setsar=1[cam];\
                 [pg]{},scale={w}:{},setsar=1[game];[cam][game]vstack=2",
                layout.cam.filter("cam"),
                layout.game.filter("game"),
                h - CAM_BAND
            ),
        };
        graph.push(format!("{stacked},format=yuv420p,split=2[portrait][pt]"));
        graph.push("[pt]fps=1,scale=270:480[thumb_portrait]".into());
    }
    graph.join(";")
}

fn ffmpeg_args(cfg: &Config, layout: &Layout) -> Vec<String> {
    // `-y`: previews survive restarts; without it ffmpeg prompts on stdin.
    let mut a: Vec<String> = [
        "-hide_banner",
        "-y",
        "-loglevel",
        "warning",
        "-nostats",
        "-progress",
        "pipe:1",
        "-stats_period",
        "0.1",
    ]
    .map(String::from)
    .to_vec();
    if let Some(hw) = &cfg.hwaccel {
        a.extend(["-hwaccel".into(), hw.clone()]);
    }
    a.extend([
        "-i".into(),
        format!("srt://0.0.0.0:{}?mode=listener", cfg.srt_port),
    ]);
    a.extend([
        "-filter_complex".into(),
        filter_graph(layout, cfg.landscape.is_some(), cfg.portrait.is_some()),
    ]);
    for (label, program) in [("landscape", &cfg.landscape), ("portrait", &cfg.portrait)] {
        let Some(p) = program else { continue };
        a.extend([
            "-map".into(),
            format!("[{label}]"),
            "-map".into(),
            "0:a?".into(),
        ]);
        a.extend(["-c:v".into(), cfg.encoder.clone()]);
        a.extend(cfg.video_opts.iter().cloned());
        let k = p.kbps;
        a.extend(
            [
                "-b:v",
                &format!("{k}k"),
                "-maxrate",
                &format!("{k}k"),
                "-bufsize",
                &format!("{}k", 2 * k),
                // Two-second keyframes: Twitch, YouTube and Kick all require it.
                "-force_key_frames",
                "expr:gte(t,n_forced*2)",
                "-c:a",
                "aac",
                "-b:a",
                "160k",
                "-ar",
                "48000",
                // A wedged relay must fail the pipeline, not hang it.
                "-rw_timeout",
                "5000000",
                "-f",
                "flv",
                &p.url,
            ]
            .map(String::from),
        );
    }
    for name in ["src", "portrait"] {
        if name == "portrait" && cfg.portrait.is_none() {
            continue;
        }
        let file = cfg.runtime_dir.join(format!("{name}.jpg"));
        a.extend(
            [
                "-map",
                &format!("[thumb_{name}]"),
                "-c:v",
                "mjpeg",
                "-q:v",
                "6",
                "-f",
                "image2",
                "-update",
                "1",
                "-atomic_writing",
                "1",
                &file.to_string_lossy(),
            ]
            .map(String::from),
        );
    }
    a
}

#[derive(Default)]
struct Shared {
    layout: Layout,
    state: &'static str,
    state_since: Option<Instant>,
    progress: HashMap<String, String>,
    out_time_us: i64,
    last_advance: Option<Instant>,
    restarts: u64,
    last_exit: Option<String>,
    log: VecDeque<String>,
    stdin: Option<ChildStdin>,
    restart: bool,
}

impl Shared {
    fn set_state(&mut self, state: &'static str) {
        if self.state != state {
            self.state = state;
            self.state_since = Some(Instant::now());
        }
    }

    fn transcoding(&self) -> bool {
        matches!(self.state, "live" | "stalled")
    }
}

type State = Arc<Mutex<Shared>>;

fn lock(state: &State) -> std::sync::MutexGuard<'_, Shared> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

fn supervise(cfg: Arc<Config>, state: State) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let layout = lock(&state).layout.clone();
        let spawned = Command::new(&cfg.ffmpeg)
            .args(ffmpeg_args(&cfg, &layout))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                let mut s = lock(&state);
                s.last_exit = Some(format!("cannot start ffmpeg: {e}"));
                s.set_state("restarting");
                drop(s);
                std::thread::sleep(Duration::from_secs(10));
                continue;
            }
        };
        let started = Instant::now();
        {
            let mut s = lock(&state);
            s.stdin = child.stdin.take();
            s.progress.clear();
            s.out_time_us = 0;
            s.last_advance = None;
            s.restart = s.layout != layout;
            s.set_state("waiting");
        }
        let stdout = child.stdout.take().expect("piped stdout");
        let st = state.clone();
        std::thread::spawn(move || {
            let mut block = HashMap::new();
            let mut prev: Option<(Instant, f64, f64)> = None;
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let Some((k, v)) = line.split_once('=') else {
                    continue;
                };
                block.insert(k.trim().to_string(), v.trim().to_string());
                if k != "progress" {
                    continue;
                }
                let t = block
                    .get("out_time_us")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                // ffmpeg's fps/speed are averages since start; report the
                // current rate so a live slowdown shows.
                let frame = block
                    .get("frame")
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let now = Instant::now();
                if let Some((then, f0, t0)) = prev {
                    let dt = now.duration_since(then).as_secs_f64();
                    block.insert("fps".into(), format!("{:.2}", (frame - f0) / dt));
                    block.insert("speed".into(), format!("{:.3}", (t as f64 - t0) / 1e6 / dt));
                }
                prev = Some((now, frame, t as f64));
                let mut s = lock(&st);
                if t > s.out_time_us {
                    s.out_time_us = t;
                    s.last_advance = Some(Instant::now());
                    s.set_state("live");
                }
                s.progress = std::mem::take(&mut block);
            }
        });
        let stderr = child.stderr.take().expect("piped stderr");
        let st = state.clone();
        let (crop_ack, crop_replies) = sync_channel(1);
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if line.starts_with("Command reply ") && line.contains(" ret:0 ") {
                    let _ = crop_ack.try_send(());
                }
                // Echoes of the framing command channel; not news.
                if line.trim().is_empty()
                    || line.starts_with("Command reply")
                    || line.starts_with("Enter command:")
                {
                    continue;
                }
                let mut s = lock(&st);
                // Wide enough that a cascade of follow-on errors keeps the root cause.
                if s.log.len() == 40 {
                    s.log.pop_front();
                }
                s.log.push_back(line);
            }
        });

        let mut quit_at: Option<Instant> = None;
        let mut requested = false;
        let mut applied = layout;
        let mut pending: Option<Instant> = None;
        let status = loop {
            std::thread::sleep(Duration::from_millis(50));
            if let Ok(Some(status)) = child.try_wait() {
                break status;
            }
            let mut s = lock(&state);
            if pending.is_some() && crop_replies.try_recv().is_ok() {
                pending = None;
            }
            if pending.is_some_and(|t| t.elapsed() > Duration::from_secs(2)) {
                s.restart = true;
            }
            if cfg.portrait.is_some() && s.state == "live" && !s.restart && pending.is_none() {
                if let Some(command) = next_crop_command(&mut applied, &s.layout) {
                    match s.stdin.as_mut().map(|i| i.write_all(command.as_bytes())) {
                        Some(Ok(())) => pending = Some(Instant::now()),
                        _ => s.restart = true,
                    }
                }
            }
            if s.restart && quit_at.is_none() {
                requested = true;
                quit_at = Some(Instant::now());
                if let Some(stdin) = s.stdin.as_mut() {
                    let _ = stdin.write_all(b"q");
                }
            }
            if s.state == "live"
                && s.last_advance
                    .is_some_and(|t| t.elapsed() > Duration::from_secs(3))
            {
                s.set_state("stalled");
            }
            drop(s);
            // `q` is only read while transcoding; a listener blocked in
            // accept() needs the kill.
            if quit_at.is_some_and(|t| t.elapsed() > Duration::from_secs(2)) {
                let _ = child.kill();
            }
        };

        let mut s = lock(&state);
        s.stdin = None;
        s.restarts += 1;
        if !requested {
            s.last_exit = Some(format!("ffmpeg exited ({status})"));
        }
        s.set_state("restarting");
        drop(s);
        backoff = if requested || started.elapsed() > Duration::from_secs(30) {
            Duration::from_millis(300)
        } else {
            (backoff * 2).min(Duration::from_secs(10))
        };
        std::thread::sleep(backoff);
    }
}

/// Persist, then apply live through ffmpeg's command interface when the
/// filter graph keeps its shape; otherwise restart the pipeline.
fn apply_layout(cfg: &Config, state: &State, layout: Layout) -> Result<&'static str, String> {
    if !layout.game.valid() || !layout.cam.valid() {
        return Err("regions must lie inside the frame and be at least 5% in size".into());
    }
    save_layout(cfg, &layout).map_err(|e| format!("cannot save layout: {e}"))?;
    let mut s = lock(state);
    let reshaped = s.layout.mode != layout.mode;
    s.layout = layout;
    if cfg.portrait.is_none() {
        return Ok("saved");
    }
    if reshaped || !s.transcoding() {
        s.restart = true;
        return Ok(if s.transcoding() {
            "restarting"
        } else {
            "saved"
        });
    }
    Ok(if s.restart { "restarting" } else { "queued" })
}

fn layout_path(cfg: &Config) -> PathBuf {
    cfg.state_dir.join("layout.json")
}

fn save_layout(cfg: &Config, layout: &Layout) -> std::io::Result<()> {
    std::fs::create_dir_all(&cfg.state_dir)?;
    let tmp = cfg.state_dir.join("layout.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(layout)?)?;
    std::fs::rename(tmp, layout_path(cfg))
}

fn load_layout(cfg: &Config) -> Layout {
    std::fs::read(layout_path(cfg))
        .ok()
        .and_then(|b| serde_json::from_slice::<Layout>(&b).ok())
        .filter(|l| l.game.valid() && l.cam.valid())
        .unwrap_or_default()
}

/// Read the relay's public, secret-free overlay feed and keep only the
/// fields the dock draws.
fn relay_state(addr: &str) -> Value {
    let fetch = || -> Option<Value> {
        let sock: SocketAddr = addr.parse().ok()?;
        let mut tcp = TcpStream::connect_timeout(&sock, Duration::from_millis(300)).ok()?;
        tcp.set_read_timeout(Some(Duration::from_millis(700)))
            .ok()?;
        tcp.write_all(format!("GET /overlay-state HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
            .ok()?;
        let mut raw = Vec::new();
        tcp.take(65536).read_to_end(&mut raw).ok()?;
        let text = String::from_utf8(raw).ok()?;
        let (head, body) = text.split_once("\r\n\r\n")?;
        if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
            return None;
        }
        let v: Value = serde_json::from_str(body).ok()?;
        Some(json!({
            "phase": v["phase"],
            "current_delay_ms": v["current_delay_ms"],
            "ingest_alive": v["ingest_alive"],
            "destinations_alive": v["destinations_alive"],
            "destinations_total": v["destinations_total"],
            "bitrate_kbps": v["stats"]["bitrate_kbps"],
        }))
    };
    fetch().unwrap_or(Value::Null)
}

fn status(cfg: &Config, state: &State) -> Value {
    let s = lock(state);
    let p = |k: &str| s.progress.get(k).cloned();
    let num = |k: &str| p(k).and_then(|v| v.trim_end_matches('x').parse::<f64>().ok());
    let pipeline = json!({
        "state": s.state,
        "for_s": s.state_since.map(|t| t.elapsed().as_secs()),
        "fps": num("fps"),
        "speed": num("speed"),
        "drop": num("drop_frames"),
        "dup": num("dup_frames"),
        "restarts": s.restarts,
        "last_exit": s.last_exit,
        "log": s.log,
    });
    let layout = s.layout.clone();
    drop(s);
    let program = |p: &Option<Program>| match p {
        None => Value::Null,
        Some(p) => json!({
            "kbps": p.kbps,
            "relay": p.relay.as_deref().map(relay_state).unwrap_or(Value::Null),
        }),
    };
    json!({
        "pipeline": pipeline,
        "srt_port": cfg.srt_port,
        "encoder": cfg.encoder,
        "landscape": program(&cfg.landscape),
        "portrait": program(&cfg.portrait),
        "layout": layout,
        "portrait_size": [PORTRAIT.0, PORTRAIT.1],
        "cam_band": CAM_BAND,
    })
}

/// Reject DNS-rebinding: the dock is only ever addressed by IP literal.
fn host_ok(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(""),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    name.parse::<IpAddr>().is_ok()
}

type Reply = tiny_http::Response<Cursor<Vec<u8>>>;

fn reply(code: u16, ctype: &str, body: impl Into<Vec<u8>>) -> Reply {
    let header = |k: &str, v: &str| tiny_http::Header::from_bytes(k, v).expect("static header");
    tiny_http::Response::from_data(body.into())
        .with_status_code(code)
        .with_header(header("Content-Type", ctype))
        .with_header(header("Cache-Control", "no-store"))
        .with_header(header("X-Content-Type-Options", "nosniff"))
}

fn handle(cfg: &Config, state: &State, req: &mut tiny_http::Request) -> Reply {
    let header = |name: &'static str| {
        req.headers()
            .iter()
            .find(|h| h.field.equiv(name))
            .map(|h| h.value.as_str().to_string())
    };
    if !header("Host").is_some_and(|h| host_ok(&h)) {
        return reply(421, "text/plain", "address the dock by IP");
    }
    let json_body = header("Content-Type").is_some_and(|c| c.starts_with("application/json"));
    let path = req.url().split('?').next().unwrap_or("").to_string();
    match (req.method(), path.as_str()) {
        (tiny_http::Method::Get, "/") => reply(200, "text/html; charset=utf-8", DOCK_HTML),
        (tiny_http::Method::Get, "/api/status") => {
            reply(200, "application/json", status(cfg, state).to_string())
        }
        (tiny_http::Method::Get, p @ ("/preview/src.jpg" | "/preview/portrait.jpg")) => {
            let file = cfg.runtime_dir.join(p.trim_start_matches("/preview/"));
            match std::fs::read(file) {
                Ok(bytes) => reply(200, "image/jpeg", bytes),
                Err(_) => reply(404, "text/plain", "no preview yet"),
            }
        }
        // A JSON content type forces a CORS preflight, so other origins in
        // the OBS browser cannot post here.
        (tiny_http::Method::Post, "/api/layout") if json_body => {
            let mut body = Vec::new();
            if req.as_reader().take(4097).read_to_end(&mut body).is_err() || body.len() > 4096 {
                return reply(413, "text/plain", "layout too large");
            }
            match serde_json::from_slice::<Layout>(&body)
                .map_err(|e| e.to_string())
                .and_then(|l| apply_layout(cfg, state, l))
            {
                Ok(applied) => reply(
                    200,
                    "application/json",
                    json!({ "applied": applied }).to_string(),
                ),
                Err(e) => reply(400, "application/json", json!({ "error": e }).to_string()),
            }
        }
        _ => reply(404, "text/plain", "not found"),
    }
}

fn main() {
    let cfg = match Config::from_args() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("stream-offload: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = std::fs::create_dir_all(&cfg.runtime_dir) {
        eprintln!(
            "stream-offload: cannot create {}: {e}",
            cfg.runtime_dir.display()
        );
        std::process::exit(1);
    }
    let state: State = Arc::new(Mutex::new(Shared {
        layout: load_layout(&cfg),
        state: "starting",
        ..Default::default()
    }));
    let server = match tiny_http::Server::http(&cfg.listen) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("stream-offload: cannot listen on {}: {e}", cfg.listen);
            std::process::exit(1);
        }
    };
    {
        let (cfg, state) = (cfg.clone(), state.clone());
        std::thread::spawn(move || supervise(cfg, state));
    }
    eprintln!(
        "stream-offload: dock on http://{}/, SRT listener on :{}",
        cfg.listen, cfg.srt_port
    );
    // ponytail: one request at a time; the dock polls once a second.
    for mut req in server.incoming_requests() {
        let response = handle(&cfg, &state, &mut req);
        let _ = req.respond(response);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crop_commands_use_only_the_latest_changed_fields() {
        let mut applied = Layout::default();
        let mut desired = applied.clone();
        desired.game.x = 0.2;
        desired.game.y = 0.1;
        assert_eq!(
            next_crop_command(&mut applied, &desired).as_deref(),
            Some("ccrop@game -1 x iw*0.200000\n")
        );
        desired.game.y = 0.3;
        desired.cam.x = 0.1; // Not in the Full graph.
        assert_eq!(
            next_crop_command(&mut applied, &desired).as_deref(),
            Some("ccrop@game -1 y ih*0.300000\n")
        );
        assert_eq!(next_crop_command(&mut applied, &desired), None);
    }

    #[test]
    fn graphs_name_every_output_and_crop() {
        let mut l = Layout::default();
        let full = filter_graph(&l, true, true);
        for label in [
            "[landscape]",
            "[portrait]",
            "[thumb_src]",
            "[thumb_portrait]",
            "crop@game=",
        ] {
            assert!(full.contains(label), "{label} missing in {full}");
        }
        assert!(full.starts_with("[0:v]split=3"));
        assert!(!full.contains("crop@cam"));
        l.mode = Mode::Split;
        let split = filter_graph(&l, false, true);
        assert!(split.starts_with("[0:v]split=2"));
        assert!(
            split.contains("crop@cam=")
                && split.contains("scale=1080:1312")
                && split.contains("vstack=2")
        );
        assert!(!split.contains("[landscape]"));
    }

    #[test]
    fn rejects_regions_outside_the_frame() {
        assert!(Layout::default().game.valid());
        assert!(!Rect {
            x: 0.9,
            y: 0.0,
            w: 0.2,
            h: 0.5
        }
        .valid());
        assert!(!Rect {
            x: 0.0,
            y: 0.0,
            w: 0.01,
            h: 0.5
        }
        .valid());
        assert!(!Rect {
            x: f64::NAN,
            y: 0.0,
            w: 0.5,
            h: 0.5
        }
        .valid());
        assert!(serde_json::from_str::<Layout>(r#"{"mode":"full","game":{"x":0,"y":0,"w":1,"h":1},"cam":{"x":0,"y":0,"w":1,"h":1},"extra":1}"#).is_err());
    }

    #[test]
    fn hosts_must_be_ip_literals() {
        assert!(host_ok("10.10.0.2:8890") && host_ok("127.0.0.1") && host_ok("[::1]:8890"));
        assert!(!host_ok("evil.example:8890") && !host_ok("nomad"));
    }
}
