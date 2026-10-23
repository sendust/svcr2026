---
name: multichannel-av-recorder
description: Reference architecture (grounded in the svcr2026 project) for building a multi-channel DeckLink broadcast recording server — ffmpeg process wrapper + watchdog/auto-restart, DeckLink 1-video+8-audio to MediaMTX path routing, card-based UI with WebRTC preview and dBFS log-scale audio level meters, two MXF transfer strategies (binary copy vs bmxtranswrap), NVENC session-limit enforcement, XDCAM50 (59.94i) MXF encode parameters, server status/log streaming, pipe-buffer sizing, and the asyncio-vs-multithreading concurrency decision for many simultaneous encoders. Load this when designing or extending any ffmpeg-based multi-encoder recording/streaming system, or when asked about any of these topics — ffmpeg watchdog design, DeckLink multi-channel audio routing to MediaMTX, broadcast-style dBFS level meters, MXF growing-file transfer, NVENC session caps, XDCAM50/mpeg2video interlaced encode params, or choosing async vs threads for N concurrent encoders.
---

# Multi-channel AV recorder architecture

Grounded in a real production system (svcr2026: FastAPI + asyncio + ffmpeg, N DeckLink
encoders, each doing simultaneous RTSP/WebRTC preview + local MXF recording + NAS
transfer). Treat the numbered values below (timeouts, buffer sizes, thresholds) as
battle-tested starting points, not arbitrary — each one exists because a specific
failure mode was observed and fixed. Don't casually change them without the same
reasoning in mind.

## 0. Before writing code: ask about the concurrency model

A server managing N encoders, each with its own ffmpeg process, watchdog, and NAS
transfer, can be built two ways:

- **asyncio + per-channel dedicated thread pool (hybrid)** — one event loop drives all
  control flow (state machine, watchdog polling, SSE broadcast, scheduler); every
  blocking filesystem/subprocess call is pushed onto a small `ThreadPoolExecutor`
  *dedicated to that one encoder* (not `asyncio.to_thread`'s shared global pool, and
  not one big shared executor). This is what svcr2026 uses.
- **one OS process (or one full thread) per encoder** — simpler mental model, true
  isolation (a stuck encoder can't touch another's memory/GIL time at all), but more
  resource overhead per channel and harder to do cheap cross-channel coordination
  (shared NVENC session counter, shared SSE broadcast, shared scheduler).

**At the start of a new project using this skill, ask the user which model they want**
before writing the encoder wrapper — don't default silently. If they pick the hybrid
model, the isolation reasoning in §10 is the part that actually matters (it's the
difference between "one NFS hang stalls one channel" and "one NFS hang stalls all N").

## 1. ffmpeg process wrapper + watchdog

One wrapper object per encoder, holding `self.process` (`asyncio.subprocess.Process`)
and an `asyncio.Lock` (`_lifecycle_lock`) serializing `start()`/`stop()`/auto-restart so
concurrent REC/STOP clicks or a watchdog-triggered restart racing a manual STOP can
never both touch `self.process` at once (classic "NoneType has no attribute kill" /
orphaned-process bug otherwise).

**`start()`**: under the lock — stop any existing process first (idempotent), set
state/flags, reset *every* watchdog counter (`last_progress_ts=now`,
`has_received_first_progress=False`, `last_file_size=-1`, `file_stagnant_since_ts=None`,
`last_rollover_ts=now`; backdate `session_start_ts` by ~2s so the first segment file
isn't misclassified as "leftover from a previous session"), re-read any config fields
that are safe to hot-reload, spawn the subprocess, widen its stdout/stderr pipes
(§9), then spawn the pipe-reader task, the watchdog task, and (if recording+transfer
enabled) the transfer-manager task.

**`stop(force=False)`**: set the "stopping" flags and target state to free
*immediately*, before acquiring the lock, and broadcast that to the UI right away —
then acquire the lock to actually tear down the process, and re-confirm the final
state *again* after the lock (a real race exists where a concurrent start could flip
state in between; re-asserting after the lock closes it). Never reset the user's
transfer-preference checkbox on STOP — STOP means "stop now," not "forget my
settings." `force=True` is reserved for actual app shutdown (see §5's SIGTERM gating
note under transfer).

**`_watchdog_loop()`**: polls on a *jittered* interval (e.g. `random.uniform(0.5,1.0)`)
so N channels don't all hit the disk/filesystem in lockstep. Each tick checks, in
order:
1. Did the process exit on its own? → trigger auto-restart.
2. Segment rollover detection (new output file appeared) → refresh a grace-period
   timestamp. Closing and reopening a large segment file (header/index flush) can
   stall capture for a moment — don't let that stall be misread as a hang.
3. Progress-timestamp timeout (ffmpeg's own `-progress` pipe stopped advancing), with
   the timeout multiplied by 3x during an initial startup grace period or shortly
   after a rollover.
4. Output file size stagnation (size unchanged across polls), same grace multiplier.
5. **Destination disk space** (rec_path), throttled to e.g. every 30s (not every tick —
   `disk_usage` is cheap but N channels checking every 0.5s is still wasteful): if free
   space drops below a hard floor, don't auto-restart — call the *stop* path instead,
   exactly as if the user pressed STOP, and persist "don't auto-resume" so a later
   server restart doesn't immediately hit the same full disk. Auto-restarting a
   disk-full channel just fails again immediately; it's not a transient fault.
   If the space-check itself errors (e.g. flaky NFS `statvfs`), log and do nothing —
   don't fail closed on a monitoring glitch.

**Auto-restart** itself: guard against double-triggering, log a loud/taggable reason
string (so it's visually distinct in a log viewer), stop the process under the lock,
sleep a short hold time, re-check that the user didn't press STOP during the wait
(cancel the restart if so), then call `start()` again preserving every session flag
(transfer preference, test-mode flag, filename tag, "started by scheduler X" ownership)
— an auto-restart is a continuation of the same logical session, not a new one.

## 2. DeckLink 1 video + 8 audio → MediaMTX path routing

One DeckLink input carries 1 video + 8 discrete audio channels. The ffmpeg
`filter_complex` needs to serve three different audiences from that one input
simultaneously: a scaled-down preview stream, 4 stereo-paired preview audio feeds (for
browser playback, where only paired/stereo audio makes sense), and all 8 raw mono
channels preserved discretely for the recording.

Pattern (encoder id `n`, pushing to MediaMTX via RTSP):

```
[0:v]split=2[v_split][v_rec];
[v_split]scale=-2:400[v_stream];
[0:a]asplit=12[as1][as2][as3][as4][ar0][ar1][ar2][ar3][ar4][ar5][ar6][ar7];
[as1]pan=stereo|c0=c0|c1=c1[a12];
[as2]pan=stereo|c0=c2|c1=c3[a34];
[as3]pan=stereo|c0=c4|c1=c5[a56];
[as4]pan=stereo|c0=c6|c1=c7[a78];
[ar0]pan=1c|c0=c0[a_ch0]; ... [ar7]pan=1c|c0=c7[a_ch7]
```

`asplit=12` fans the 8-channel audio into 12 independent copies: 4 become stereo pairs
(`pan=stereo|c0=X|c1=Y`) for preview, 8 stay single mono channels for the recording
branch. MediaMTX path naming convention: `input{n}00` = video, `input{n}12` /
`input{n}34` / `input{n}56` / `input{n}78` = the 4 stereo audio pairs (number encodes
which two source channels it carries). Preview audio is encoded cheap (e.g. opus,
32kbps/pair is plenty for a monitoring feed) and video preview is downscaled
(`scale=-2:400`) — never push full-resolution/full-bitrate video to a browser preview.
The recording branch (`v_rec` + all 8 `a_ch*`) maps into a single local file output
with uncompressed PCM per channel (see §7) — it never touches MediaMTX at all.

## 3. Card-based UI: video preview + 8-channel level meters

One card per encoder: header (name / current filename / status badge) → a row with a
plain `<video>` element (`object-fit: contain`) for preview + a fixed-size meter panel
→ the meter panel holds one shared dB scale (§4) plus 4 channel-pairs, each pair being
two vertical bar meters with a text label.

**WebRTC/WHEP preview wiring**: for each encoder, open one WHEP reader against the
video path and one more against *each* of the 4 stereo audio paths (so 5 WHEP
connections per encoder: 1 video + 4 audio). Video track → `video.srcObject`. Each
audio track → feed into the meter pipeline below.

**Per-channel level extraction (Web Audio API)**: `MediaStreamSource` → route through a
*zero-gain* `GainNode` to `destination` (this is a deliberate silent-routing trick —
some browsers' RTP audio decoders throttle/stall if the track is never connected to an
active audio graph node; a muted gain node keeps the decoder actually running without
making sound) → `ChannelSplitterNode(2)` → one `AnalyserNode` per channel. Use an
`fftSize` larger than the default (e.g. 1024, not 256) specifically to damp
RMS jitter caused by momentary waveform phase — a too-small FFT window makes the meter
visibly "dance" even on a steady test tone.

## 4. dBFS log-scale level meter

A meter that reads in real dBFS and *looks* like broadcast gear, not a generic
0–100% linear bar.

- Pick `METER_MIN_DB` / `METER_MAX_DB` (e.g. -60 / 0). Map any dB value to a bar-height
  percentage with a straight linear clamp-and-rescale:
  `percent = (clamp(db, MIN, MAX) - MIN) / (MAX - MIN) * 100`.
  This *is* the log scale — dB is already the log of amplitude, so mapping it linearly
  to pixel height is correct; don't apply a second log() on top of it.
- RMS → dBFS: `dbfs = rms > 0 ? 20*log10(rms) : MIN_DB` (never call `log10(0)`).
  Sanity check: a full-scale sine wave (peak amplitude 1.0) has RMS ≈0.707, which
  should read as ≈ -3.01 dBFS — if your math doesn't reproduce that, it's wrong.
- Smooth with asymmetric attack/release (EMA with two different coefficients: fast
  rise, slow fall) so the meter doesn't look jittery but still responds instantly to
  transients.
- Draw a shared tick scale (e.g. 0, -6, -12, -18, -30, -45, -60 dBFS) once per card,
  not per channel — real mixer bridges share one printed scale across many meters.
- **Color bands at fixed dB thresholds (e.g. green below -18dBFS, yellow -18 to -6,
  red -6 to 0) must be painted on the *fixed-height background element*, never on the
  element whose height animates with signal level.** CSS `linear-gradient` percentages
  are relative to the box the gradient is painted on — if you paint the gradient on the
  animated bar itself, the color boundaries stretch and compress with the signal and
  never sit at a fixed dB position (a real, previously-shipped bug). Instead: paint the
  3-color gradient once on the fixed-height track background, and render the "meter"
  as an opaque dark *mask* layered on top, anchored to the top, whose height is
  `(100 - level)%` — it uncovers the fixed-position colors from the bottom up as level
  rises, rather than painting new colors itself.
- **Peak hold**: track a separate peak value that snaps up instantly to any new higher
  reading, holds for a short period (~1–1.5s), then decays linearly at a fixed
  %/second rate, floored at the current instantaneous level. Render it as a thin line
  positioned against the *same fixed-height parent* as the color bands (not the
  animated mask) so it isn't subject to the bug above.

## 5. Two MXF transfer strategies (recording → NAS)

Support picking between two transfer implementations via one global config flag (not
per-channel — keep it simple, one process-wide choice), so you can switch strategies
without code changes.

**Binary copy (homegrown, append-aware)**: while the recording file is still growing,
poll its size on an interval and copy only the newly-appended tail in bounded chunks
(e.g. 20MB) — never re-copy from the start. Before starting, verify the destination is
actually writable by performing a real write+fsync+delete of a throwaway file (not
just `os.access`, which misses NFS stale-mount/quota/permission-mismatch cases), and
check free space against a threshold. Once growth stops (two equal-size polls *and*
it's no longer the latest file), wait one more grace period to catch any
trailing writes, then do a final size-reconciliation copy, then explicitly re-copy the
file's header bytes and footer bytes (MXF index/duration metadata lives in the footer,
not the header — a partial/never-finalized footer is what makes editors fail to
recognize a "growing" MXF's true length). Abort rather than guess if a final
size-mismatch persists after one retry — don't risk silently shipping a truncated/
corrupt copy.

**External tool wrapping (e.g. bmxtranswrap) for growing-file-aware rewrap**: if a
purpose-built tool already understands "the source file is still growing" (via its own
retry/stagger flags), prefer launching it as a subprocess and let it own the retry
logic entirely — don't duplicate retry/backoff in your own code. Key implementation
points:
- Run the subprocess-babysitting logic on a **real OS thread** (`threading.Thread`),
  not inside the asyncio executor layer — you need a thread object you can
  individually `.join()` during graceful shutdown, which a generic executor task
  doesn't give you as cleanly.
- Stagger launches after recording starts (e.g. fixed delay + random jitter) so many
  channels rolling a segment at once don't all spawn the external tool simultaneously.
- **Gate SIGTERM to the external process behind "is the whole app actually shutting
  down," not "did the user press stop on this channel."** If the tool paces itself in
  real time against a live, still-growing source (e.g. a `--realtime-factor`-style
  flag), it structurally lags the live recording by its own startup stagger. Killing
  it the instant the channel stops truncates that trailing lag off the final output.
  Since the source file has already stopped growing at that point, the right move is
  to leave the external process running — it'll notice growth has stopped (via its own
  retry/delay budget) and exit cleanly on its own, with nothing truncated. Only send a
  real SIGTERM when the *host process* itself is exiting and can't wait around.
- Run progress polling (reading dest-file size off a possibly-NFS-mounted destination,
  purely for a UI progress bar) in a **separate daemon thread**, decoupled from the
  thread that's watching for the stop signal and reaping the subprocess. A `getsize()`
  against a hung NFS mount can block in uninterruptible sleep indefinitely — if that
  call lives in the same loop that's supposed to notice a stop request and terminate
  the subprocess, a hung NFS stat call silently defeats your entire graceful-shutdown
  path. Decoupling means the worst case is one harmlessly-stuck daemon thread, not a
  stuck shutdown.
- Every exit path (normal completion, graceful-terminate, forced-kill, and any
  exception) must end with a *blocking* `wait()` on the subprocess before the function
  returns, including inside exception handlers — this is what actually prevents
  zombie processes, not any particular polling interval.

## 6. Server status + log streaming

- Attach a custom logging handler to the root logger that appends every log record
  into a bounded ring buffer (e.g. `deque(maxlen=2000)`) — this makes "show recent
  server logs in the web UI" work for free, no separate log-shipping needed, and
  captures logs from anywhere in the app without each call site needing to know about
  the UI.
- Push live status over Server-Sent Events, multiplexing several event *types* (e.g.
  per-channel status, server-info/log-tail, schedule list) down one `/events` stream so
  the browser only needs one `EventSource`. Use separate internal tick intervals per
  event type (e.g. channel status every 0.2s, server-info/logs every 1s, a scheduler
  tick every 5s) rather than one shared interval — status needs to feel instant, logs
  don't.
- Only do the (possibly nontrivial) work of serializing/broadcasting an event type if
  at least one client is actually connected — but keep any *side-effecting* periodic
  work (like a scheduler's own tick) running unconditionally regardless of whether
  anyone's watching.
- A separate `/debug` view with its own independent SSE stream is useful for raw
  per-channel ffmpeg stdout/stderr tailing (keep a small per-channel ring buffer of raw
  process output, separate from the structured app-log buffer above) plus live
  disk-space numbers — don't conflate this with the main operator UI's event stream.
- A "restart everything" debug action should SIGTERM *every* OS process matching the
  ffmpeg binary name host-wide (not just ones this instance thinks it's tracking — this
  cleans up orphans from any previous crash), then self-SIGTERM and rely on the process
  supervisor (e.g. systemd `Restart=always`) to bring it back up — don't call the
  supervisor's restart command directly from inside the app; self-terminating and
  trusting the supervisor works regardless of which user/permissions the app runs
  under.

## 7. NVENC session-limit enforcement

GPU hardware encoders (particularly consumer/GeForce-class) cap the number of
*concurrent* encode sessions (often ~3), while datacenter/workstation GPUs may not.
Handle this as a live, per-attempt check, not a static reservation:

- At startup, actually probe NVENC availability by running a trivial real encode (not
  just checking if the encoder name exists in `ffmpeg -encoders`) — use a frame size
  large enough to clear the encoder's minimum-resolution floor (tiny test frames can
  produce false negatives on hardware that works fine at real resolutions).
- Keep a configurable global max-concurrent-sessions number, and allow a per-channel
  override of "prefer NVENC or not" that falls back to the global default when unset
  (not every channel needs to agree).
- When a channel is about to start and wants NVENC, count how many *other currently
  running* channels have NVENC active right now (a live count over actual process
  state, not a counter/semaphore that could drift) — if under the cap, grant it and
  remember "this channel is using NVENC" on the channel's own state; otherwise fall
  back to a software encoder (e.g. libx264) and log that this happened (an operator
  silently landing on CPU encoding should not be invisible). Clear "this channel is
  using NVENC" immediately when the channel's process stops, so its slot is
  immediately available to the next channel that starts.

## 8. XDCAM50-class MXF encode parameters (59.94i)

For a 50Mbps 4:2:2 interlaced MPEG-2 MXF recording (XDCAM HD422-pattern), the encode
flags that matter and why:

```
-flags +ildct+ilme+cgop -field_order tt      # interlaced DCT/motion-estimation + closed GOP; tt = top-field-first (59.94i)
-vcodec mpeg2video -pix_fmt yuv422p          # 4:2:2 chroma, required for this profile
-profile:v 0 -level:v 2                      # MPEG-2 4:2:2 Profile @ Main/High Level — the XDCAM HD422 signature combo
-colorspace bt709 -color_primaries bt709 -color_trc bt709
-sc_threshold 1000000000                     # disable scene-cut-triggered GOP breaks — keep GOP structure fixed/predictable
-g 15 -bf 2 -trellis 0 -mpv_flags strict_gop  # fixed 15-frame closed GOP with 2 B-frames
-b:v 50M -maxrate 50M -minrate 50M -bufsize 35000k   # CBR-like 50Mbps (XDCAM50)
-c:a pcm_s24le                                # uncompressed 24-bit PCM, one discrete track per source channel
```

Plus, separately: burn in a real timecode (compute drop-frame timecode correctly for
29.97fps — frame numbers 0 and 1 are skipped at the start of every minute except
multiples of 10) via `-timecode` and matching `-metadata:s:v:0 timecode=...` plus a
`tmcd` data track tag, and use ffmpeg's own segment muxer
(`-f segment -segment_time <N> -segment_format mxf -strftime 1 <pattern>`) to roll a
new MXF file every N seconds (e.g. hourly) rather than running one unbounded file per
session — this bounds both file size and the blast radius of any single-file
corruption, and gives the transfer layer (§5) natural per-file boundaries to work
against.

## 9. Enlarge ffmpeg's stdout/stderr pipe buffers

Default OS pipe buffer size (commonly 64KB on Linux) can fill up during a burst of
ffmpeg log output (error spam, or the moment a segment rolls over and ffmpeg logs a
flurry of index/flush messages) — once full, ffmpeg's own `write()` to that pipe
blocks, which stalls *ffmpeg itself*, not just your reader. A watchdog that infers
"hung" from a stale progress timestamp will then misdiagnose this as a real hang and
trigger an unwanted restart, when the actual cause was your own pipe being too small.
Fix: immediately after spawning the subprocess, reach into the underlying OS pipe file
descriptors for stdout/stderr and grow them (on Linux, `fcntl(fd, F_SETPIPE_SZ, size)`,
e.g. to 512KB) before any output can accumulate. Treat failure to do this as
non-fatal (log a warning, keep running) — some kernels/permission setups may not allow
it, and the system should degrade gracefully rather than refuse to start.

## 10. Per-channel I/O thread-pool isolation (the hybrid model's load-bearing detail)

If you chose the asyncio-hybrid concurrency model in §0: give **each encoder its own
small dedicated `ThreadPoolExecutor`** (e.g. 2-3 workers) for all of that encoder's
blocking filesystem calls, and route every blocking call through it explicitly —
do *not* use `asyncio.to_thread()` for this. `asyncio.to_thread()` draws from one
process-wide shared pool; if the recording/transfer destination is a network mount
(NFS, SMB) and it ever hangs on a blocking syscall (`open`, `read`, `write`,
`os.path.getsize`, `statvfs`) with no timeout, that one stuck call occupies one shared
worker thread indefinitely. Enough simultaneous channels hitting the same flaky mount
can exhaust the shared pool entirely, at which point *every other channel's* unrelated
filesystem calls also start queuing behind the stuck ones — a single bad network mount
starves the whole fleet. A dedicated per-channel executor means a stuck NFS call can
only ever starve that one channel's own future filesystem work; every other channel's
dedicated pool is completely unaffected. This is cheap (a few extra idle OS threads
per channel) and is the single detail that makes the hybrid model's isolation
guarantee actually hold under real network-storage failure conditions — without it,
"async for control flow, threads for I/O" doesn't actually buy you fault isolation
between channels.

## 11. HD video + 8-channel audio test signal (no hardware input needed)

For testing the encoder pipeline, UI, transfer logic, and watchdog without a real
capture card attached, synthesize the input via `lavfi` instead of a hardware source —
and feed it through the *exact same* filter-graph structure as the real input (§2), so
the test path exercises identical downstream code:

```
-f lavfi -re -i testsrc2=size=1920x1080:rate=60000/1001
-f lavfi -re -i sine=frequency=1000:sample_rate=48000:beep_factor=4
```

- **`-re` on both inputs is mandatory, not optional.** Without it, `lavfi` generates
  frames as fast as the CPU allows instead of pacing to wallclock time — an hour-long
  segment would "record" in seconds, breaking every timing assumption downstream
  (watchdog progress-rate checks, segment rollover timing, transfer-stagger logic). `-re`
  makes the synthetic source behave like a real live feed for every consumer downstream.
- **`testsrc2=size=1920x1080:rate=60000/1001`**: a moving HD test pattern (resolution bars
  + a readable frame counter) at true 59.94p (`60000/1001`, not a rounded `60`) — use the
  exact NTSC-family fractional rate, since downstream interlace/timecode math (§8) is
  built around 59.94/29.97, and a rounded 60fps source will drift against drop-frame
  timecode over time.
- **`testsrc2` is progressive**; if production otherwise records interlaced (§8), convert
  it explicitly in the filter graph: `fps=60000/1001,tinterlace=4` (mode 4 = interleave
  alternating fields from the progressive source) *before* any split/scale — this makes
  the synthetic source hit the same interlaced encode path as a real interlaced capture
  card, rather than silently testing a progressive-only code path that production never
  actually uses.
- **`sine=frequency=1000:sample_rate=48000:beep_factor=4`**: a single mono 1kHz tone;
  `beep_factor=4` accents every other second's tone at 4x amplitude, giving an audible
  once-per-second marker useful for manually checking A/V sync and channel routing by
  ear. `lavfi` has no ready-made 8-discrete-channel test source, so for a fixture
  standing in for real multi-channel audio, just reuse this single mono source as the
  input to *every* `pan=` branch in the same `asplit=12` graph from §2 (`pan=1c|c0=c0`
  for each of the 8 recorded channels, `pan=stereo|c0=c0|c1=c0` for each preview pair) —
  the point is validating the *routing/splitting/encoding machinery*, not supplying
  content-accurate 8-channel audio.
- Gate this behind a per-channel "test mode" flag so an operator can point any encoder
  at the synthetic source instead of its real hardware input without touching any other
  config, and route the flag through `start()`'s input-source selection (test vs real)
  the same way the real system does — but if that flag is ever something a session can
  be mid-recording under, **persist it across process restarts the same way you persist
  the recording-in-progress flag itself.** A real-world failure mode: a channel left
  recording in test mode, the host process restarts for any reason (crash, package
  manager triggering a service bounce, a supervisor-level restart), and the "resume
  interrupted recording" logic on startup doesn't know the channel was in test mode —
  so it silently resumes against the *real* hardware input instead, with nothing in the
  UI calling that out. Whatever field remembers "this channel should auto-resume
  recording after a restart" must carry the test-mode flag right alongside it.
