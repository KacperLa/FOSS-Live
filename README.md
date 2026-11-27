# FOSS-Live

A compact Rust voice API connecting speech recognition, a tool-capable chat model,
and streaming speech synthesis to an external agent. Application code and synthetic
tests live in `src/main.rs`.

This implements an **OpenAI Live client-delegation subset**, not the complete OpenAI
Realtime API or a drop-in replacement for every Realtime client.

## How it works

Audio arrives over WebRTC (Opus) or WebSocket (PCM). Voice activity and optional
decision checks determine turn completion and interruptions. The chat model streams
completed sentences to TTS and can call `delegate_to_agent({"task":"..."})`.
Your client executes that task with its own agent and sends progress/results back.
This server does not execute shell commands, browse, or control devices itself.

Provider adapters support OpenAI-style chat completions, multipart WAV transcription,
streaming WAV speech output, and an optional decision endpoint. The tested speech
providers are Parakeet and Pocket TTS. Experimental `native_audio_in` sends audio
directly to a compatible multimodal chat model instead of ASR. It has no real input
transcript and omits transcript-based turn/interruption decisions.

## Build and run

Requires Rust 1.92+, Linux, libopus and preferably libopus-dev/pkg-config.

```sh
./check.sh
./check.sh build --release --locked
cp config.example.json config.json
chmod 600 config.json
# Edit config.json: set a strong private API key and your provider endpoints/keys.
LOCAL_LIVE_CONFIG="$PWD/config.json" LOCAL_LIVE_BIND=127.0.0.1:8652 ./target/release/local-live-rust
```

Put an authenticated HTTPS proxy in front for browser use. Keep project credentials
server-side. `./check.sh` runs formatting, Clippy, and synthetic tests; tests open
loopback sockets but need no GPU, real providers, microphone, or credentials.
The script also enforces a conservative 200 KB limit on the main source file.

## Client protocol

```text
Supported protocol (OpenAI Live CLIENT-DELEGATION SUBSET, not full Realtime)
  GET /health
  POST /v1/live/sessions           WebRTC SDP offer -> 201 answer
  WS /v1/live/sessions             session.start, then PCM16LE mono 24 kHz
  GET /v1/live/sessions/{id}       owning project's resource
  DELETE /v1/live/sessions/{id}
  GET /v1/live/sessions/{id}/diagnostics   metadata only, owning project
  WS /v1/live/sessions/{id}/attach server control; max two observers

  Every session endpoint requires Authorization: Bearer <project-key>.
  Session creation takes {session:{...},transport:{type:"webrtc",sdp:"..."}}.
  Model aliases local-live/gpt-live-1 select the local pipeline, never a cloud API.
  Session fields: instructions, input text history, audio format/output voice,
    delegation:{type:"client"}, and optional client.data_channel event allowlists.
  Only the configured voice is accepted. PCM 24k is the internal media format.
  WebRTC negotiates Opus and carries events on the oai-events data channel.

  Commands: session.input_audio.append (WebSocket primary only, base64 "audio"),
    session.input_audio.mute/unmute, session.close,
    session.thinking.append, session.commentary.append, session.instructions.append.
  Appends carry content, optional delegation_id, and preferably unique event_id.
  A delegation ID must belong to this session. Null means unassociated commentary.
  Commentary is verified speakable result text and streams directly through TTS;
    progress is an observation added to model context. New feedback is considered
    for a natural spoken update when idle; the model may summarize it or stay silent.
    This also works without a delegation_id. New feedback is independent of the
    timed keepalive limit. Repeated identical observations do not trigger more speech.
    Feedback never goes directly to TTS and never enables new tool calls.
  Server emits session.started, session.input_transcript.delta,
    session.output_transcript.delta, session.delegation.created (includes task),
    session.output_audio.delta (WS PCM), session.output_audio.cleared,
    append acknowledgements, error, session.closed.
  Captions mark transport delivery, not proof a person heard the audio.
  Clients MUST flush their own playback queue on session.output_audio.cleared.
  Data-channel allowlists never widen server-side permissions. Sidebands cannot
    inject microphone audio or start sessions; disconnecting one leaves calls open.
  Sideband subscription is future-only. There is no event replay or automatic
    action retry. Accepted results survive interruptions within a live session,
    NOT process restarts. The executor must persist task outcomes independently.

```

## Boundaries and privacy

- Clients own tool permissions, approvals, task persistence, and cancellation.
- Interrupting speech does not cancel an external action.
- Results survive interruptions within a session, not server restarts.
- Complete validated arguments are required before delegation. There is no
  post-response delegation policing or forced-tool retry.
- Input audio is not saved to disk. Optional conversation logging is disabled
  in the example configuration; enabling it can retain personal text.
- Providers and clients may have their own logging policies.
- Queues, history and provider responses are bounded. Secrets and raw provider
  errors are excluded from application logs.

The Signal/Hermes transport bridge, production configuration, deployment records,
recordings, binaries, and private conversation logs are not included.
