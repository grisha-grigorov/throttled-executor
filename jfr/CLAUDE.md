# jfr — `:executors-jfr`

Loaded when a file under `jfr/` is touched. Satellites use the public SPI only; see the
root `CLAUDE.md`. `JfrTaskListener` — one Flight Recorder event per task. Discoverable via `provides` and `META-INF/services`; `jdk.jfr` is `requires transitive`.

The contract below is this satellite's README, imported so it loads only in sessions that
touch this directory.

@README.md
