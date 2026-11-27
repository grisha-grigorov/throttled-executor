# observability — `:executors-observability`

Loaded when a file under `observability/` is touched. Satellites use the public SPI only; see the
root `CLAUDE.md`. `ExecutionRecorder` — bounded history tree with aggregates, text/JSON rendering, JMX MBean. Fold trigger is weak-ref-cleared only. Discoverable via `provides` and `META-INF/services`.

The contract below is this satellite's README, imported so it loads only in sessions that
touch this directory.

@README.md
