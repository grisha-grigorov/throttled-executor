# micrometer — `:executors-micrometer`

Loaded when a file under `micrometer/` is touched. Satellites use the public SPI only; see the
root `CLAUDE.md`. `MicrometerTaskListener` — task counts/timers and admission meters to a `MeterRegistry`, tagged by executor id **never task name**. Needs a registry, so not discoverable. `micrometer.core` is only an automatic module, so this project cannot go into a `jlink` image.

The contract below is this satellite's README, imported so it loads only in sessions that
touch this directory.

@README.md
