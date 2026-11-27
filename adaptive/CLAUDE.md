# adaptive — `:executors-adaptive`

Loaded when a file under `adaptive/` is touched. Satellites use the public SPI only; see the
root `CLAUDE.md`. `AdaptiveSizing` learns each task's memory cost and duration; `AdaptiveSizingPolicy` decorates any policy with resized tasks and quarantines unknown names by declaring them `requiresExclusive` (core does the isolating). Samples are stamped with time and `AdaptiveConfig.version()`; stale ones are ignored. Needs a store path, so not discoverable.

The contract below is this satellite's README, imported so it loads only in sessions that
touch this directory.

@README.md
