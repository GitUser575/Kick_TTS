# Kick TTS App

Windows Python application for Kick chat monitoring, TTS, alerts, voice playback, and stream-session tools.

## Current status

This project is currently distributed as Python source with a setup script. The current baseline version is **0.1.0**.

## Installation

1. Install or clone this repository into a writable folder.
2. Run `setup.bat`.
3. After setup completes, run `Launch Chatterbox.bat`.

The setup script configures the supported Python environment and installs the required audio, browser, TTS, and GPU dependencies.

## Important local files

The application creates user-specific files such as `settings.json`, `voices.json`, `channel_points.json`, `donations.json`, and `stream_session.json`. These files are intentionally excluded from version control and should not be shared.

## Releases

Releases use semantic version tags such as `v0.1.0`, `v0.2.0`, and `v0.2.1`. Future application updates will use release packages while preserving the user's local JSON settings and generated audio.

## License and audio assets

Before distributing this repository or its audio assets publicly, verify that the source code, voice samples, sounds, and other included media may legally be redistributed.
