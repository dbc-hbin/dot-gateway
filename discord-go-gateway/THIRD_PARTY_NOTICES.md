# Third-party provenance

- Hermes references: https://github.com/NousResearch/hermes-agent,
  original bridge reference `f42f579cf8bac4918ac9599bece71618afadd846` and
  follow-up/worker-control reference `10c6188de188871f64a88dd95bc6b262adb0c307`,
  MIT, copyright 2025 Nous Research. The latest behavior review covers
  `plugins/platforms/discord/adapter_media.py`, `gateway/run_turn_runner.py`,
  `gateway/run_startup.py`, and `gateway/slash_commands.py`. This repository
  contains original Go adaptations of limited media/lifecycle behavior, not the
  upstream Python runner. Its external native assistant requires its own
  controller for actual worker recovery and interruption. The full MIT notice
  follows; upstream source is not bundled here.
- DiscordGo `v0.29.1-0.20250705141350-98dc13349786`:
  https://github.com/bwmarrin/discordgo, BSD-3-Clause. Upstream commit
  `98dc1334978683bb0bec263fe809a70e3ec00e91`, with one reviewed Gateway handshake
  patch in `third_party/discordgo/wsapi.go`. The original license, source hashes,
  exact patch and maintenance rationale are retained in that directory.
- Gorilla WebSocket `v1.5.3`: https://github.com/gorilla/websocket, BSD-2-Clause
- modernc SQLite `v1.38.2`: https://gitlab.com/cznic/sqlite, BSD-3-Clause;
  embedded SQLite is public domain. Exact transitive versions/checksums are in
  `go.mod`/`go.sum`; their license notices are included in module distributions.

## Hermes MIT notice

Copyright (c) 2025 Nous Research

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
