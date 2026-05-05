# ReviewLoopWidget

A macOS WidgetKit extension that displays a glance-friendly summary of the reviewloop daemon's current state in the desktop or Notification Center widget gallery.

## Prerequisites

| Tool | Version |
|------|---------|
| macOS | 15.0+ |
| Xcode | 16.0+ |
| xcodegen | any recent (`brew install xcodegen`) |

## Build & Install

```bash
cd apple/ReviewLoopWidget

# 1. Generate the Xcode project from project.yml
xcodegen generate

# 2. Open the generated project
open ReviewLoopWidget.xcodeproj
```

In Xcode:

3. **Select your Personal Team** for both `HostApp` and `Widget` targets under *Signing & Capabilities*.

4. **Update the App Group identifier** — the placeholder `group.ai.reviewloop.local` is not a real registered group.  
   Replace it with `group.<your-bundle-id>.shared` in:
   - `HostApp/HostApp.entitlements`
   - `Widget/Widget.entitlements`
   
   Also update the constant in `Widget/WidgetTimelineProvider.swift`:
   ```swift
   private let appGroupID = "group.<your-bundle-id>.shared"
   ```

5. **Configure the reviewloop daemon** to write the snapshot into the App Group container:
   ```toml
   # ~/.config/reviewloop/config.toml
   [core]
   widget_state_dir = "/Users/<you>/Library/Group Containers/group.<your-bundle-id>.shared"
   ```
   The daemon (W2A) will write `widget-state.json` into that directory.

6. **Build & run** (`⌘R`) the HostApp scheme. The host app window just needs to appear once so macOS registers the widget extension.

7. **Add the widget**: open Notification Center or right-click the desktop → *Edit Widgets* → search for "ReviewLoop". Two sizes are available: Small and Medium.

## JSON snapshot contract

The widget reads `widget-state.json` written by the reviewloop daemon. Schema version 1:

```json
{
  "schema_version": 1,
  "generated_at": "2026-05-05T20:16:58Z",
  "project_id": "main",
  "summary": { "active_count": 3, "failed_recent_24h": 1, "completed_today": 2 },
  "active_jobs": [
    { "paper_id": "main", "status": "PROCESSING", "attempt": 2,
      "next_poll_at": "2026-05-05T20:25:00Z", "started_at": "2026-05-05T19:50:00Z" }
  ],
  "recent_failures": [
    { "paper_id": "workshop", "status": "FAILED",
      "last_error": "review generation failed",
      "occurred_at": "2026-05-05T18:00:00Z" }
  ],
  "last_tick_at": "2026-05-05T20:16:30Z",
  "last_tick_error": null,
  "tick_health": "normal"
}
```

All timestamps are RFC 3339 / ISO 8601 (UTC `Z`).

## File lookup order

1. App Group container (`group.<your-bundle-id>.shared/widget-state.json`) — works in production sandbox.
2. `~/.review_loop/widget-state.json` — fallback for development/debugging. **Only works if the sandbox is disabled or the user has explicitly granted file access via `NSOpenPanel`.**

## Known limitations

- **Refresh rate**: WidgetKit enforces a budget of ~40–70 background refreshes per day per widget family. The provider requests a 5-minute refresh interval; actual refresh may be less frequent.
- **Sandbox**: The app-group container path requires both the daemon and the widget to be signed with the same App Group entitlement. Without a paid developer account this requires disabling the sandbox (for development only).
- **Data is read-only**: the widget cannot send commands back to the daemon.
- **Daemon must be running**: if the daemon hasn't written the snapshot yet, the widget displays an "unconfigured" state.

## Directory layout

```
apple/ReviewLoopWidget/
  project.yml                    ← xcodegen project definition
  README.md                      ← this file
  HostApp/
    HostAppApp.swift             ← minimal SwiftUI host app
    Info.plist
    HostApp.entitlements         ← App Group placeholder
  Widget/
    WidgetBundle.swift           ← @main WidgetBundle
    Widget.swift                 ← StaticConfiguration + family dispatch
    WidgetTimelineProvider.swift ← JSON reader + Timeline builder
    WidgetView.swift             ← SwiftUI views (Small + Medium)
    WidgetState.swift            ← Codable models matching JSON schema
    Info.plist
    Widget.entitlements          ← App Group placeholder
```
