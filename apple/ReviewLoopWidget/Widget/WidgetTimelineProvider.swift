import Foundation
import WidgetKit

// MARK: - Errors

enum WidgetReadError: Error {
    case containerUnavailable
    case fileNotFound(URL)
    case decodingFailed(Error)
}

// MARK: - JSON loading

private let appGroupID = "group.ai.reviewloop.local"
private let fileName = "widget-state.json"

private func makeDecoder() -> JSONDecoder {
    let decoder = JSONDecoder()
    let fmt = ISO8601DateFormatter()
    fmt.formatOptions = [.withInternetDateTime]
    decoder.dateDecodingStrategy = .custom { dec in
        let str = try dec.singleValueContainer().decode(String.self)
        guard let date = fmt.date(from: str) else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: dec.codingPath,
                      debugDescription: "Expected ISO8601 date, got: \(str)")
            )
        }
        return date
    }
    return decoder
}

/// Returns the preferred App Group container URL, then falls back to `~/.review_loop/`.
private func candidateURLs() -> [URL] {
    var urls: [URL] = []
    if let container = FileManager.default
        .containerURL(forSecurityApplicationGroupIdentifier: appGroupID) {
        urls.append(container.appendingPathComponent(fileName))
    }
    // Fallback: home-dir path. Only works when sandbox is absent or user granted access.
    // TODO: verify on build — NSOpenPanel-based access may be required in production builds.
    let home = FileManager.default.homeDirectoryForCurrentUser
    urls.append(home.appendingPathComponent(".review_loop/\(fileName)"))
    return urls
}

func loadWidgetState() -> Result<WidgetState, WidgetReadError> {
    let decoder = makeDecoder()
    for url in candidateURLs() {
        guard FileManager.default.fileExists(atPath: url.path) else { continue }
        do {
            let data = try Data(contentsOf: url)
            let state = try decoder.decode(WidgetState.self, from: data)
            return .success(state)
        } catch let err as WidgetReadError {
            return .failure(err)
        } catch {
            return .failure(.decodingFailed(error))
        }
    }
    return .failure(.fileNotFound(candidateURLs().first ?? URL(fileURLWithPath: fileName)))
}

// MARK: - Timeline entry

struct ReviewLoopEntry: TimelineEntry {
    let date: Date
    let result: Result<WidgetState, WidgetReadError>
}

// MARK: - TimelineProvider

struct ReviewLoopTimelineProvider: TimelineProvider {
    typealias Entry = ReviewLoopEntry

    func placeholder(in context: Context) -> ReviewLoopEntry {
        ReviewLoopEntry(date: Date(), result: .success(.placeholder))
    }

    func getSnapshot(in context: Context, completion: @escaping (ReviewLoopEntry) -> Void) {
        let result = loadWidgetState()
        completion(ReviewLoopEntry(date: Date(), result: result))
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<ReviewLoopEntry>) -> Void) {
        let now = Date()
        let result = loadWidgetState()
        let entry = ReviewLoopEntry(date: now, result: result)
        // Minimum recommended refresh interval for non-system widgets is 5 minutes.
        let nextRefresh = Calendar.current.date(byAdding: .minute, value: 5, to: now) ?? now
        let timeline = Timeline(entries: [entry], policy: .after(nextRefresh))
        completion(timeline)
    }
}
