import SwiftUI
import WidgetKit

// MARK: - Shared helpers

private extension WidgetState.TickHealth {
    var color: Color {
        switch self {
        case .normal: return .green
        case .stale: return .yellow
        case .stuck: return .orange
        case .unknown: return .gray
        }
    }
}

private func relativeLabel(for date: Date?) -> String {
    guard let date else { return "—" }
    let diff = date.timeIntervalSinceNow
    if diff < 0 { return "overdue" }
    let mins = Int(diff / 60)
    return mins < 1 ? "< 1 min" : "in \(mins) min"
}

// MARK: - Unconfigured / error view

private struct UnconfiguredView: View {
    var body: some View {
        VStack(spacing: 6) {
            Image(systemName: "exclamationmark.triangle")
                .font(.title2)
                .foregroundStyle(.secondary)
            Text("No data")
                .font(.headline)
            Text("Run reviewloop daemon")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding()
    }
}

// MARK: - Small widget view

struct WidgetSmallView: View {
    let state: WidgetState

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 4) {
                Image(systemName: "doc.text.magnifyingglass")
                    .foregroundStyle(.accent)
                Text("ReviewLoop")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Text("\(state.summary.activeCount)")
                .font(.system(size: 44, weight: .bold, design: .rounded))
                .minimumScaleFactor(0.5)
                .lineLimit(1)

            Text("active")
                .font(.caption)
                .foregroundStyle(.secondary)

            Spacer()

            failureBadge
        }
        .padding()
        .containerBackground(for: .widget) {
            Color(.windowBackgroundColor)
        }
    }

    @ViewBuilder
    private var failureBadge: some View {
        if state.summary.failedRecent24h > 0 {
            Label(
                "\(state.summary.failedRecent24h) failed (24h)",
                systemImage: "xmark.circle.fill"
            )
            .font(.caption2)
            .foregroundStyle(.red)
        } else {
            Label("all clear", systemImage: "checkmark.circle.fill")
                .font(.caption2)
                .foregroundStyle(.green)
        }
    }
}

// MARK: - Medium widget view

struct WidgetMediumView: View {
    let state: WidgetState

    private var displayedJobs: [WidgetState.ActiveJob] {
        Array(state.activeJobs.prefix(3))
    }

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            // Left column: summary
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 4) {
                    Image(systemName: "doc.text.magnifyingglass")
                        .foregroundStyle(.accent)
                    Text("ReviewLoop")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }

                Spacer()

                Text("\(state.summary.activeCount)")
                    .font(.system(size: 36, weight: .bold, design: .rounded))
                    .minimumScaleFactor(0.5)
                    .lineLimit(1)

                Text("active")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                Spacer()

                failureBadge
            }
            .frame(maxWidth: 90, alignment: .leading)

            Divider()

            // Right column: job list
            VStack(alignment: .leading, spacing: 4) {
                if displayedJobs.isEmpty {
                    Spacer()
                    Text("No active jobs")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                } else {
                    ForEach(displayedJobs) { job in
                        jobRow(job)
                    }
                    Spacer()
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding()
        .containerBackground(for: .widget) {
            Color(.windowBackgroundColor)
        }
    }

    private func jobRow(_ job: WidgetState.ActiveJob) -> some View {
        HStack(spacing: 6) {
            Image(systemName: "circle.fill")
                .font(.system(size: 6))
                .foregroundStyle(.blue)
            VStack(alignment: .leading, spacing: 1) {
                Text(job.paperId)
                    .font(.caption)
                    .fontWeight(.medium)
                    .lineLimit(1)
                HStack(spacing: 4) {
                    Text(job.status)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    Text("·")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    Text(relativeLabel(for: job.nextPollAt))
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    @ViewBuilder
    private var failureBadge: some View {
        if state.summary.failedRecent24h > 0 {
            Label(
                "\(state.summary.failedRecent24h) failed",
                systemImage: "xmark.circle.fill"
            )
            .font(.caption2)
            .foregroundStyle(.red)
        } else {
            Label("all clear", systemImage: "checkmark.circle.fill")
                .font(.caption2)
                .foregroundStyle(.green)
        }
    }
}

// MARK: - Entry view dispatcher

struct ReviewLoopEntryView: View {
    @Environment(\.widgetFamily) private var family
    let entry: ReviewLoopEntry

    var body: some View {
        switch entry.result {
        case .success(let state):
            switch family {
            case .systemMedium:
                WidgetMediumView(state: state)
            default:
                WidgetSmallView(state: state)
            }
        case .failure:
            UnconfiguredView()
        }
    }
}
