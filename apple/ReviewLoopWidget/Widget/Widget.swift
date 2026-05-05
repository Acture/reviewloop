import WidgetKit
import SwiftUI

struct ReviewLoopWidget: Widget {
    let kind: String = "ReviewLoopWidget"

    var body: some WidgetConfiguration {
        StaticConfiguration(kind: kind, provider: ReviewLoopTimelineProvider()) { entry in
            ReviewLoopEntryView(entry: entry)
        }
        .configurationDisplayName("ReviewLoop")
        .description("Glance at active reviews and recent failures.")
        .supportedFamilies([.systemSmall, .systemMedium])
    }
}
