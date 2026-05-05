import SwiftUI

@main
struct HostApp: App {
    var body: some Scene {
        WindowGroup {
            VStack(spacing: 16) {
                Text("ReviewLoop Widget Host").font(.title)
                Text("Quit me if you want; the widget keeps running once installed.")
                    .font(.body)
                    .multilineTextAlignment(.center)
                    .padding()
            }.frame(width: 360, height: 200)
        }
    }
}
