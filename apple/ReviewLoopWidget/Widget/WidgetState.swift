//
//  WidgetState.swift
//  ReviewLoopWidget
//
//  This file mirrors the Rust-emitted JSON schema. See
//  docs/widget-schema.md (in the reviewloop repo) for the full
//  contract, schema-bump procedure, and field semantics.
//

import Foundation

// MARK: - Top-level snapshot

/// Mirrors the JSON contract written by the reviewloop daemon (schema_version = 1).
struct WidgetState: Codable {
    let schemaVersion: Int
    let generatedAt: Date
    let projectId: String
    let summary: Summary
    let activeJobs: [ActiveJob]
    let recentFailures: [Failure]
    let lastTickAt: Date
    let lastTickError: TickError?
    let tickHealth: TickHealth

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedAt = "generated_at"
        case projectId = "project_id"
        case summary
        case activeJobs = "active_jobs"
        case recentFailures = "recent_failures"
        case lastTickAt = "last_tick_at"
        case lastTickError = "last_tick_error"
        case tickHealth = "tick_health"
    }
}

// MARK: - Summary

extension WidgetState {
    struct Summary: Codable {
        let activeCount: Int
        let failedRecent24h: Int
        let completedToday: Int

        enum CodingKeys: String, CodingKey {
            case activeCount = "active_count"
            case failedRecent24h = "failed_recent_24h"
            case completedToday = "completed_today"
        }
    }
}

// MARK: - ActiveJob

extension WidgetState {
    struct ActiveJob: Codable, Identifiable {
        let paperId: String
        let status: String
        let attempt: Int
        let nextPollAt: Date?
        let startedAt: Date?

        var id: String { paperId }

        enum CodingKeys: String, CodingKey {
            case paperId = "paper_id"
            case status
            case attempt
            case nextPollAt = "next_poll_at"
            case startedAt = "started_at"
        }
    }
}

// MARK: - Failure

extension WidgetState {
    struct Failure: Codable, Identifiable {
        let paperId: String
        let status: String
        let lastError: String
        let occurredAt: Date

        var id: String { paperId }

        enum CodingKeys: String, CodingKey {
            case paperId = "paper_id"
            case status
            case lastError = "last_error"
            case occurredAt = "occurred_at"
        }
    }
}

// MARK: - TickError

extension WidgetState {
    struct TickError: Codable {
        let at: Date
        let message: String
    }
}

// MARK: - TickHealth

extension WidgetState {
    enum TickHealth: String, Codable {
        case normal
        case stale
        case stuck
        case unknown

        // Fallback for unknown future values
        init(from decoder: Decoder) throws {
            let raw = try decoder.singleValueContainer().decode(String.self)
            self = TickHealth(rawValue: raw) ?? .unknown
        }
    }
}

// MARK: - Placeholder

extension WidgetState {
    static var placeholder: WidgetState {
        let now = Date()
        return WidgetState(
            schemaVersion: 1,
            generatedAt: now,
            projectId: "—",
            summary: Summary(activeCount: 0, failedRecent24h: 0, completedToday: 0),
            activeJobs: [],
            recentFailures: [],
            lastTickAt: now,
            lastTickError: nil,
            tickHealth: .unknown
        )
    }
}
