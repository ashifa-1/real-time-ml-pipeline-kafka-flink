# Analysis

## Batch vs. Streaming Divergence

The streaming pipeline computes features continuously as events arrive, while a batch approach would only compute features at fixed intervals over a static snapshot. In this implementation, the Flink job computes user features with a 1-hour tumbling event-time window, content engagement with a 15-minute sliding window, and category affinity scores using joined metadata. A batch computation on the same event set would produce a single summary for a fixed cut-off time and would not preserve the intermediate state updates that the feature-store topic delivers in real time.

Key differences:

* Streaming features are updated immediately after a window closes and may change as new windows start. For example, `click_rate` and `avg_dwell_time` are emitted for each user every hour, while a batch run would only provide a final aggregation for the full dataset.
* The sliding window used for `engagement_rate` produces multiple overlapping summaries every 5 minutes. A batch job cannot reproduce this near-real-time cadence without running repeatedly.
* Late and out-of-order events are handled by watermarks in the streaming pipeline, so some events may be incorporated into earlier windows after arrival. A batch run would simply include all events in the same pass and would not expose the timing semantics of window closure.

Because streaming manages event time explicitly, values from the pipeline can differ from a naive batch aggregate when events arrive late or out of order. This is expected: real-time models need feature values that reflect the time boundaries of windows, not just the total counts in a static dataset.

## Late Event Handling

The Flink job is configured with event-time processing and a bounded out-of-orderness watermark strategy of exactly 30 seconds. This means the job will wait up to 30 seconds for late data before closing a window and emitting results.

Evidence from the pipeline:

* The producer intentionally emits at least 5% of `user-events` with timestamps between 35 and 90 seconds behind the current simulation clock.
* The Flink job writes a separate `pipeline-metrics` stream that includes `watermark_ms` and `late_event_count` values.
* The dashboard consumes those metrics and displays them in real time.

If an event arrives more than 30 seconds behind the current watermark, it is classified as late. The implementation counts these events and exposes them in the dashboard as `Late Events Count`. This makes the watermark behavior visible and explains why some events are no longer eligible for their original window once the watermark has advanced past their timestamp.

Overall, the pipeline is designed so that late arrival is tolerated within the specified window, but genuinely tardy events are still tracked and surfaced as operational telemetry.
