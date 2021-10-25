use vector_core::{
    event::{Metric, MetricKind, MetricValue},
    metrics::AgentDDSketch,
};

use crate::sinks::util::buffer::metrics::{MetricNormalize, MetricSet};

pub struct DatadogMetricsNormalizer;

impl MetricNormalize for DatadogMetricsNormalizer {
    fn apply_state(state: &mut MetricSet, metric: Metric) -> Option<Metric> {
        // We primarily care about making sure that counters are incremental, and that gauges are
        // always absolute.  For other metric kinds, we want them to be incremental.
        match &metric.value() {
            // We always send counters as incremental and gauges as absolute.  Realistically, any
            // system sending an incremental gauge update is kind of doing it wrong, but alas.
            MetricValue::Counter { .. } => state.make_incremental(metric),
            MetricValue::Gauge { .. } => state.make_absolute(metric),
            // We convert distributions and aggregated histograms to sketches internally. We can't
            // send absolute sketches to Datadog, though, so we incrementalize them first.
            MetricValue::Distribution { .. } => state
                .make_incremental(metric)
                .filter(|metric| !metric.value().is_empty())
                .map(AgentDDSketch::transform_to_sketch),
            MetricValue::AggregatedHistogram { .. } => state
                .make_incremental(metric)
                .filter(|metric| !metric.value().is_empty())
                .map(AgentDDSketch::transform_to_sketch),
            // Sketches cannot be subtracted from one another, so we treat them as implicitly
            // incremental, and just update the metric type.
            MetricValue::Sketch { .. } => Some(metric.into_incremental()),
            // Otherwise, ensure that it's incremental.
            _ => state.make_incremental(metric),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use vector_core::event::{
        metric::{Bucket, Sample},
        Metric, MetricKind, MetricValue, StatisticKind,
    };

    use crate::sinks::util::buffer::metrics::{MetricNormalize, MetricSet};

    use super::DatadogMetricsNormalizer;

    fn buckets_from_samples(values: &[f64]) -> (Vec<Bucket>, f64, u32) {
        // Generate buckets, and general statistics, for an input set of data.  We only use this in
        // tests, and so we have some semi-realistic buckets here, but mainly we use them for testing,
        // not for most accurately/efficiently representing the input samples.
        let bounds = &[
            1.0,
            2.0,
            4.0,
            8.0,
            16.0,
            32.0,
            64.0,
            128.0,
            256.0,
            512.0,
            1024.0,
            f64::INFINITY,
        ];
        let mut buckets = bounds
            .iter()
            .map(|b| Bucket {
                upper_limit: *b,
                count: 0,
            })
            .collect::<Vec<_>>();

        let mut sum = 0.0;
        let mut count = 0;
        for value in values {
            for bucket in buckets.iter_mut() {
                if *value <= bucket.upper_limit {
                    bucket.count += 1;
                }
            }

            sum += *value;
            count += 1;
        }

        (buckets, sum, count)
    }

    fn get_counter(value: f64, kind: MetricKind) -> Metric {
        Metric::new("counter", kind, MetricValue::Counter { value })
    }

    fn get_gauge(value: f64, kind: MetricKind) -> Metric {
        Metric::new("gauge", kind, MetricValue::Gauge { value })
    }

    fn get_set<S, N>(values: S, kind: MetricKind) -> Metric
    where
        S: IntoIterator<Item = N>,
        N: fmt::Display,
    {
        Metric::new(
            "set",
            kind,
            MetricValue::Set {
                values: values.into_iter().map(|i| i.to_string()).collect(),
            },
        )
    }

    fn get_distribution<S, N>(samples: S, kind: MetricKind) -> Metric
    where
        S: IntoIterator<Item = N>,
        N: Into<f64>,
    {
        Metric::new(
            "distribution",
            kind,
            MetricValue::Distribution {
                samples: samples
                    .into_iter()
                    .map(|n| Sample {
                        value: n.into(),
                        rate: 1,
                    })
                    .collect(),
                statistic: StatisticKind::Histogram,
            },
        )
    }

    fn get_aggregated_histogram<S, N>(samples: S, kind: MetricKind) -> Metric
    where
        S: IntoIterator<Item = N>,
        N: Into<f64>,
    {
        let samples = samples.into_iter().map(Into::into).collect::<Vec<_>>();
        let (buckets, sum, count) = buckets_from_samples(&samples);

        Metric::new(
            "agg_histogram",
            kind,
            MetricValue::AggregatedHistogram {
                buckets,
                count,
                sum,
            },
        )
    }

    #[test]
    fn absolute_counter() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;

        let counters = vec![
            get_counter(first_value, MetricKind::Absolute),
            get_counter(second_value, MetricKind::Absolute),
        ];

        let expected_counters = vec![
            None,
            Some(get_counter(
                second_value - first_value,
                MetricKind::Incremental,
            )),
        ];

        for (input, expected) in counters.into_iter().zip(expected_counters) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn incremental_counter() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;

        let counters = vec![
            get_counter(first_value, MetricKind::Incremental),
            get_counter(second_value, MetricKind::Incremental),
        ];

        let expected_counters = counters
            .clone()
            .into_iter()
            .map(Option::Some)
            .collect::<Vec<_>>();

        for (input, expected) in counters.into_iter().zip(expected_counters) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn mixed_counter() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;
        let third_value = 16.19;

        let counters = vec![
            get_counter(first_value, MetricKind::Incremental),
            get_counter(second_value, MetricKind::Absolute),
            get_counter(third_value, MetricKind::Absolute),
            get_counter(first_value, MetricKind::Absolute),
            get_counter(second_value, MetricKind::Incremental),
            get_counter(third_value, MetricKind::Incremental),
        ];

        let expected_counters = vec![
            Some(get_counter(first_value, MetricKind::Incremental)),
            None,
            Some(get_counter(
                third_value - second_value,
                MetricKind::Incremental,
            )),
            None,
            Some(get_counter(second_value, MetricKind::Incremental)),
            Some(get_counter(third_value, MetricKind::Incremental)),
        ];

        for (input, expected) in counters.into_iter().zip(expected_counters) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn absolute_gauge() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;

        let gauges = vec![
            get_gauge(first_value, MetricKind::Absolute),
            get_gauge(second_value, MetricKind::Absolute),
        ];

        let expected_gauges = gauges
            .clone()
            .into_iter()
            .map(Option::Some)
            .collect::<Vec<_>>();

        for (input, expected) in gauges.into_iter().zip(expected_gauges) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn incremental_gauge() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;

        let gauges = vec![
            get_gauge(first_value, MetricKind::Incremental),
            get_gauge(second_value, MetricKind::Incremental),
        ];

        let expected_gauges = vec![
            Some(get_gauge(first_value, MetricKind::Absolute)),
            Some(get_gauge(first_value + second_value, MetricKind::Absolute)),
        ];

        for (input, expected) in gauges.into_iter().zip(expected_gauges) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn mixed_gauge() {
        let mut metric_set = MetricSet::default();

        let first_value = 3.14;
        let second_value = 8.675309;
        let third_value = 16.19;

        let gauges = vec![
            get_gauge(first_value, MetricKind::Incremental),
            get_gauge(second_value, MetricKind::Absolute),
            get_gauge(third_value, MetricKind::Absolute),
            get_gauge(first_value, MetricKind::Absolute),
            get_gauge(second_value, MetricKind::Incremental),
            get_gauge(third_value, MetricKind::Incremental),
        ];

        let expected_gauges = vec![
            Some(get_gauge(first_value, MetricKind::Absolute)),
            Some(get_gauge(second_value, MetricKind::Absolute)),
            Some(get_gauge(third_value, MetricKind::Absolute)),
            Some(get_gauge(first_value, MetricKind::Absolute)),
            Some(get_gauge(first_value + second_value, MetricKind::Absolute)),
            Some(get_gauge(
                first_value + second_value + third_value,
                MetricKind::Absolute,
            )),
        ];

        for (input, expected) in gauges.into_iter().zip(expected_gauges) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn absolute_set() {
        let mut metric_set = MetricSet::default();

        let sets = vec![
            get_set(1..=20, MetricKind::Absolute),
            get_set(15..=25, MetricKind::Absolute),
        ];

        let expected_sets = vec![None, Some(get_set(21..=25, MetricKind::Incremental))];

        for (input, expected) in sets.into_iter().zip(expected_sets) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn incremental_set() {
        let mut metric_set = MetricSet::default();

        let sets = vec![
            get_set(1..=20, MetricKind::Incremental),
            get_set(15..=25, MetricKind::Incremental),
        ];

        let expected_sets = vec![
            Some(get_set(1..=20, MetricKind::Incremental)),
            Some(get_set(15..=25, MetricKind::Incremental)),
        ];

        for (input, expected) in sets.into_iter().zip(expected_sets) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn mixed_set() {
        let mut metric_set = MetricSet::default();

        let sets = vec![
            get_set(1..=20, MetricKind::Incremental),
            get_set(10..=16, MetricKind::Absolute),
            get_set(15..=25, MetricKind::Absolute),
            get_set(1..5, MetricKind::Incremental),
            get_set(3..=42, MetricKind::Incremental),
        ];

        let expected_sets = vec![
            Some(get_set(1..=20, MetricKind::Incremental)),
            None,
            Some(get_set(17..=25, MetricKind::Incremental)),
            Some(get_set(1..5, MetricKind::Incremental)),
            Some(get_set(3..=42, MetricKind::Incremental)),
        ];

        for (input, expected) in sets.into_iter().zip(expected_sets) {
            let result = DatadogMetricsNormalizer::apply_state(&mut metric_set, input);
            assert_eq!(result, expected);
        }
    }
}
