#[cfg(all(test, feature = "datadog-agent-integration-tests"))]
mod integration_tests;
#[cfg(test)]
mod tests;

pub mod logs;
pub mod metrics;
pub mod traces;

use std::{fmt::Debug, io::Read, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes};
use chrono::{serde::ts_milliseconds, DateTime, Utc};
use codecs::decoding::{DeserializerConfig, FramingConfig};
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use futures::FutureExt;
use http::StatusCode;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tracing::Span;
use value::Kind;
use vector_config::configurable_component;
use vector_core::event::{BatchNotifier, BatchStatus};
use warp::{filters::BoxedFilter, reject::Rejection, reply::Response, Filter, Reply};

use crate::{
    codecs::{Decoder, DecodingConfig},
    config::{
        log_schema, AcknowledgementsConfig, DataType, GenerateConfig, Output, Resource,
        SourceConfig, SourceContext, SourceDescription,
    },
    event::Event,
    internal_events::{HttpBytesReceived, HttpDecompressError, StreamClosedError},
    schema,
    serde::{bool_or_struct, default_decoding, default_framing_message_based},
    sources::{self, util::ErrorMessage},
    tls::{MaybeTlsSettings, TlsEnableableConfig},
    SourceSender,
};

pub const LOGS: &str = "logs";
pub const METRICS: &str = "metrics";
pub const TRACES: &str = "traces";

/// Configuration for the `datadog_agent` source.
#[configurable_component(source)]
#[derive(Clone, Debug)]
pub struct DatadogAgentConfig {
    /// The address to accept connections on.
    ///
    /// The address _must_ include a port.
    address: SocketAddr,

    /// When incoming events contain a Datadog API key, if this setting is set to `true` the key will kept in the event
    /// metadata and will be used if the event is sent to a Datadog sink.
    #[serde(default = "crate::serde::default_true")]
    store_api_key: bool,

    /// If this settings is set to `true`, logs won't be accepted by the component.
    #[serde(default = "crate::serde::default_false")]
    disable_logs: bool,

    /// If this settings is set to `true`, metrics won't be accepted by the component.
    #[serde(default = "crate::serde::default_false")]
    disable_metrics: bool,

    /// If this settings is set to `true`, traces won't be accepted by the component.
    #[serde(default = "crate::serde::default_false")]
    disable_traces: bool,

    /// If this setting is set to `true` logs, metrics and traces will be sent to different outputs.
    ///
    /// For a source component named `agent` the received logs, metrics, and traces can then be accessed by specifying
    /// `agent.logs`, `agent.metrics`, and `agent.traces`, respectively, as the input to another component.
    #[serde(default = "crate::serde::default_false")]
    multiple_outputs: bool,

    #[configurable(derived)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    #[serde(default = "default_framing_message_based")]
    framing: FramingConfig,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for DatadogAgentConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:8080".parse().unwrap(),
            tls: None,
            store_api_key: true,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
            acknowledgements: AcknowledgementsConfig::default(),
            disable_logs: false,
            disable_metrics: false,
            disable_traces: false,
            multiple_outputs: false,
        })
        .unwrap()
    }
}

inventory::submit! {
    SourceDescription::new::<DatadogAgentConfig>("datadog_agent")
}

#[async_trait::async_trait]
#[typetag::serde(name = "datadog_agent")]
impl SourceConfig for DatadogAgentConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let logs_schema_definition = cx
            .schema_definitions
            .get(&Some(LOGS.to_owned()))
            .or_else(|| cx.schema_definitions.get(&None))
            .expect("registered log schema required")
            .clone();
        let metrics_schema_definition = cx
            .schema_definitions
            .get(&Some(METRICS.to_owned()))
            .or_else(|| cx.schema_definitions.get(&None))
            .expect("registered metrics schema required")
            .clone();

        let decoder = DecodingConfig::new(self.framing.clone(), self.decoding.clone()).build();
        let tls = MaybeTlsSettings::from_config(&self.tls, true)?;
        let source = DatadogAgentSource::new(
            self.store_api_key,
            decoder,
            tls.http_protocol_name(),
            logs_schema_definition,
            metrics_schema_definition,
        );
        let listener = tls.bind(&self.address).await?;
        let acknowledgements = cx.do_acknowledgements(&self.acknowledgements);
        let filters = source.build_warp_filters(cx.out, acknowledgements, self)?;
        let shutdown = cx.shutdown;
        Ok(Box::pin(async move {
            let span = Span::current();
            let routes = filters
                .with(warp::trace(move |_info| span.clone()))
                .recover(|r: Rejection| async move {
                    if let Some(e_msg) = r.find::<ErrorMessage>() {
                        let json = warp::reply::json(e_msg);
                        Ok(warp::reply::with_status(json, e_msg.status_code()))
                    } else {
                        // other internal error - will return 500 internal server error
                        Err(r)
                    }
                });
            warp::serve(routes)
                .serve_incoming_with_graceful_shutdown(
                    listener.accept_stream(),
                    shutdown.map(|_| ()),
                )
                .await;

            Ok(())
        }))
    }

    fn outputs(&self) -> Vec<Output> {
        let definition = match self.decoding {
            // See: `LogMsg` struct.
            DeserializerConfig::Bytes => schema::Definition::empty()
                .with_field("message", Kind::bytes(), Some("message"))
                .with_field("status", Kind::bytes(), Some("severity"))
                .with_field("timestamp", Kind::timestamp(), Some("timestamp"))
                .with_field("hostname", Kind::bytes(), Some("host"))
                .with_field("service", Kind::bytes(), Some("service"))
                .with_field("ddsource", Kind::bytes(), Some("source"))
                .with_field("ddtags", Kind::bytes(), Some("tags"))
                .merge(self.decoding.schema_definition()),

            // JSON deserializer can overwrite existing fields at runtime, so we have to treat
            // those events as if there is no known type details we can provide, other than the
            // details provided by the generic JSON schema definition.
            DeserializerConfig::Json => self.decoding.schema_definition(),

            // Syslog deserializer allows for arbritrary "structured data" that can overwrite
            // existing fields, similar to the JSON deserializer.
            //
            // See also: https://datatracker.ietf.org/doc/html/rfc5424#section-6.3
            #[cfg(feature = "sources-syslog")]
            DeserializerConfig::Syslog => self.decoding.schema_definition(),

            DeserializerConfig::Native => self.decoding.schema_definition(),
            DeserializerConfig::NativeJson => self.decoding.schema_definition(),
        };

        if self.multiple_outputs {
            vec![
                Output::default(DataType::Metric).with_port(METRICS),
                Output::default(DataType::Log)
                    .with_schema_definition(definition)
                    .with_port(LOGS),
                Output::default(DataType::Trace).with_port(TRACES),
            ]
        } else {
            vec![Output::default(DataType::all()).with_schema_definition(definition)]
        }
    }

    fn source_type(&self) -> &'static str {
        "datadog_agent"
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Snafu)]
pub(crate) enum ApiError {
    BadRequest,
    InvalidDataFormat,
    ServerShutdown,
}

impl warp::reject::Reject for ApiError {}

#[derive(Deserialize)]
pub struct ApiKeyQueryParams {
    #[serde(rename = "dd-api-key")]
    pub dd_api_key: Option<String>,
}

#[derive(Clone)]
pub(crate) struct DatadogAgentSource {
    pub(crate) api_key_extractor: ApiKeyExtractor,
    pub(crate) log_schema_host_key: &'static str,
    pub(crate) log_schema_timestamp_key: &'static str,
    pub(crate) log_schema_source_type_key: &'static str,
    pub(crate) decoder: Decoder,
    protocol: &'static str,
    logs_schema_definition: Arc<schema::Definition>,
    metrics_schema_definition: Arc<schema::Definition>,
}

#[derive(Clone)]
pub struct ApiKeyExtractor {
    matcher: Regex,
    store_api_key: bool,
}

impl ApiKeyExtractor {
    pub fn extract(
        &self,
        path: &str,
        header: Option<String>,
        query_params: Option<String>,
    ) -> Option<Arc<str>> {
        if !self.store_api_key {
            return None;
        }
        // Grab from URL first
        self.matcher
            .captures(path)
            .and_then(|cap| cap.name("api_key").map(|key| key.as_str()).map(Arc::from))
            // Try from query params
            .or_else(|| query_params.map(Arc::from))
            // Try from header next
            .or_else(|| header.map(Arc::from))
    }
}

impl DatadogAgentSource {
    pub(crate) fn new(
        store_api_key: bool,
        decoder: Decoder,
        protocol: &'static str,
        logs_schema_definition: schema::Definition,
        metrics_schema_definition: schema::Definition,
    ) -> Self {
        Self {
            api_key_extractor: ApiKeyExtractor {
                store_api_key,
                matcher: Regex::new(r"^/v1/input/(?P<api_key>[[:alnum:]]{32})/??")
                    .expect("static regex always compiles"),
            },
            log_schema_host_key: log_schema().host_key(),
            log_schema_source_type_key: log_schema().source_type_key(),
            log_schema_timestamp_key: log_schema().timestamp_key(),
            decoder,
            protocol,
            logs_schema_definition: Arc::new(logs_schema_definition),
            metrics_schema_definition: Arc::new(metrics_schema_definition),
        }
    }

    fn build_warp_filters(
        &self,
        out: SourceSender,
        acknowledgements: bool,
        config: &DatadogAgentConfig,
    ) -> crate::Result<BoxedFilter<(Response,)>> {
        let mut filters = (!config.disable_logs).then(|| {
            logs::build_warp_filter(
                acknowledgements,
                config.multiple_outputs,
                out.clone(),
                self.clone(),
            )
        });

        if !config.disable_traces {
            let trace_filter = traces::build_warp_filter(
                acknowledgements,
                config.multiple_outputs,
                out.clone(),
                self.clone(),
            );
            filters = filters
                .map(|f| f.or(trace_filter.clone()).unify().boxed())
                .or(Some(trace_filter));
        }

        if !config.disable_metrics {
            let metrics_filter = metrics::build_warp_filter(
                acknowledgements,
                config.multiple_outputs,
                out,
                self.clone(),
            );
            filters = filters
                .map(|f| f.or(metrics_filter.clone()).unify().boxed())
                .or(Some(metrics_filter));
        }

        filters.ok_or_else(|| "At least one of the supported data type shall be enabled".into())
    }

    pub(crate) fn decode(
        &self,
        header: &Option<String>,
        mut body: Bytes,
        path: &str,
    ) -> Result<Bytes, ErrorMessage> {
        if let Some(encodings) = header {
            for encoding in encodings.rsplit(',').map(str::trim) {
                body = match encoding {
                    "identity" => body,
                    "gzip" | "x-gzip" => {
                        let mut decoded = Vec::new();
                        MultiGzDecoder::new(body.reader())
                            .read_to_end(&mut decoded)
                            .map_err(|error| handle_decode_error(encoding, error))?;
                        decoded.into()
                    }
                    "deflate" | "x-deflate" => {
                        let mut decoded = Vec::new();
                        ZlibDecoder::new(body.reader())
                            .read_to_end(&mut decoded)
                            .map_err(|error| handle_decode_error(encoding, error))?;
                        decoded.into()
                    }
                    encoding => {
                        return Err(ErrorMessage::new(
                            StatusCode::UNSUPPORTED_MEDIA_TYPE,
                            format!("Unsupported encoding {}", encoding),
                        ))
                    }
                }
            }
        }
        emit!(HttpBytesReceived {
            byte_size: body.len(),
            http_path: path,
            protocol: self.protocol,
        });
        Ok(body)
    }
}

pub(crate) async fn handle_request(
    events: Result<Vec<Event>, ErrorMessage>,
    acknowledgements: bool,
    mut out: SourceSender,
    output: Option<&str>,
) -> Result<Response, Rejection> {
    match events {
        Ok(mut events) => {
            let receiver = BatchNotifier::maybe_apply_to(acknowledgements, &mut events);
            let count = events.len();

            if let Some(name) = output {
                out.send_batch_named(name, events).await
            } else {
                out.send_batch(events).await
            }
            .map_err(move |error: crate::source_sender::ClosedError| {
                emit!(StreamClosedError { error, count });
                warp::reject::custom(ApiError::ServerShutdown)
            })?;
            match receiver {
                None => Ok(warp::reply().into_response()),
                Some(receiver) => match receiver.await {
                    BatchStatus::Delivered => Ok(warp::reply().into_response()),
                    BatchStatus::Errored => Err(warp::reject::custom(ErrorMessage::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Error delivering contents to sink".into(),
                    ))),
                    BatchStatus::Rejected => Err(warp::reject::custom(ErrorMessage::new(
                        StatusCode::BAD_REQUEST,
                        "Contents failed to deliver to sink".into(),
                    ))),
                },
            }
        }
        Err(err) => Err(warp::reject::custom(err)),
    }
}

fn handle_decode_error(encoding: &str, error: impl std::error::Error) -> ErrorMessage {
    emit!(HttpDecompressError {
        encoding,
        error: &error
    });
    ErrorMessage::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        format!("Failed decompressing payload with {} decoder.", encoding),
    )
}

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LogMsg {
    pub message: Bytes,
    pub status: Bytes,
    #[serde(
        deserialize_with = "ts_milliseconds::deserialize",
        serialize_with = "ts_milliseconds::serialize"
    )]
    pub timestamp: DateTime<Utc>,
    pub hostname: Bytes,
    pub service: Bytes,
    pub ddsource: Bytes,
    pub ddtags: Bytes,
}
