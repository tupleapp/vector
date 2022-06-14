use std::{
    fmt::Debug,
    io::{self, Write},
    num::NonZeroUsize,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use codecs::{encoding::Framer, CharacterDelimitedEncoder, JsonSerializer};
use futures::{stream::BoxStream, StreamExt};
use lookup::path;
use snafu::Snafu;
use tower::Service;
use vector_core::{
    buffers::Acker,
    config::{log_schema, LogSchema},
    event::{Event, EventFinalizers, Finalizable, Value},
    partition::Partitioner,
    sink::StreamSink,
    stream::{BatcherSettings, DriverResponse},
    ByteSizeOf,
};

use super::{config::MAX_PAYLOAD_BYTES, service::LogApiRequest};
use crate::{
    codecs::Encoder,
    config::SinkContext,
    sinks::util::{
        encoding::{Encoder as _, Transformer},
        request_builder::EncodeResult,
        Compression, Compressor, RequestBuilder, SinkBuilderExt,
    },
};
#[derive(Default)]
struct EventPartitioner;

impl Partitioner for EventPartitioner {
    type Item = Event;
    type Key = Option<Arc<str>>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        item.metadata().datadog_api_key()
    }
}

#[derive(Debug)]
pub struct LogSinkBuilder<S> {
    encoding: JsonEncoding,
    service: S,
    context: SinkContext,
    batch_settings: BatcherSettings,
    compression: Option<Compression>,
    default_api_key: Arc<str>,
}

impl<S> LogSinkBuilder<S> {
    pub fn new(
        transformer: Transformer,
        service: S,
        context: SinkContext,
        default_api_key: Arc<str>,
        batch_settings: BatcherSettings,
    ) -> Self {
        Self {
            encoding: JsonEncoding::new(transformer),
            service,
            context,
            default_api_key,
            batch_settings,
            compression: None,
        }
    }

    pub const fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn build(self) -> LogSink<S> {
        LogSink {
            default_api_key: self.default_api_key,
            encoding: self.encoding,
            schema_enabled: false,
            acker: self.context.acker(),
            service: self.service,
            batch_settings: self.batch_settings,
            compression: self.compression.unwrap_or_default(),
        }
    }
}

pub struct LogSink<S> {
    /// The default Datadog API key to use
    ///
    /// In some instances an `Event` will come in on the stream with an
    /// associated API key. That API key is the one it'll get batched up by but
    /// otherwise we will see `Event` instances with no associated key. In that
    /// case we batch them by this default.
    default_api_key: Arc<str>,
    /// The ack system for this sink to vector's buffer mechanism
    acker: Acker,
    /// The API service
    service: S,
    /// The encoding of payloads
    encoding: JsonEncoding,
    /// Whether to enable schema support.
    schema_enabled: bool,
    /// The compression technique to use when building the request body
    compression: Compression,
    /// Batch settings: timeout, max events, max bytes, etc.
    batch_settings: BatcherSettings,
}

/// Customized encoding specific to the Datadog Logs sink, as the logs API only accepts JSON encoded
/// log lines, and requires some specific normalization of certain event fields.
#[derive(Clone, Debug)]
pub struct JsonEncoding {
    log_schema: &'static LogSchema,
    encoder: (Transformer, Encoder<Framer>),
}

impl JsonEncoding {
    pub fn new(transformer: Transformer) -> Self {
        Self {
            log_schema: log_schema(),
            encoder: (
                transformer,
                Encoder::<Framer>::new(
                    CharacterDelimitedEncoder::new(b',').into(),
                    JsonSerializer::new().into(),
                ),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SemanticJsonEncoding {
    log_schema: &'static LogSchema,
    encoder: (Transformer, Encoder<Framer>),
}

impl crate::sinks::util::encoding::Encoder<Vec<Event>> for JsonEncoding {
    fn encode_input(&self, mut input: Vec<Event>, writer: &mut dyn io::Write) -> io::Result<usize> {
        for event in input.iter_mut() {
            let log = event.as_mut_log();
            log.rename_key(self.log_schema.message_key(), path!("message"));
            log.rename_key(self.log_schema.host_key(), path!("host"));
            if let Some(Value::Timestamp(ts)) = log.remove(self.log_schema.timestamp_key()) {
                log.insert(path!("timestamp"), Value::Integer(ts.timestamp_millis()));
            }
        }

        self.encoder.encode_input(input, writer)
    }
}

impl crate::sinks::util::encoding::Encoder<Vec<Event>> for SemanticJsonEncoding {
    fn encode_input(&self, mut input: Vec<Event>, writer: &mut dyn io::Write) -> io::Result<usize> {
        for event in input.iter_mut() {
            let log = event.as_mut_log();

            // message
            let message_key = log
                .find_key_by_meaning("message")
                .expect("enforced by schema");
            log.rename_key(message_key.as_str(), path!("message"));

            // host
            let host_key = log
                .find_key_by_meaning("host")
                .unwrap_or_else(|| self.log_schema.host_key().into());
            log.rename_key(host_key.as_str(), path!("host"));

            // timestamp
            let ts = log
                .get_by_meaning("timestamp")
                .expect("enforced by schema")
                .as_timestamp_unwrap();
            let ms = ts.timestamp_millis();
            log.insert(path!("timestamp"), Value::Integer(ms));
        }

        self.encoder.encode_input(input, writer)
    }
}

#[derive(Debug, Snafu)]
pub enum RequestBuildError {
    #[snafu(display("Encoded payload is greater than the max limit."))]
    PayloadTooBig,
    #[snafu(display("Failed to build payload with error: {}", error))]
    Io { error: std::io::Error },
}

impl From<io::Error> for RequestBuildError {
    fn from(error: io::Error) -> RequestBuildError {
        RequestBuildError::Io { error }
    }
}

struct LogRequestBuilder {
    default_api_key: Arc<str>,
    encoding: JsonEncoding,
    compression: Compression,
}

impl RequestBuilder<(Option<Arc<str>>, Vec<Event>)> for LogRequestBuilder {
    type Metadata = (Arc<str>, usize, EventFinalizers, usize);
    type Events = Vec<Event>;
    type Encoder = JsonEncoding;
    type Payload = Bytes;
    type Request = LogApiRequest;
    type Error = RequestBuildError;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoding
    }

    fn split_input(&self, input: (Option<Arc<str>>, Vec<Event>)) -> (Self::Metadata, Self::Events) {
        let (api_key, mut events) = input;
        let events_len = events.len();
        let finalizers = events.take_finalizers();
        let events_byte_size = events.size_of();

        let api_key = api_key.unwrap_or_else(|| Arc::clone(&self.default_api_key));
        ((api_key, events_len, finalizers, events_byte_size), events)
    }

    fn encode_events(
        &self,
        events: Self::Events,
    ) -> Result<EncodeResult<Self::Payload>, Self::Error> {
        // We need to first serialize the payload separately so that we can figure out how big it is
        // before compression.  The Datadog Logs API has a limit on uncompressed data, so we can't
        // use the default implementation of this method.
        //
        // TODO: We should probably make `build_request` fallible itself, because then this override of `encode_events`
        // wouldn't even need to exist, and we could handle it in `build_request` which is required by all implementors.
        //
        // On the flip side, it would mean that we'd potentially be compressing payloads that we would inevitably end up
        // rejecting anyways, which is meh. This might be a signal that the true "right" fix is to actually switch this
        // sink to incremental encoding and simply put up with suboptimal batch sizes if we need to end up splitting due
        // to (un)compressed size limitations.
        let mut buf = Vec::new();
        let uncompressed_size = self.encoder().encode_input(events, &mut buf)?;
        if uncompressed_size > MAX_PAYLOAD_BYTES {
            return Err(RequestBuildError::PayloadTooBig);
        }

        // Now just compress it like normal.
        let mut compressor = Compressor::from(self.compression);
        let _ = compressor.write_all(&buf)?;
        let bytes = compressor.into_inner().freeze();

        if self.compression.is_compressed() {
            Ok(EncodeResult::compressed(bytes, uncompressed_size))
        } else {
            Ok(EncodeResult::uncompressed(bytes))
        }
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let (api_key, batch_size, finalizers, events_byte_size) = metadata;
        let uncompressed_size = payload.uncompressed_byte_size;
        LogApiRequest {
            batch_size,
            api_key,
            compression: self.compression,
            body: payload.into_payload(),
            finalizers,
            events_byte_size,
            uncompressed_size,
        }
    }
}

struct SemanticLogRequestBuilder {
    default_api_key: Arc<str>,
    encoding: SemanticJsonEncoding,
    compression: Compression,
}

impl RequestBuilder<(Option<Arc<str>>, Vec<Event>)> for SemanticLogRequestBuilder {
    type Metadata = (Arc<str>, usize, EventFinalizers, usize);
    type Events = Vec<Event>;
    type Encoder = SemanticJsonEncoding;
    type Payload = Bytes;
    type Request = LogApiRequest;
    type Error = RequestBuildError;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoding
    }

    fn split_input(&self, input: (Option<Arc<str>>, Vec<Event>)) -> (Self::Metadata, Self::Events) {
        let (api_key, mut events) = input;
        let events_len = events.len();
        let finalizers = events.take_finalizers();
        let events_byte_size = events.size_of();

        let api_key = api_key.unwrap_or_else(|| Arc::clone(&self.default_api_key));
        ((api_key, events_len, finalizers, events_byte_size), events)
    }

    fn encode_events(
        &self,
        events: Self::Events,
    ) -> Result<EncodeResult<Self::Payload>, Self::Error> {
        // We need to first serialize the payload separately so that we can figure out how big it is
        // before compression.  The Datadog Logs API has a limit on uncompressed data, so we can't
        // use the default implementation of this method.
        //
        // TODO: We should probably make `build_request` fallible itself, because then this override of `encode_events`
        // wouldn't even need to exist, and we could handle it in `build_request` which is required by all implementors.
        //
        // On the flip side, it would mean that we'd potentially be compressing payloads that we would inevitably end up
        // rejecting anyways, which is meh. This might be a signal that the true "right" fix is to actually switch this
        // sink to incremental encoding and simply put up with suboptimal batch sizes if we need to end up splitting due
        // to (un)compressed size limitations.
        let mut buf = Vec::new();
        let uncompressed_size = self.encoder().encode_input(events, &mut buf)?;
        if uncompressed_size > MAX_PAYLOAD_BYTES {
            return Err(RequestBuildError::PayloadTooBig);
        }

        // Now just compress it like normal.
        let mut compressor = Compressor::from(self.compression);
        let _ = compressor.write_all(&buf)?;
        let bytes = compressor.into_inner().freeze();

        if self.compression.is_compressed() {
            Ok(EncodeResult::compressed(bytes, uncompressed_size))
        } else {
            Ok(EncodeResult::uncompressed(bytes))
        }
    }

    fn build_request(
        &self,
        metadata: Self::Metadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let (api_key, batch_size, finalizers, events_byte_size) = metadata;
        let uncompressed_size = payload.uncompressed_byte_size;
        LogApiRequest {
            batch_size,
            api_key,
            compression: self.compression,
            body: payload.into_payload(),
            finalizers,
            events_byte_size,
            uncompressed_size,
        }
    }
}

impl<S> LogSink<S>
where
    S: Service<LogApiRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let default_api_key = Arc::clone(&self.default_api_key);

        let partitioner = EventPartitioner::default();

        let builder_limit = NonZeroUsize::new(64);
        if self.schema_enabled {
            let sink = input
                .batched_partitioned(partitioner, self.batch_settings)
                .request_builder(
                    builder_limit,
                    SemanticLogRequestBuilder {
                        default_api_key,
                        encoding: SemanticJsonEncoding {
                            log_schema: self.encoding.log_schema,
                            encoder: self.encoding.encoder,
                        },
                        compression: self.compression,
                    },
                )
                .filter_map(|request| async move {
                    match request {
                        Err(e) => {
                            error!("Failed to build Datadog Logs request: {:?}.", e);
                            None
                        }
                        Ok(req) => Some(req),
                    }
                })
                .into_driver(self.service, self.acker);

            sink.run().await
        } else {
            let sink = input
                .batched_partitioned(partitioner, self.batch_settings)
                .request_builder(
                    builder_limit,
                    LogRequestBuilder {
                        default_api_key,
                        encoding: self.encoding,
                        compression: self.compression,
                    },
                )
                .filter_map(|request| async move {
                    match request {
                        Err(e) => {
                            error!("Failed to build Datadog Logs request: {:?}.", e);
                            None
                        }
                        Ok(req) => Some(req),
                    }
                })
                .into_driver(self.service, self.acker);

            sink.run().await
        }
    }
}

#[async_trait]
impl<S> StreamSink<Event> for LogSink<S>
where
    S: Service<LogApiRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
