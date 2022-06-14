use crate::{
    config::{
        AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext, SinkDescription,
    },
    event::Event,
    http::{Auth, HttpClient},
    sinks::util::{
        encoding::Transformer,
        http::{BatchedHttpSink, HttpEventEncoder, HttpSink},
        BatchConfig, BoxedRawValue, JsonArrayBuffer, SinkBatchSettings, TowerRequestConfig,
        UriSerde,
    },
};
use bytes::Bytes;
use futures::{FutureExt, SinkExt};
use http::{Request, StatusCode, Uri};
use hyper::Body;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::Value;

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ApexSinkConfig {
    pub uri: UriSerde,
    pub project_id: String,
    pub auth: Option<Auth>,
    #[serde(default)]
    pub batch: BatchConfig<ApexDefaultBatchSettings>,
    #[serde(
        default,
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    encoding: Transformer,
    #[serde(default)]
    request: TowerRequestConfig,
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ApexDefaultBatchSettings;

impl SinkBatchSettings for ApexDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(100_000);
    const TIMEOUT_SECS: f64 = 5.0;
}

inventory::submit! {
    SinkDescription::new::<ApexSinkConfig>("apex")
}

impl GenerateConfig for ApexSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"uri = "https://10.22.212.22:9000/endpoint""#).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "apex")]
impl SinkConfig for ApexSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let request_settings = self.request.unwrap_with(&TowerRequestConfig::default());
        let batch_settings = self.batch.into_batch_settings()?;
        let buffer = JsonArrayBuffer::new(batch_settings.size);
        let client = HttpClient::new(None, cx.proxy())?;

        let sink = BatchedHttpSink::new(
            self.clone(),
            buffer,
            request_settings,
            batch_settings.timeout,
            client.clone(),
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal apex sink error.", %error));

        let healthcheck = healthcheck(self.clone(), client).boxed();

        Ok((super::VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "apex"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

pub struct ApexEventEncoder {
    transformer: Transformer,
}

impl HttpEventEncoder<serde_json::Value> for ApexEventEncoder {
    fn encode_event(&mut self, mut event: Event) -> Option<serde_json::Value> {
        self.transformer.transform(&mut event);
        let event = event.into_log();
        let body = json!(&event);

        Some(body)
    }
}

#[async_trait::async_trait]
impl HttpSink for ApexSinkConfig {
    type Input = Value;
    type Output = Vec<BoxedRawValue>;
    type Encoder = ApexEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        ApexEventEncoder {
            transformer: self.encoding.clone(),
        }
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<http::Request<Bytes>> {
        let uri: Uri = self.uri.uri.clone();

        let full_body_string = json!({
            "project_id": self.project_id,
            "events": events
        });

        let body = crate::serde::json::to_bytes(&full_body_string)
            .unwrap()
            .freeze();
        let builder = Request::post(uri).header("Content-Type", "application/json");
        let mut request = builder.body(body).unwrap();

        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        Ok(request)
    }
}

async fn healthcheck(config: ApexSinkConfig, client: HttpClient) -> crate::Result<()> {
    let uri = config.uri.with_default_parts();
    let mut request = Request::head(&uri.uri).body(Body::empty()).unwrap();

    if let Some(auth) = config.auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(super::HealthcheckError::UnexpectedStatus { status }.into()),
    }
}
