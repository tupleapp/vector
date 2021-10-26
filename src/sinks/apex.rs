use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::Event,
    http::{Auth, HttpClient, MaybeAuth},
    internal_events::{HttpEventEncoded, HttpEventMissingMessage},
    sinks::util::{
        encoding::{EncodingConfig, EncodingConfiguration},
        http::{BatchedHttpSink, HttpSink, RequestConfig},
        BatchConfig, BatchSettings, Buffer, Compression, TowerRequestConfig, UriSerde,
    },
    tls::{TlsOptions, TlsSettings},
};
use flate2::write::GzEncoder;
use futures::{future, FutureExt, SinkExt};
use http::{
    header::{self, HeaderName, HeaderValue},
    Method, Request, StatusCode, Uri,
};
use hyper::Body;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::{ResultExt, Snafu};
use std::io::Write;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("{}: {}", source, name))]
    InvalidHeaderName {
        name: String,
        source: header::InvalidHeaderName,
    },
    #[snafu(display("{}: {}", source, value))]
    InvalidHeaderValue {
        value: String,
        source: header::InvalidHeaderValue,
    },
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ApexSinkConfig {
    pub uri: UriSerde,
    pub project_id: String,
    pub method: Option<HttpMethod>,
    pub auth: Option<Auth>,
    // Deprecated, moved to request.
    pub headers: Option<IndexMap<String, String>>,
    #[serde(default)]
    pub compression: Compression,
    pub encoding: EncodingConfig<Encoding>,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default)]
    pub request: RequestConfig,
    pub tls: Option<TlsOptions>,
}

#[cfg(test)]
fn default_config(e: Encoding) -> ApexSinkConfig {
    ApexSinkConfig {
        uri: Default::default(),
        project_id: Default::default(),
        method: Default::default(),
        auth: Default::default(),
        headers: Default::default(),
        compression: Default::default(),
        batch: Default::default(),
        encoding: e.into(),
        request: Default::default(),
        tls: Default::default(),
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
pub enum HttpMethod {
    #[derivative(Default)]
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Trace,
    Patch,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Text,
    Ndjson,
    Json,
}

inventory::submit! {
    SinkDescription::new::<ApexSinkConfig>("apex")
}

impl GenerateConfig for ApexSinkConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"uri = "https://10.22.212.22:9000/endpoint"
            encoding.codec = "json""#,
        )
        .unwrap()
    }
}

impl ApexSinkConfig {
    fn build_http_client(&self, cx: &SinkContext) -> crate::Result<HttpClient> {
        let tls = TlsSettings::from_options(&self.tls)?;
        Ok(HttpClient::new(tls, cx.proxy())?)
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "apex")]
impl SinkConfig for ApexSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let client = self.build_http_client(&cx)?;

        let healthcheck = match cx.healthcheck.uri.clone() {
            Some(healthcheck_uri) => {
                healthcheck(healthcheck_uri, self.auth.clone(), client.clone()).boxed()
            }
            None => future::ok(()).boxed(),
        };

        let mut config = ApexSinkConfig {
            auth: self.auth.choose_one(&self.uri.auth)?,
            uri: self.uri.with_default_parts(),
            ..self.clone()
        };

        config.request.add_old_option(config.headers.take());
        validate_headers(&config.request.headers, &config.auth)?;

        let batch = BatchSettings::default()
            .bytes(10_000_000)
            .timeout(1)
            .parse_config(config.batch)?;
        let request = config
            .request
            .tower
            .unwrap_with(&TowerRequestConfig::default());
        let sink = BatchedHttpSink::new(
            config,
            Buffer::new(batch.size, Compression::None),
            request,
            batch.timeout,
            client,
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal HTTP sink error.", %error));

        let sink = super::VectorSink::Sink(Box::new(sink));

        Ok((sink, healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn sink_type(&self) -> &'static str {
        "apex"
    }
}

#[async_trait::async_trait]
impl HttpSink for ApexSinkConfig {
    type Input = Vec<u8>;
    type Output = Vec<u8>;

    fn encode_event(&self, mut event: Event) -> Option<Self::Input> {
        self.encoding.apply_rules(&mut event);
        let event = event.into_log();

        let body = match &self.encoding.codec() {
            Encoding::Text => {
                if let Some(v) = event.get(crate::config::log_schema().message_key()) {
                    let mut b = v.to_string_lossy().into_bytes();
                    b.push(b'\n');
                    b
                } else {
                    emit!(&HttpEventMissingMessage);
                    return None;
                }
            }

            Encoding::Ndjson => {
                let mut b = serde_json::to_vec(&event)
                    .map_err(|error| panic!("Unable to encode into JSON: {}", error))
                    .ok()?;
                b.push(b'\n');
                b
            }

            Encoding::Json => {
                let mut b = serde_json::to_vec(&event)
                    .map_err(|error| panic!("Unable to encode into JSON: {}", error))
                    .ok()?;
                b.push(b',');
                b
            }
        };

        emit!(&HttpEventEncoded {
            byte_size: body.len(),
        });

        Some(body)
    }

    async fn build_request(&self, mut body: Self::Output) -> crate::Result<http::Request<Vec<u8>>> {
        let method = match &self.method.clone().unwrap_or(HttpMethod::Post) {
            HttpMethod::Get => Method::GET,
            HttpMethod::Head => Method::HEAD,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Delete => Method::DELETE,
            HttpMethod::Options => Method::OPTIONS,
            HttpMethod::Trace => Method::TRACE,
            HttpMethod::Patch => Method::PATCH,
        };
        let uri: Uri = self.uri.uri.clone();

        let ct = match self.encoding.codec() {
            Encoding::Text => "text/plain",
            Encoding::Ndjson => "application/x-ndjson",
            Encoding::Json => "application/json",
        };

        body.pop(); // remove trailing comma from last record
        let full_body_string = json!({
            "project_id": self.project_id,
            "events": [body]
        });

        debug!("{}", full_body_string.as_str().unwrap());

        body = serde_json::to_vec(&full_body_string).unwrap();

        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", ct);

        match self.compression {
            Compression::Gzip(level) => {
                builder = builder.header("Content-Encoding", "gzip");

                let mut w = GzEncoder::new(Vec::new(), level);
                w.write_all(&body).expect("Writing to Vec can't fail");
                body = w.finish().expect("Writing to Vec can't fail");
            }
            Compression::None => {}
        }

        for (header, value) in self.request.headers.iter() {
            builder = builder.header(header.as_str(), value.as_str());
        }

        let mut request = builder.body(body).unwrap();

        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        Ok(request)
    }
}

async fn healthcheck(uri: UriSerde, auth: Option<Auth>, client: HttpClient) -> crate::Result<()> {
    let auth = auth.choose_one(&uri.auth)?;
    let uri = uri.with_default_parts();
    let mut request = Request::head(&uri.uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(super::HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

fn validate_headers(map: &IndexMap<String, String>, auth: &Option<Auth>) -> crate::Result<()> {
    for (name, value) in map {
        if auth.is_some() && name.eq_ignore_ascii_case("Authorization") {
            return Err("Authorization header can not be used with defined auth options".into());
        }

        HeaderName::from_bytes(name.as_bytes()).with_context(|| InvalidHeaderName { name })?;
        HeaderValue::from_bytes(value.as_bytes()).with_context(|| InvalidHeaderValue { value })?;
    }

    Ok(())
}
