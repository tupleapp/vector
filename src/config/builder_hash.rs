#[cfg(feature = "api")]
use super::api;
use super::{
    builder::ConfigBuilder, provider, ComponentKey, EnrichmentTableOuter, HealthcheckOptions,
    SinkOuter, SourceOuter, TestDefinition, TransformOuter,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::btree_map::BTreeMap;
use vector_core::config::GlobalOptions;

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct ConfigBuilderHash {
    #[serde(flatten)]
    pub global: GlobalOptions,
    #[cfg(feature = "api")]
    #[serde(default)]
    pub api: api::Options,
    #[serde(default)]
    pub healthchecks: HealthcheckOptions,
    #[serde(default)]
    pub enrichment_tables: BTreeMap<ComponentKey, EnrichmentTableOuter>,
    #[serde(default)]
    pub sources: BTreeMap<ComponentKey, SourceOuter>,
    #[serde(default)]
    pub sinks: BTreeMap<ComponentKey, SinkOuter<String>>,
    #[serde(default)]
    pub transforms: BTreeMap<ComponentKey, TransformOuter<String>>,
    #[serde(default)]
    pub tests: Vec<TestDefinition>,
    pub provider: Option<Box<dyn provider::ProviderConfig>>,
}

impl From<ConfigBuilder> for ConfigBuilderHash {
    fn from(cb: ConfigBuilder) -> Self {
        Self {
            global: cb.global,
            #[cfg(feature = "api")]
            api: cb.api,
            healthchecks: cb.healthchecks,
            enrichment_tables: cb.enrichment_tables.into_iter().collect(),
            sources: cb.sources.into_iter().collect(),
            sinks: cb.sinks.into_iter().collect(),
            transforms: cb.transforms.into_iter().collect(),
            tests: cb.tests,
            provider: cb.provider,
        }
    }
}

impl ConfigBuilderHash {
    /// SHA256 hexidecimal representation of a config builder. This is generated by serializing
    /// an order-stable JSON of the config builder and feeding its bytes into a SHA256 hasher.
    pub fn sha256_hex(&self) -> String {
        let json = serde_json::to_string(self).expect("should serialize ConfigBuilder to JSON");
        let output = Sha256::digest(json.as_bytes());

        hex::encode(output)
    }
}
