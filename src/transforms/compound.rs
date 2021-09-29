use crate::{
    config::{DataType, GenerateConfig, TransformConfig, TransformContext, TransformDescription},
    event::Event,
    transforms::{TaskTransform, Transform},
};
use futures::{stream, Stream, StreamExt};
use serde::{self, Deserialize, Serialize};
use std::pin::Pin;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompoundConfig {
    steps: Vec<Box<dyn TransformConfig>>,
}

inventory::submit! {
    TransformDescription::new::<CompoundConfig>("compound")
}

impl GenerateConfig for CompoundConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self { steps: Vec::new() }).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "compound")]
impl TransformConfig for CompoundConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        Compound::new(self.clone(), context)
            .await
            .map(Transform::task)
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn output_type(&self) -> DataType {
        DataType::Any
    }

    fn transform_type(&self) -> &'static str {
        "compound"
    }
}

pub struct Compound {
    transforms: Vec<Transform>,
}

impl Compound {
    pub async fn new(config: CompoundConfig, context: &TransformContext) -> crate::Result<Self> {
        let steps = &config.steps;
        let mut transforms = vec![];
        if !steps.is_empty() {
            for transform_config in steps.iter() {
                let transform = transform_config.build(context).await?;
                transforms.push(transform);
            }
            Ok(Self { transforms })
        } else {
            Err("must specify at least one transform".into())
        }
    }
}

impl TaskTransform for Compound {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let mut task = task;
        for t in self.transforms {
            match t {
                Transform::Task(t) => {
                    task = t.transform(task);
                }
                Transform::Function(mut t) => {
                    task = Box::pin(task.flat_map(move |v| {
                        let mut output = Vec::<Event>::new();
                        t.transform(&mut output, v);
                        stream::iter(output)
                    }));
                }
            }
        }
        task
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::CompoundConfig>();
    }

    #[test]
    fn can_serialize_nested_transforms() {
        // We need to serialize the config to check if a config has
        // changed when reloading.
        let config = toml::from_str::<CompoundConfig>(
            r#"
            [[steps]]
            type = "mock"
            suffix = "step1"

            [[steps]]
            type = "mock"
            id = "foo"
            suffix = "step1"
        "#,
        )
        .unwrap()
        .expand()
        .unwrap()
        .unwrap();

        assert_eq!(
            serde_json::to_string(&config).unwrap(),
            r#"[{"0":{"type":"mock"},"foo":{"type":"mock"}},"Serial"]"#
        );
    }
}
