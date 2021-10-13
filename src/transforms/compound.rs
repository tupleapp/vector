use crate::{
    config::{DataType, GenerateConfig, TransformConfig, TransformContext, TransformDescription},
    event::Event,
    internal_events::{CompoundErrorEvents, CompoundTypeMismatchEventDropped},
    transforms::{TaskTransform, Transform},
};
use futures::{stream, Stream, StreamExt};
use serde::{self, Deserialize, Serialize};
use serde_json::json;
use std::{convert::TryInto, future::ready, pin::Pin};

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

impl CompoundConfig {
    fn consistent_types(&self) -> bool {
        let mut pairs = self.steps.windows(2).map(|items| match items {
            [a, b] => (a.output_type(), b.input_type()),
            _ => unreachable!(),
        });

        !pairs.any(|pair| {
            matches!(
                pair,
                (DataType::Log, DataType::Metric) | (DataType::Metric, DataType::Log)
            )
        })
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "compound")]
impl TransformConfig for CompoundConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        if !self.consistent_types() {
            Err("Inconsistent type in a compound transform".into())
        } else {
            Compound::new(self.clone(), context)
                .await
                .map(Transform::task)
        }
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
    transforms: Vec<(Transform, DataType)>,
}

impl Compound {
    pub async fn new(config: CompoundConfig, context: &TransformContext) -> crate::Result<Self> {
        let steps = &config.steps;
        let mut transforms = vec![];
        if !steps.is_empty() {
            for transform_config in steps.iter() {
                let transform = transform_config.build(context).await?;
                transforms.push((transform, transform_config.input_type()));
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
                (Transform::Task(t), input_type) => {
                    task = t.transform(type_filter(task, input_type));
                }
                (Transform::Function(mut t), input_type) => {
                    task = Box::pin(type_filter(task, input_type).flat_map(move |v| {
                        let mut output = Vec::<Event>::new();
                        t.transform(&mut output, v);
                        stream::iter(output)
                    }));
                }
                (Transform::FallibleFunction(mut t), input_type) => {
                    task = Box::pin(type_filter(task, input_type).flat_map(move |v| {
                        let mut output = Vec::<Event>::new();
                        let mut errors = Vec::<Event>::new();
                        t.transform(&mut output, &mut errors, v);
                        emit!(&CompoundErrorEvents { count: errors.len()});
                        errors.into_iter().for_each(|e| {
                            let event: serde_json::Value = e.try_into().unwrap_or_else(|_| json!("unable to render event"));
                            warn!(
                                message = "A faillible function failed to process an event within a compound transform.",
                                %event
                            )
                        });
                        stream::iter(output)
                    }));
                }
            }
        }
        task
    }
}

fn type_filter(
    task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    data_type: DataType,
) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
    Box::pin(task.filter(move |e| {
        if match data_type {
            DataType::Any => true,
            DataType::Log => matches!(e, Event::Log(_)),
            DataType::Metric => matches!(e, Event::Metric(_)),
        } {
            return ready(true);
        }
        emit!(&CompoundTypeMismatchEventDropped {});
        ready(false)
    }))
}

#[cfg(test)]
mod test {
    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::CompoundConfig>();
    }
}
