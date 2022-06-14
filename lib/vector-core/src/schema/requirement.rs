use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use lookup::LookupBuf;
use value::Kind;

use super::Definition;

/// The input schema for a given component.
///
/// This schema defines the (semantic) fields a component expects to receive from its input
/// components.
#[derive(Debug, Clone, PartialEq)]
pub struct Requirement {
    /// Semantic meanings confingured for this requirement.
    meaning: BTreeMap<&'static str, SemanticMeaning>,
}

/// The semantic meaning of an event.
#[derive(Debug, Clone, PartialEq)]
struct SemanticMeaning {
    /// The type required by this semantic meaning.
    kind: Kind,

    /// Whether the meaning is optional.
    ///
    /// If a meaning is optional, the sink must not error when the meaning is not defined in the
    /// provided `Definition`, but it *must* error if it is defined, but its type does not meet the
    /// requirement.
    optional: bool,
}

impl Requirement {
    /// Create a new empty schema.
    ///
    /// An empty schema is the most "open" schema, in that there are no restrictions.
    pub fn empty() -> Self {
        Self {
            meaning: BTreeMap::default(),
        }
    }

    /// Check if the requirement is "empty", meaning:
    ///
    /// 1. There are no required fields defined.
    /// 2. The unknown fields are set to "any".
    /// 3. There are no required meanings defined.
    pub fn is_empty(&self) -> bool {
        self.meaning.is_empty()
    }

    /// Add a restriction to the schema.
    #[must_use]
    pub fn required_meaning(mut self, meaning: &'static str, kind: Kind) -> Self {
        self.insert_meaning(meaning, kind, false);
        self
    }

    /// Add an optional restriction to the schema.
    ///
    /// This differs from `required_meaning` in that it is valid for the event to not have the
    /// specified meaning defined, but invalid for that meaning to be defined, but its [`Kind`] not
    /// matching the configured expectation.
    #[must_use]
    pub fn optional_meaning(mut self, meaning: &'static str, kind: Kind) -> Self {
        self.insert_meaning(meaning, kind, true);
        self
    }

    fn insert_meaning(&mut self, identifier: &'static str, kind: Kind, optional: bool) {
        let meaning = SemanticMeaning { kind, optional };
        self.meaning.insert(identifier, meaning);
    }

    /// Validate the provided [`Definition`] against the current requirement.
    ///
    /// # Errors
    ///
    /// Returns a list of errors if validation fails.
    pub fn validate(&self, definition: &Definition) -> Result<(), ValidationErrors> {
        let mut errors = vec![];

        for (identifier, req_meaning) in &self.meaning {
            // Check if we're dealing with an invalid meaning, meaning the definition has a single
            // meaning identifier pointing to multiple paths.
            if let Some(paths) = definition.invalid_meaning(identifier).cloned() {
                errors.push(ValidationError::MeaningDuplicate { identifier, paths });
                continue;
            }

            let maybe_meaning_path = definition.meanings().find_map(|(def_id, path)| {
                if def_id == identifier {
                    Some(path)
                } else {
                    None
                }
            });

            match maybe_meaning_path {
                Some(path) => {
                    // Get the kind at the path for the given semantic meaning.
                    // If no kind is found, we set the kind to "any", since we
                    // lack any further information.
                    let definition_kind = definition
                        .collection()
                        .find_known_at_path(&mut path.to_lookup())
                        .ok()
                        .flatten()
                        .map_or_else(Kind::any, Cow::into_owned);

                    if !req_meaning.kind.is_superset(&definition_kind) {
                        // We found a field matching the defined semantic
                        // meaning, but its kind does not match the expected
                        // kind, so we can't use it in the sink.
                        errors.push(ValidationError::MeaningKind {
                            identifier,
                            want: req_meaning.kind.clone(),
                            got: definition_kind,
                        });
                    }
                }
                None if !req_meaning.optional => {
                    errors.push(ValidationError::MeaningMissing { identifier });
                }
                _ => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors(errors))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    pub fn is_meaning_missing(&self) -> bool {
        self.0.iter().any(ValidationError::is_meaning_missing)
    }

    pub fn is_meaning_kind(&self) -> bool {
        self.0.iter().any(ValidationError::is_meaning_kind)
    }

    pub fn errors(&self) -> &[ValidationError] {
        &self.0
    }
}

impl std::error::Error for ValidationErrors {
    fn source(&self) -> Option<&(dyn snafu::Error + 'static)> {
        Some(&self.0[0])
    }
}

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.0 {
            error.fmt(f)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum ValidationError {
    /// A required semantic meaning is missing.
    MeaningMissing { identifier: &'static str },

    /// A semantic meaning has an invalid `[Kind]`.
    MeaningKind {
        identifier: &'static str,
        want: Kind,
        got: Kind,
    },

    /// A semantic meaning is pointing to multiple paths.
    MeaningDuplicate {
        identifier: &'static str,
        paths: BTreeSet<LookupBuf>,
    },
}

impl ValidationError {
    pub fn is_meaning_missing(&self) -> bool {
        matches!(self, Self::MeaningMissing { .. })
    }

    pub fn is_meaning_kind(&self) -> bool {
        matches!(self, Self::MeaningKind { .. })
    }

    pub fn is_meaning_duplicate(&self) -> bool {
        matches!(self, Self::MeaningDuplicate { .. })
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MeaningMissing { identifier } => {
                write!(f, "missing semantic meaning: {}", identifier)
            }
            Self::MeaningKind {
                identifier,
                want,
                got,
            } => write!(
                f,
                "invalid semantic meaning: {} (expected {}, got {})",
                identifier, want, got
            ),
            Self::MeaningDuplicate { identifier, paths } => write!(
                f,
                "semantic meaning {} pointing to multiple fields: {}",
                identifier,
                paths
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_validate() {
        struct TestCase {
            requirement: Requirement,
            definition: Definition,
            errors: Vec<ValidationError>,
        }

        for (
            title,
            TestCase {
                requirement,
                definition,
                errors,
            },
        ) in HashMap::from([
            (
                "empty",
                TestCase {
                    requirement: Requirement::empty(),
                    definition: Definition::empty(),
                    errors: vec![],
                },
            ),
            (
                "missing required meaning",
                TestCase {
                    requirement: Requirement::empty().required_meaning("foo", Kind::any()),
                    definition: Definition::empty(),
                    errors: vec![ValidationError::MeaningMissing { identifier: "foo" }],
                },
            ),
            (
                "missing required meanings",
                TestCase {
                    requirement: Requirement::empty()
                        .required_meaning("foo", Kind::any())
                        .required_meaning("bar", Kind::any()),
                    definition: Definition::empty(),
                    errors: vec![
                        ValidationError::MeaningMissing { identifier: "bar" },
                        ValidationError::MeaningMissing { identifier: "foo" },
                    ],
                },
            ),
            (
                "missing optional meaning",
                TestCase {
                    requirement: Requirement::empty().optional_meaning("foo", Kind::any()),
                    definition: Definition::empty(),
                    errors: vec![],
                },
            ),
            (
                "missing mixed meanings",
                TestCase {
                    requirement: Requirement::empty()
                        .optional_meaning("foo", Kind::any())
                        .required_meaning("bar", Kind::any()),
                    definition: Definition::empty(),
                    errors: vec![ValidationError::MeaningMissing { identifier: "bar" }],
                },
            ),
            (
                "invalid required meaning kind",
                TestCase {
                    requirement: Requirement::empty().required_meaning("foo", Kind::boolean()),
                    definition: Definition::empty().with_field("foo", Kind::integer(), Some("foo")),
                    errors: vec![ValidationError::MeaningKind {
                        identifier: "foo",
                        want: Kind::boolean(),
                        got: Kind::integer(),
                    }],
                },
            ),
            (
                "invalid optional meaning kind",
                TestCase {
                    requirement: Requirement::empty().optional_meaning("foo", Kind::boolean()),
                    definition: Definition::empty().with_field("foo", Kind::integer(), Some("foo")),
                    errors: vec![ValidationError::MeaningKind {
                        identifier: "foo",
                        want: Kind::boolean(),
                        got: Kind::integer(),
                    }],
                },
            ),
            (
                "duplicate meaning pointers",
                TestCase {
                    requirement: Requirement::empty().optional_meaning("foo", Kind::boolean()),
                    definition: Definition::empty()
                        .with_field("foo", Kind::integer(), Some("foo"))
                        .merge(Definition::empty().with_field("bar", Kind::boolean(), Some("foo"))),
                    errors: vec![ValidationError::MeaningDuplicate {
                        identifier: "foo",
                        paths: BTreeSet::from(["foo".into(), "bar".into()]),
                    }],
                },
            ),
        ]) {
            let got = requirement.validate(&definition);
            let want = if errors.is_empty() {
                Ok(())
            } else {
                Err(ValidationErrors(errors))
            };

            assert_eq!(got, want, "{}", title);
        }
    }
}
