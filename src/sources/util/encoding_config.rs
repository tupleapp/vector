use vector_config::configurable_component;

/// Character set encoding.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EncodingConfig {
    /// Encoding of the source messages.
    ///
    /// Takes one of the encoding [label strings](https://encoding.spec.whatwg.org/#concept-encoding-get) defined as
    /// part of the [Encoding Standard](https://encoding.spec.whatwg.org/).
    ///
    /// When set, the messages are transcoded from the specified encoding to UTF-8, which is the encoding that Vector
    /// assumes internally for string-like data. You should enable this transcoding operation if you need your data to
    /// be in UTF-8 for further processing. At the time of transcoding, any malformed sequences (that can’t be mapped to
    /// UTF-8) will be replaced with the Unicode [REPLACEMENT
    /// CHARACTER](https://en.wikipedia.org/wiki/Specials_(Unicode_block)#Replacement_character) and warnings will be
    /// logged.
    pub charset: &'static encoding_rs::Encoding,
}
