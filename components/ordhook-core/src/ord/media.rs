use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use anyhow::{anyhow, Error};

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum Media {
    Audio,
    Code(Language),
    Font,
    Iframe,
    Image,
    Markdown,
    Model,
    Pdf,
    Text,
    Unknown,
    Video,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum Language {
    Css,
    JavaScript,
    Json,
    Python,
    Yaml,
}

impl Display for Language {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Css => "css",
                Self::JavaScript => "javascript",
                Self::Json => "json",
                Self::Python => "python",
                Self::Yaml => "yaml",
            }
        )
    }
}

impl Media {
    #[rustfmt::skip]
    const TABLE: &'static [(&'static str, Media, &'static [&'static str])] = &[
      ("application/cbor",            Media::Unknown,                    &["cbor"]),
      ("application/json",            Media::Code(Language::Json),       &["json"]),
      ("application/octet-stream",    Media::Unknown,                    &["bin"]),
      ("application/pdf",             Media::Pdf,                        &["pdf"]),
      ("application/pgp-signature",   Media::Text,                       &["asc"]),
      ("application/protobuf",        Media::Unknown,                    &["binpb"]),
      ("application/x-javascript",    Media::Code(Language::JavaScript), &[]),
      ("application/yaml",            Media::Code(Language::Yaml),       &["yaml", "yml"]),
      ("audio/flac",                  Media::Audio,                      &["flac"]),
      ("audio/mpeg",                  Media::Audio,                      &["mp3"]),
      ("audio/wav",                   Media::Audio,                      &["wav"]),
      ("font/otf",                    Media::Font,                       &["otf"]),
      ("font/ttf",                    Media::Font,                       &["ttf"]),
      ("font/woff",                   Media::Font,                       &["woff"]),
      ("font/woff2",                  Media::Font,                       &["woff2"]),
      ("image/apng",                  Media::Image,                      &["apng"]),
      ("image/avif",                  Media::Image,                      &[]),
      ("image/gif",                   Media::Image,                      &["gif"]),
      ("image/jpeg",                  Media::Image,                      &["jpg", "jpeg"]),
      ("image/png",                   Media::Image,                      &["png"]),
      ("image/svg+xml",               Media::Iframe,                     &["svg"]),
      ("image/webp",                  Media::Image,                      &["webp"]),
      ("model/gltf+json",             Media::Model,                      &["gltf"]),
      ("model/gltf-binary",           Media::Model,                      &["glb"]),
      ("model/stl",                   Media::Unknown,                    &["stl"]),
      ("text/css",                    Media::Code(Language::Css),        &["css"]),
      ("text/html",                   Media::Iframe,                     &[]),
      ("text/html;charset=utf-8",     Media::Iframe,                     &["html"]),
      ("text/javascript",             Media::Code(Language::JavaScript), &["js"]),
      ("text/markdown",               Media::Markdown,                   &[]),
      ("text/markdown;charset=utf-8", Media::Markdown,                   &["md"]),
      ("text/plain",                  Media::Text,                       &[]),
      ("text/plain;charset=utf-8",    Media::Text,                       &["txt"]),
      ("text/x-python",               Media::Code(Language::Python),     &["py"]),
      ("video/mp4",                   Media::Video,                      &["mp4"]),
      ("video/webm",                  Media::Video,                      &["webm"]),
    ];
}

impl FromStr for Media {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for entry in Self::TABLE {
            if entry.0 == s {
                return Ok(entry.1);
            }
        }

        Err(anyhow!("unknown content type: {s}"))
    }
}
