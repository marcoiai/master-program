use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SceneLayerType {
    MediaEmbed,
    Effect,
    Artwork,
    DataPanel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneLayerSource {
    pub kind: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneLayerPresentation {
    pub visible: bool,
    pub slot: String,
    pub motion: String,
    pub profile: String,
    pub z_index: i32,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub style: Value,
}

impl Default for SceneLayerPresentation {
    fn default() -> Self {
        Self {
            visible: true,
            slot: "center-wall".to_string(),
            motion: "drop_from_top".to_string(),
            profile: "pirate_broadcast".to_string(),
            z_index: 40,
            style: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneLayer {
    pub layer_id: String,
    #[serde(rename = "type")]
    pub layer_type: SceneLayerType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SceneLayerSource>,
    pub presentation: SceneLayerPresentation,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneLayerPatch {
    pub layer_id: Option<String>,
    #[serde(default)]
    pub visible: Option<bool>,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub motion: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub z_index: Option<i32>,
    #[serde(default)]
    pub source: Option<SceneLayerSource>,
}

pub fn normalize_youtube_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("media_url_required".to_string());
    }
    if trimmed.contains("youtube.com/embed/") {
        return Ok(trimmed.to_string());
    }
    if let Some(video_id) = trimmed.split("youtu.be/").nth(1) {
        let id = video_id.split(['?', '&', '/']).next().unwrap_or("").trim();
        if !id.is_empty() {
            return Ok(format!("https://www.youtube.com/embed/{id}"));
        }
    }
    if let Some(query) = trimmed.split("watch?").nth(1) {
        for pair in query.split('&') {
            if let Some(id) = pair.strip_prefix("v=") {
                let id = id.trim();
                if !id.is_empty() {
                    return Ok(format!("https://www.youtube.com/embed/{id}"));
                }
            }
        }
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_short_youtube_url_to_embed_url() {
        assert_eq!(
            normalize_youtube_url("https://youtu.be/abc123?t=8").unwrap(),
            "https://www.youtube.com/embed/abc123"
        );
    }

    #[test]
    fn keeps_non_youtube_url_for_local_media() {
        assert_eq!(
            normalize_youtube_url("http://127.0.0.1:8600/live/index.m3u8").unwrap(),
            "http://127.0.0.1:8600/live/index.m3u8"
        );
    }
}
