use axum::{
    Json, Router,
    extract::Path,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{Html, IntoResponse},
    routing::{delete, get, post},
};
use futures_util::Stream;
mod mesh;
use mesh::transports::local_http::{LocalHttpTransport, get_peer_json, parse_http_url};
use mesh::transports::stubs::StubTransport;
use mesh::{MeshEngine, SceneLayer, SceneLayerPatch};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tower::ServiceExt;
use tower_http::services::ServeDir;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::Level;
use uuid::Uuid;

struct AppState {
    events_tx: broadcast::Sender<Envelope>,
    started_at: OffsetDateTime,
    mesh: MeshEngine,
    apps: RwLock<HashMap<String, RegisteredApp>>,
    scene_state: RwLock<SceneStateModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope {
    version: String,
    kind: String,
    request_id: Uuid,
    timestamp: String,
    source: String,
    capability: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    ok: bool,
    name: &'static str,
    version: &'static str,
    started_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilitiesResponse {
    ok: bool,
    capabilities: Vec<Capability>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Capability {
    id: &'static str,
    version: &'static str,
    description: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisteredApp {
    id: String,
    app_type: String,
    dist_dir: String,
    entry: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterAppRequest {
    id: String,
    #[serde(default)]
    app_type: Option<String>,
    dist_dir: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RegisterAppResponse {
    ok: bool,
    app: RegisteredApp,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AppsListResponse {
    ok: bool,
    apps: Vec<RegisteredApp>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PingRequest {
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PingResponse {
    ok: bool,
    request_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshNodeResponse {
    ok: bool,
    node_id: String,
    started_at: String,
    node: mesh::identity::NodeIdentity,
    transports: Vec<mesh::engine::MeshTransportStatus>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshPeersResponse {
    ok: bool,
    node_id: String,
    peers: Vec<mesh::MeshPeer>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshPeerResponse {
    ok: bool,
    node_id: String,
    peer: mesh::MeshPeer,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshConnectRequest {
    url: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default = "default_connect_ping")]
    ping: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshConnectResponse {
    ok: bool,
    node_id: String,
    peer: mesh::MeshPeer,
    reachable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_started_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteMeshNodeResponse {
    ok: bool,
    node_id: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    node: Option<mesh::identity::NodeIdentity>,
}

fn default_connect_ping() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneAmbience {
    preset: String,
    day_haze: bool,
    rain: bool,
    city_blink: bool,
    window_lightning: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneObjects {
    left_chinese_sign: bool,
    top_marquee: bool,
    shelf_robot_signal: bool,
    blue_robot_pulse: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneStateModel {
    ambience: SceneAmbience,
    objects: SceneObjects,
    #[serde(default)]
    layers: Vec<SceneLayer>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SceneStateResponse {
    ok: bool,
    scene_state: SceneStateModel,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneAmbiencePatch {
    preset: Option<String>,
    day_haze: Option<bool>,
    rain: Option<bool>,
    city_blink: Option<bool>,
    window_lightning: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneObjectsPatch {
    left_chinese_sign: Option<bool>,
    top_marquee: Option<bool>,
    shelf_robot_signal: Option<bool>,
    blue_robot_pulse: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SceneStatePatch {
    #[serde(default)]
    ambience: SceneAmbiencePatch,
    #[serde(default)]
    objects: SceneObjectsPatch,
    #[serde(default)]
    layers: Option<Vec<SceneLayer>>,
    #[serde(default)]
    layer: Option<SceneLayerPatch>,
}

impl Default for SceneStateModel {
    fn default() -> Self {
        Self {
            ambience: SceneAmbience {
                preset: "night".to_string(),
                day_haze: false,
                rain: true,
                city_blink: true,
                window_lightning: true,
            },
            objects: SceneObjects {
                left_chinese_sign: true,
                top_marquee: true,
                shelf_robot_signal: true,
                blue_robot_pulse: true,
            },
            layers: Vec::new(),
        }
    }
}

impl SceneStateModel {
    fn apply_patch(&mut self, patch: SceneStatePatch) {
        if let Some(preset) = patch.ambience.preset {
            let trimmed = preset.trim();
            if !trimmed.is_empty() {
                self.ambience.preset = trimmed.to_string();
            }
        }
        if let Some(day_haze) = patch.ambience.day_haze {
            self.ambience.day_haze = day_haze;
        }
        if let Some(rain) = patch.ambience.rain {
            self.ambience.rain = rain;
        }
        if let Some(city_blink) = patch.ambience.city_blink {
            self.ambience.city_blink = city_blink;
        }
        if let Some(window_lightning) = patch.ambience.window_lightning {
            self.ambience.window_lightning = window_lightning;
        }

        if let Some(left_chinese_sign) = patch.objects.left_chinese_sign {
            self.objects.left_chinese_sign = left_chinese_sign;
        }
        if let Some(top_marquee) = patch.objects.top_marquee {
            self.objects.top_marquee = top_marquee;
        }
        if let Some(shelf_robot_signal) = patch.objects.shelf_robot_signal {
            self.objects.shelf_robot_signal = shelf_robot_signal;
        }
        if let Some(blue_robot_pulse) = patch.objects.blue_robot_pulse {
            self.objects.blue_robot_pulse = blue_robot_pulse;
        }

        if let Some(layers) = patch.layers {
            self.layers = layers;
        }
        if let Some(layer_patch) = patch.layer {
            self.apply_layer_patch(layer_patch);
        }
    }

    fn apply_layer_patch(&mut self, patch: SceneLayerPatch) {
        let Some(layer_id) = patch.layer_id else {
            return;
        };
        let Some(layer) = self
            .layers
            .iter_mut()
            .find(|candidate| candidate.layer_id == layer_id)
        else {
            return;
        };
        if let Some(visible) = patch.visible {
            layer.presentation.visible = visible;
        }
        if let Some(slot) = patch.slot {
            layer.presentation.slot = slot;
        }
        if let Some(motion) = patch.motion {
            layer.presentation.motion = motion;
        }
        if let Some(profile) = patch.profile {
            layer.presentation.profile = profile;
        }
        if let Some(z_index) = patch.z_index {
            layer.presentation.z_index = z_index;
        }
        if let Some(source) = patch.source {
            layer.source = Some(source);
        }
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn started_at_rfc3339(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn emit_ping_signal(
    state: &AppState,
    kind: &'static str,
    peer_id: Option<&str>,
    payload: serde_json::Value,
) {
    let env = Envelope {
        version: "1".to_string(),
        kind: kind.to_string(),
        request_id: Uuid::new_v4(),
        timestamp: now_rfc3339(),
        source: "master-program".to_string(),
        capability: "core.ping".to_string(),
        payload: serde_json::json!({
            "peerId": peer_id,
            "signal": kind,
            "detail": payload,
        }),
    };

    let _ = state.events_tx.send(env);
}

fn base_capabilities() -> Vec<Capability> {
    vec![
        Capability {
            id: "core.health",
            version: "1",
            description: "Health check endpoint.",
        },
        Capability {
            id: "core.events",
            version: "1",
            description: "Server-sent events stream.",
        },
        Capability {
            id: "core.ping",
            version: "1",
            description: "Emit a ping event.",
        },
        Capability {
            id: "plugins.registry",
            version: "1",
            description: "Register external dist apps (platform surfaces).",
        },
        Capability {
            id: "scene-control",
            version: "1",
            description: "Live scene ambience and object switch control.",
        },
        Capability {
            id: "mesh.peers",
            version: "0",
            description: "Prototype mesh peer registry and remote ping.",
        },
        Capability {
            id: "mesh.connect",
            version: "0",
            description: "Manual node-to-node connection handshake over an available transport.",
        },
        Capability {
            id: "mesh.protocol",
            version: "0",
            description: "Versioned local-first mesh protocol envelope and router.",
        },
        Capability {
            id: "mesh.stream",
            version: "0",
            description: "Text stream sessions with ACK/NACK, backpressure, and error control.",
        },
        Capability {
            id: "scene.layers",
            version: "0",
            description: "Dynamic scene layers for media embeds, effects, artwork, and data panels.",
        },
    ]
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        name: "master-program",
        version: env!("CARGO_PKG_VERSION"),
        started_at: started_at_rfc3339(state.started_at),
    })
}

async fn capabilities() -> Json<CapabilitiesResponse> {
    Json(CapabilitiesResponse {
        ok: true,
        capabilities: base_capabilities(),
    })
}

async fn ping(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PingRequest>,
) -> Result<Json<PingResponse>, (StatusCode, Json<serde_json::Value>)> {
    let request_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "message": req.message.unwrap_or_else(|| "ping".to_string()),
    });

    let env = Envelope {
        version: "1".to_string(),
        kind: "core.ping.received".to_string(),
        request_id,
        timestamp: now_rfc3339(),
        source: "master-program".to_string(),
        capability: "core.ping".to_string(),
        payload,
    };

    let _ = state.events_tx.send(env);

    Ok(Json(PingResponse {
        ok: true,
        request_id,
    }))
}

async fn mesh_node(State(state): State<Arc<AppState>>) -> Json<MeshNodeResponse> {
    let node = state.mesh.node();
    Json(MeshNodeResponse {
        ok: true,
        node_id: node.node_id.clone(),
        started_at: started_at_rfc3339(state.started_at),
        node,
        transports: state.mesh.transport_statuses(),
    })
}

async fn list_mesh_peers(State(state): State<Arc<AppState>>) -> Json<MeshPeersResponse> {
    let node = state.mesh.node();
    Json(MeshPeersResponse {
        ok: true,
        node_id: node.node_id,
        peers: state.mesh.list_peers(),
    })
}

async fn register_mesh_peer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<mesh::RegisterPeerRequest>,
) -> Result<Json<MeshPeerResponse>, (StatusCode, Json<serde_json::Value>)> {
    if let Some(url) = req
        .url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
    {
        if parse_http_url(url.trim_end_matches('/')).is_err() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "peer_url_must_be_http_url" })),
            ));
        }
    }

    let peer = state.mesh.register_peer_request(req).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": error })),
        )
    })?;
    let node = state.mesh.node();

    Ok(Json(MeshPeerResponse {
        ok: true,
        node_id: node.node_id,
        peer: mesh::MeshPeer::from(peer),
    }))
}

async fn connect_mesh_peer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MeshConnectRequest>,
) -> Result<Json<MeshConnectResponse>, (StatusCode, Json<serde_json::Value>)> {
    let url = req.url.trim().trim_end_matches('/').to_string();
    if url.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "peer_url_required" })),
        ));
    }
    if parse_http_url(&url).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "peer_url_must_be_http_url" })),
        ));
    }

    let remote_value = get_peer_json(&url, "/v1/mesh/node").map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "peer_node_unreachable", "detail": error })),
        )
    })?;
    let remote: RemoteMeshNodeResponse = serde_json::from_value(remote_value).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "peer_node_invalid", "detail": error.to_string() })),
        )
    })?;
    if !remote.ok {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "peer_node_not_ok" })),
        ));
    }

    let remote_node = remote.node;
    let peer_id = req
        .id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| remote.node_id.clone());
    let display_name = req.display_name.or_else(|| {
        remote_node
            .as_ref()
            .and_then(|node| node.display_name.clone())
    });

    let peer = state
        .mesh
        .register_peer_request(mesh::RegisterPeerRequest {
            id: peer_id.clone(),
            role: remote_node.as_ref().map(|node| node.role.clone()),
            url: Some(url),
            display_name,
            transport: Some("local_http".to_string()),
            transports: vec![],
            capabilities: remote_node
                .as_ref()
                .map(|node| node.capabilities.clone())
                .unwrap_or_else(|| vec!["mesh.control".to_string(), "core.ping".to_string()]),
            transmission_profile: remote_node
                .as_ref()
                .and_then(|node| node.transmission_profile.clone()),
            connectivity_policy: remote_node
                .as_ref()
                .map(|node| node.connectivity_policy.clone()),
            metadata: serde_json::json!({
                "connectedVia": "manual_http",
                "remoteNodeId": remote.node_id,
            }),
        })
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": error })),
            )
        })?;

    let (peer, reachable) = if req.ping {
        emit_ping_signal(
            state.as_ref(),
            "core.ping.sent",
            Some(&peer_id),
            serde_json::json!({ "url": req.url }),
        );
        let outcome = state.mesh.ping_peer(&peer_id).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "peer_ping_failed", "detail": error })),
            )
        })?;
        emit_ping_signal(
            state.as_ref(),
            "core.ping.confirmed",
            Some(&peer_id),
            serde_json::json!({
                "url": req.url,
                "latencyMs": outcome.peer.latency_ms,
            }),
        );
        (outcome.peer, true)
    } else {
        (peer, false)
    };
    let node = state.mesh.node();

    Ok(Json(MeshConnectResponse {
        ok: true,
        node_id: node.node_id,
        peer: mesh::MeshPeer::from(peer),
        reachable,
        remote_started_at: remote.started_at,
    }))
}

async fn ping_mesh_peer(
    State(state): State<Arc<AppState>>,
    Path(peer_id): Path<String>,
) -> Result<Json<MeshPeerResponse>, (StatusCode, Json<serde_json::Value>)> {
    emit_ping_signal(
        state.as_ref(),
        "core.ping.sent",
        Some(&peer_id),
        serde_json::json!({}),
    );
    let outcome = state.mesh.ping_peer(&peer_id).map_err(|error| {
        let status = if error == "peer_not_found" {
            StatusCode::NOT_FOUND
        } else if error == "peer_rate_limited" {
            StatusCode::TOO_MANY_REQUESTS
        } else if error == "peer_transport_unavailable" {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::BAD_GATEWAY
        };
        (status, Json(serde_json::json!({ "error": error })))
    })?;
    emit_ping_signal(
        state.as_ref(),
        "core.ping.confirmed",
        Some(&peer_id),
        serde_json::json!({
            "latencyMs": outcome.peer.latency_ms,
            "transport": outcome.peer.last_transport,
        }),
    );
    let node = state.mesh.node();

    Ok(Json(MeshPeerResponse {
        ok: outcome.ok,
        node_id: node.node_id,
        peer: mesh::MeshPeer::from(outcome.peer),
    }))
}

async fn receive_mesh_envelope(
    State(state): State<Arc<AppState>>,
    Json(envelope): Json<mesh::envelope::ProtocolEnvelope>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let accepted = state.mesh.receive(envelope).map_err(|error| {
        let status = match error.as_str() {
            "peer_rate_limited" => StatusCode::TOO_MANY_REQUESTS,
            "payload_too_large" | "invalid_envelope" | "unsupported_protocol_version" => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::BAD_GATEWAY,
        };
        (status, Json(serde_json::json!({ "error": error })))
    })?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "accepted": accepted,
    })))
}

async fn upsert_scene_layer(
    State(state): State<Arc<AppState>>,
    Json(layer): Json<SceneLayer>,
) -> Result<Json<SceneStateResponse>, (StatusCode, Json<serde_json::Value>)> {
    if layer.layer_id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "layer_id_required" })),
        ));
    }

    let scene_state = {
        let mut scene_state = state.scene_state.write().await;
        if let Some(existing) = scene_state
            .layers
            .iter_mut()
            .find(|candidate| candidate.layer_id == layer.layer_id)
        {
            *existing = layer;
        } else {
            scene_state.layers.push(layer);
        }
        scene_state.clone()
    };

    emit_scene_state(state.as_ref(), &scene_state);
    Ok(Json(SceneStateResponse {
        ok: true,
        scene_state,
    }))
}

async fn remove_scene_layer(
    State(state): State<Arc<AppState>>,
    Path(layer_id): Path<String>,
) -> Json<SceneStateResponse> {
    let scene_state = {
        let mut scene_state = state.scene_state.write().await;
        scene_state
            .layers
            .retain(|layer| layer.layer_id != layer_id);
        scene_state.clone()
    };
    emit_scene_state(state.as_ref(), &scene_state);
    Json(SceneStateResponse {
        ok: true,
        scene_state,
    })
}

async fn get_scene_state(State(state): State<Arc<AppState>>) -> Json<SceneStateResponse> {
    let scene_state = state.scene_state.read().await.clone();
    Json(SceneStateResponse {
        ok: true,
        scene_state,
    })
}

async fn update_scene_state(
    State(state): State<Arc<AppState>>,
    Json(patch): Json<SceneStatePatch>,
) -> Result<Json<SceneStateResponse>, (StatusCode, Json<serde_json::Value>)> {
    let scene_state = {
        let mut scene_state = state.scene_state.write().await;
        scene_state.apply_patch(patch);
        scene_state.clone()
    };

    emit_scene_state(state.as_ref(), &scene_state);

    Ok(Json(SceneStateResponse {
        ok: true,
        scene_state,
    }))
}

fn emit_scene_state(state: &AppState, scene_state: &SceneStateModel) {
    let env = Envelope {
        version: "1".to_string(),
        kind: "scene.state.updated".to_string(),
        request_id: Uuid::new_v4(),
        timestamp: now_rfc3339(),
        source: "master-program".to_string(),
        capability: "scene-control".to_string(),
        payload: serde_json::json!({
            "sceneState": scene_state,
        }),
    };

    let _ = state.events_tx.send(env);
}

fn validate_dist_dir(dist_dir: &FsPath) -> Result<(PathBuf, PathBuf), String> {
    let canonical = dist_dir
        .canonicalize()
        .map_err(|e| format!("dist_dir_invalid: {e}"))?;
    if !canonical.is_dir() {
        return Err("dist_dir_not_a_directory".to_string());
    }
    let index = canonical.join("index.html");
    if !index.is_file() {
        return Err("dist_missing_index_html".to_string());
    }
    Ok((canonical, index))
}

async fn register_app(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterAppRequest>,
) -> Result<Json<RegisterAppResponse>, (StatusCode, Json<serde_json::Value>)> {
    if req.id.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "id_required" })),
        ));
    }

    let app_type = req
        .app_type
        .unwrap_or_else(|| "platform".to_string())
        .trim()
        .to_string();

    let (canonical_dist, index) =
        validate_dist_dir(FsPath::new(&req.dist_dir)).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": error })),
            )
        })?;

    let app = RegisteredApp {
        id: req.id.trim().to_string(),
        app_type,
        dist_dir: canonical_dist.to_string_lossy().to_string(),
        entry: index.to_string_lossy().to_string(),
    };

    {
        let mut apps = state.apps.write().await;
        apps.insert(app.id.clone(), app.clone());
    }

    Ok(Json(RegisterAppResponse { ok: true, app }))
}

async fn list_apps(State(state): State<Arc<AppState>>) -> Json<AppsListResponse> {
    let apps = state.apps.read().await;
    Json(AppsListResponse {
        ok: true,
        apps: apps.values().cloned().collect(),
    })
}

async fn app_index(
    State(state): State<Arc<AppState>>,
    Path(app_id): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let apps = state.apps.read().await;
    let Some(app) = apps.get(&app_id) else {
        return Err(StatusCode::NOT_FOUND);
    };

    let html = tokio::fs::read_to_string(&app.entry)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Html(html))
}

async fn app_asset(
    State(state): State<Arc<AppState>>,
    Path((app_id, rest)): Path<(String, String)>,
) -> Result<impl IntoResponse, StatusCode> {
    let apps = state.apps.read().await;
    let Some(app) = apps.get(&app_id) else {
        return Err(StatusCode::NOT_FOUND);
    };

    let svc = ServeDir::new(&app.dist_dir);
    let mut req = axum::http::Request::builder()
        .uri(format!("/{}", rest))
        .body(axum::body::Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // Some static middleware expects a Host header; keep it minimal.
    req.headers_mut()
        .insert("Host", HeaderValue::from_static("master-program"));

    svc.oneshot(req).await.map_err(|_| StatusCode::NOT_FOUND)
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.events_tx.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(envelope) => {
                    if let Ok(json) = serde_json::to_string(&envelope) {
                        let evt = if envelope.kind.starts_with("core.ping.") {
                            Event::default().event(envelope.kind.clone()).data(json)
                        } else if envelope.kind == "scene.state.updated" {
                            Event::default().event("scene.state.updated").data(json)
                        } else {
                            Event::default().data(json)
                        };
                        yield Ok(evt);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(20)))
}

fn sse_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("no-cache, no-transform"),
    );
    headers.insert("X-Accel-Buffering", HeaderValue::from_static("no"));
    headers
}

async fn events_with_headers(
    state: State<Arc<AppState>>,
) -> (
    HeaderMap,
    Sse<impl Stream<Item = Result<Event, Infallible>>>,
) {
    (sse_headers(), events(state).await)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "master_program=info,tower_http=info".into()),
        )
        .init();

    let (events_tx, _events_rx) = broadcast::channel::<Envelope>(256);
    let node_id = std::env::var("MASTER_PROGRAM_NODE_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("node-{}", Uuid::new_v4()));
    let mesh = MeshEngine::new(node_id, Some("master-program".to_string()));
    mesh.register_transport(Arc::new(LocalHttpTransport));
    mesh.register_transport(Arc::new(StubTransport::web_rtc()));
    mesh.register_transport(Arc::new(StubTransport::bluetooth()));
    mesh.register_transport(Arc::new(StubTransport::serial()));
    mesh.register_transport(Arc::new(StubTransport::store_forward()));

    let state = Arc::new(AppState {
        events_tx,
        started_at: OffsetDateTime::now_utc(),
        mesh,
        apps: RwLock::new(HashMap::new()),
        scene_state: RwLock::new(SceneStateModel::default()),
    });

    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/ping", post(ping))
        .route("/v1/mesh/node", get(mesh_node))
        .route("/v1/mesh/peers", get(list_mesh_peers))
        .route("/v1/mesh/connect", post(connect_mesh_peer))
        .route("/v1/mesh/peers/register", post(register_mesh_peer))
        .route("/v1/mesh/peers/{id}/ping", post(ping_mesh_peer))
        .route("/v1/mesh/envelope", post(receive_mesh_envelope))
        .route(
            "/v1/scene-state",
            get(get_scene_state).post(update_scene_state),
        )
        .route("/v1/scene-layers", post(upsert_scene_layer))
        .route("/v1/scene-layers/{id}", delete(remove_scene_layer))
        .route("/v1/events", get(events_with_headers))
        .route("/v1/apps", get(list_apps))
        .route("/v1/apps/register", post(register_app))
        .route("/apps/{id}/", get(app_index))
        .route("/apps/{id}/{*rest}", get(app_asset))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let port = std::env::var("MASTER_PROGRAM_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(17321);

    let host = std::env::var("MASTER_PROGRAM_HOST")
        .ok()
        .and_then(|v| v.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let addr = SocketAddr::from((host, port));

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!("failed to bind http://{addr}: {error}");
            std::process::exit(1);
        }
    };

    tracing::info!("master-program listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("serve");
}
