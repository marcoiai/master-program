use axum::{
    Json, Router,
    extract::Path,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
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
    node_id: String,
    apps: RwLock<HashMap<String, RegisteredApp>>,
    mesh_peers: RwLock<HashMap<String, MeshPeer>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MeshPeer {
    id: String,
    url: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_seen: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_ms: Option<u128>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterPeerRequest {
    id: String,
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeerPingRequest {
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshNodeResponse {
    ok: bool,
    node_id: String,
    started_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshPeersResponse {
    ok: bool,
    node_id: String,
    peers: Vec<MeshPeer>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeshPeerResponse {
    ok: bool,
    node_id: String,
    peer: MeshPeer,
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
    Json(MeshNodeResponse {
        ok: true,
        node_id: state.node_id.clone(),
        started_at: started_at_rfc3339(state.started_at),
    })
}

async fn list_mesh_peers(State(state): State<Arc<AppState>>) -> Json<MeshPeersResponse> {
    let peers = state.mesh_peers.read().await;
    Json(MeshPeersResponse {
        ok: true,
        node_id: state.node_id.clone(),
        peers: peers.values().cloned().collect(),
    })
}

async fn register_mesh_peer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterPeerRequest>,
) -> Result<Json<MeshPeerResponse>, (StatusCode, Json<serde_json::Value>)> {
    let id = req.id.trim();
    let url = req.url.trim().trim_end_matches('/');

    if id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "peer_id_required" })),
        ));
    }

    if parse_http_url(url).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "peer_url_must_be_http_url" })),
        ));
    }

    let peer = MeshPeer {
        id: id.to_string(),
        url: url.to_string(),
        status: "registered".to_string(),
        last_seen: None,
        latency_ms: None,
    };

    {
        let mut peers = state.mesh_peers.write().await;
        peers.insert(peer.id.clone(), peer.clone());
    }

    Ok(Json(MeshPeerResponse {
        ok: true,
        node_id: state.node_id.clone(),
        peer,
    }))
}

async fn ping_mesh_peer(
    State(state): State<Arc<AppState>>,
    Path(peer_id): Path<String>,
    Json(req): Json<PeerPingRequest>,
) -> Result<Json<MeshPeerResponse>, (StatusCode, Json<serde_json::Value>)> {
    let peer = {
        let peers = state.mesh_peers.read().await;
        peers.get(&peer_id).cloned()
    }
    .ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "peer_not_found" })),
        )
    })?;

    let started = Instant::now();
    let url = peer.url.clone();
    let message = req
        .message
        .unwrap_or_else(|| format!("mesh ping from {}", state.node_id));
    let result = tokio::task::spawn_blocking(move || {
        post_peer_ping(&url, &serde_json::json!({ "message": message }).to_string())
    })
    .await
    .map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": format!("peer_task_failed: {error}") })),
        )
    })?;

    let mut next_peer = peer;
    match result {
        Ok(()) => {
            next_peer.status = "online".to_string();
            next_peer.last_seen = Some(now_rfc3339());
            next_peer.latency_ms = Some(started.elapsed().as_millis());
        }
        Err(error) => {
            next_peer.status = format!("offline: {error}");
            next_peer.latency_ms = None;
        }
    }

    {
        let mut peers = state.mesh_peers.write().await;
        peers.insert(next_peer.id.clone(), next_peer.clone());
    }

    Ok(Json(MeshPeerResponse {
        ok: next_peer.status == "online",
        node_id: state.node_id.clone(),
        peer: next_peer,
    }))
}

fn parse_http_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "only_http_supported_in_mesh_v0".to_string())?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("missing_host".to_string());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let parsed_port = port.parse::<u16>().map_err(|_| "invalid_port".to_string())?;
            (host.to_string(), parsed_port)
        }
        None => (authority.to_string(), 80),
    };
    Ok((host, port, path))
}

fn post_peer_ping(base_url: &str, body: &str) -> Result<(), String> {
    let (host, port, base_path) = parse_http_url(base_url)?;
    let path = if base_path == "/" {
        "/v1/ping".to_string()
    } else {
        format!("{}/v1/ping", base_path.trim_end_matches('/'))
    };
    let mut stream = std::net::TcpStream::connect((host.as_str(), port))
        .map_err(|error| format!("connect_failed: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(4)))
        .map_err(|error| format!("read_timeout_failed: {error}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(4)))
        .map_err(|error| format!("write_timeout_failed: {error}"))?;

    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write_failed: {error}"))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("read_failed: {error}"))?;
    if response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2") {
        Ok(())
    } else {
        let status = response.lines().next().unwrap_or("empty_response");
        Err(format!("peer_http_error: {status}"))
    }
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

    Ok(Json(SceneStateResponse {
        ok: true,
        scene_state,
    }))
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
                        let evt = if envelope.kind == "core.ping.received" {
                            Event::default().event("core.ping.received").data(json)
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
    let state = Arc::new(AppState {
        events_tx,
        started_at: OffsetDateTime::now_utc(),
        node_id: std::env::var("MASTER_PROGRAM_NODE_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("node-{}", Uuid::new_v4())),
        apps: RwLock::new(HashMap::new()),
        mesh_peers: RwLock::new(HashMap::new()),
        scene_state: RwLock::new(SceneStateModel::default()),
    });

    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/ping", post(ping))
        .route("/v1/mesh/node", get(mesh_node))
        .route("/v1/mesh/peers", get(list_mesh_peers))
        .route("/v1/mesh/peers/register", post(register_mesh_peer))
        .route("/v1/mesh/peers/{id}/ping", post(ping_mesh_peer))
        .route(
            "/v1/scene-state",
            get(get_scene_state).post(update_scene_state),
        )
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
