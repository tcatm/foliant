use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use foliant::Streamer;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use foliant::multi_list::MultiShardListStreamer;
use foliant::{load_db, Database, Entry, TagMode};

/// Command-line options for the HTTP server.
#[derive(Parser)]
#[command(
    author,
    version,
    about = "Foliant HTTP server for browsing prefix indexes"
)]
struct Config {
    /// Path to the on-disk index (single .idx file or directory of shards)
    #[arg(short, long, value_name = "INDEX")]
    index: PathBuf,

    /// Address to bind the HTTP server to (e.g. 127.0.0.1:3000)
    #[arg(long, default_value = "127.0.0.1:3000")]
    addr: SocketAddr,

    /// Default maximum number of items to return in one page
    #[arg(short = 'n', long, default_value_t = 1000)]
    limit: usize,
}

/// Shared application state.
#[derive(Clone)]
struct AppState {
    db: Arc<Database<serde_json::Value>>,
    default_limit: usize,
}

/// Standard paged response envelope.
#[derive(Serialize)]
struct Paged<T> {
    items: Vec<T>,
    next_cursor: Option<String>,
}

/// Error response.
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// Entry object returned for each key or common prefix, including type and optional payload.
#[derive(Serialize)]
struct KeyEntry {
    /// The key or common prefix string
    key: String,
    /// Entry type: "Key" or "CommonPrefix"
    #[serde(rename = "type")]
    kind: String,
    /// Optional JSON payload associated with the key (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cfg = Config::parse();

    // Load the database (single shard or directory), then attach any .tags indexes
    let mut db = load_db::<serde_json::Value, _>(&cfg.index, None)?;
    db.load_tag_index()?;

    let state = AppState {
        db: Arc::new(db),
        default_limit: cfg.limit,
    };

    // Build the router
    let app = Router::new()
        .route("/keys", get(list_keys))
        .route("/tags", get(list_tags_names))
        .route("/tags/counts", get(list_tags_counts))
        .route("/tags/filter", get(list_by_tags))
        .with_state(state);

    tracing::info!("listening on {}", cfg.addr);
    axum::Server::bind(&cfg.addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

/// Query parameters for /keys
#[derive(Deserialize)]
struct KeysParams {
    prefix: Option<String>,
    delim: Option<char>,
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_keys(
    State(state): State<AppState>,
    Query(params): Query<KeysParams>,
) -> Result<Json<Paged<KeyEntry>>, (StatusCode, Json<ErrorResponse>)> {
    // parse limit
    let limit = params.limit.unwrap_or(state.default_limit);
    // build prefix bytes
    let prefix_bytes = params.prefix.unwrap_or_default().into_bytes();
    // delimiter
    let delim = params.delim;
    // initialize streamer
    let mut stream = if let Some(cur) = params.cursor {
        match general_purpose::STANDARD.decode(cur) {
            Ok(bytes) => MultiShardListStreamer::resume(
                &state.db.shards(),
                prefix_bytes.clone(),
                delim.map(|c| c as u8),
                bytes,
            ),
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid cursor: {}", e),
                    }),
                ))
            }
        }
    } else {
        MultiShardListStreamer::new(
            &state.db.shards(),
            prefix_bytes.clone(),
            delim.map(|c| c as u8),
        )
    };

    let mut items = Vec::new();
    for _ in 0..limit {
        if let Some(entry) = stream.next() {
            let key = entry.as_str().to_string();
            let kind = entry.kind().to_string();
            let payload = match entry {
                Entry::Key(_, _, payload) => payload,
                Entry::CommonPrefix(_) => None,
            };
            items.push(KeyEntry { key, kind, payload });
        } else {
            break;
        }
    }

    // build next_cursor
    let next_cursor = if items.len() == limit {
        Some(general_purpose::STANDARD.encode(stream.cursor()))
    } else {
        None
    };

    Ok(Json(Paged { items, next_cursor }))
}

/// Query parameters for tag listing (names only)
#[derive(Deserialize)]
struct TagsParams {
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_tags_names(
    State(state): State<AppState>,
    Query(params): Query<TagsParams>,
) -> Result<Json<Paged<String>>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(state.default_limit);
    let mut stream = match params.cursor {
        Some(cur) => {
            let bytes = general_purpose::STANDARD.decode(cur).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid cursor: {}", e),
                    }),
                )
            })?;
            let mut s = state.db.list_tag_names(params.prefix.as_deref()).unwrap();
            s.seek(bytes);
            s
        }
        None => state.db.list_tag_names(params.prefix.as_deref()).unwrap(),
    };

    let mut items = Vec::new();
    for _ in 0..limit {
        if let Some(tag) = stream.next() {
            items.push(tag);
        } else {
            break;
        }
    }

    let next_cursor = if items.len() == limit {
        Some(general_purpose::STANDARD.encode(stream.cursor()))
    } else {
        None
    };

    Ok(Json(Paged { items, next_cursor }))
}

/// Query parameters for tag counts listing
#[derive(Deserialize)]
struct TagsCountsParams {
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct TagCount {
    tag: String,
    count: usize,
}

async fn list_tags_counts(
    State(state): State<AppState>,
    Query(params): Query<TagsCountsParams>,
) -> Result<Json<Paged<TagCount>>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(state.default_limit);
    let mut stream = match params.cursor {
        Some(cur) => {
            let bytes = general_purpose::STANDARD.decode(cur).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid cursor: {}", e),
                    }),
                )
            })?;
            let mut s = state.db.list_tags(params.prefix.as_deref()).unwrap();
            s.seek(bytes);
            s
        }
        None => state.db.list_tags(params.prefix.as_deref()).unwrap(),
    };

    let mut items = Vec::new();
    for _ in 0..limit {
        if let Some((tag, cnt)) = stream.next() {
            items.push(TagCount { tag, count: cnt });
        } else {
            break;
        }
    }

    let next_cursor = if items.len() == limit {
        Some(general_purpose::STANDARD.encode(stream.cursor()))
    } else {
        None
    };

    Ok(Json(Paged { items, next_cursor }))
}

/// Query parameters for list_by_tags
#[derive(Deserialize)]
struct TagsFilterParams {
    include: Option<String>,
    exclude: Option<String>,
    mode: Option<String>,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn list_by_tags(
    State(state): State<AppState>,
    Query(params): Query<TagsFilterParams>,
) -> Result<Json<Paged<String>>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(state.default_limit);
    let include_tags: Vec<&str> = params
        .include
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .collect();
    let exclude_tags: Vec<&str> = params
        .exclude
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .collect();
    let mode = match params.mode.as_deref() {
        Some("or") => TagMode::Or,
        _ => TagMode::And,
    };

    let mut stream = match params.cursor {
        Some(cur) => {
            let bytes = general_purpose::STANDARD.decode(cur).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid cursor: {}", e),
                    }),
                )
            })?;
            let mut s = state
                .db
                .list_by_tags(&include_tags, &exclude_tags, mode, params.prefix.as_deref())
                .unwrap();
            s.seek(bytes);
            s
        }
        None => state
            .db
            .list_by_tags(&include_tags, &exclude_tags, mode, params.prefix.as_deref())
            .unwrap(),
    };

    let mut items = Vec::new();
    for _ in 0..limit {
        if let Some(Entry::Key(k, _, _)) = stream.next() {
            items.push(k);
        } else {
            break;
        }
    }

    let next_cursor = if items.len() == limit {
        Some(general_purpose::STANDARD.encode(stream.cursor()))
    } else {
        None
    };

    Ok(Json(Paged { items, next_cursor }))
}
