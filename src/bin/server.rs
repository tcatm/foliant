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

use foliant::multi_list::{MultiShardListStreamer, TagFilterBitmap};
use foliant::{load_db, Database, Entry, TagMode, TagFilterConfig};

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

    /// Maximum allowed limit for any single request
    #[arg(short = 'n', long, default_value_t = 1000)]
    limit: usize,
}

/// Shared application state.
#[derive(Clone)]
struct AppState {
    db: Arc<Database<serde_json::Value>>,
    max_limit: usize,
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
    /// Number of children under this common prefix (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
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
        max_limit: cfg.limit,
    };

    // Build the router
    let app = Router::new()
        .route("/keys", get(list_keys))
        .route("/tags", get(list_tags_names))
        .route("/tags/counts", get(list_tags_counts))
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
    /// Comma-separated list of tags to include (AND/OR based on mode)
    include_tags: Option<String>,
    /// Comma-separated list of tags to exclude
    exclude_tags: Option<String>,
    /// Tag combination mode: "and" or "or" (default: "and")
    mode: Option<String>,
}

async fn list_keys(
    State(state): State<AppState>,
    Query(params): Query<KeysParams>,
) -> Result<Json<Paged<KeyEntry>>, (StatusCode, Json<ErrorResponse>)> {
    // parse limit with default of max_limit and cap client requests
    let limit = params.limit.map(|l| l.min(state.max_limit)).unwrap_or(state.max_limit);
    // build prefix bytes
    let prefix_bytes = params.prefix.unwrap_or_default().into_bytes();
    // delimiter
    let delim = params.delim;
    
    // Parse tag filtering parameters
    let tag_config = if params.include_tags.is_some() || params.exclude_tags.is_some() {
        let include_tags: Vec<String> = params
            .include_tags
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        
        let exclude_tags: Vec<String> = params
            .exclude_tags
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        
        let mode = match params.mode.as_deref() {
            Some("or") => TagMode::Or,
            _ => TagMode::And,
        };
        
        Some(TagFilterConfig {
            include_tags,
            exclude_tags,
            mode,
        })
    } else {
        None
    };
    
    // Build a tag-based bitmap filter from the TagFilterConfig, if any
    let tag_filter: Option<TagFilterBitmap> = if let Some(cfg) = tag_config.clone() {
        match TagFilterBitmap::new(
            &state.db.shards(),
            &cfg.include_tags,
            &cfg.exclude_tags,
            cfg.mode,
        ) {
            Ok(bm) => Some(bm),
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: format!("failed to build tag filter: {}", e) }),
                ));
            }
        }
    } else {
        None
    };
    
    // Initialize streamer - use MultiShardListStreamer with optional bitmap filter
    let stream = if let Some(cur) = params.cursor {
        match general_purpose::STANDARD.decode(cur) {
            Ok(bytes) => MultiShardListStreamer::resume_with_filter(
                &state.db.shards(),
                prefix_bytes.clone(),
                delim.map(|c| c as u8),
                bytes,
                tag_filter.clone(),
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
        MultiShardListStreamer::new_with_filter(
            &state.db.shards(),
            prefix_bytes.clone(),
            delim.map(|c| c as u8),
            tag_filter.clone(),
        )
    };
    
    let mut stream = match stream {
        Ok(s) => s,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to create tag-filtered stream: {}", e),
                }),
            ))
        }
    };

    let mut items = Vec::new();
    for _ in 0..limit {
        if let Some(entry) = stream.next() {
            let key = entry.as_str().to_string();
            let kind = entry.kind().to_string();
            let (payload, count) = match &entry {
                Entry::Key(_, _, payload) => (payload.clone(), None),
                Entry::CommonPrefix(_, count) => (None, *count),
            };
            items.push(KeyEntry { key, kind, payload, count });
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
    let limit = params.limit.map(|l| l.min(state.max_limit)).unwrap_or(state.max_limit);
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
    let limit = params.limit.map(|l| l.min(state.max_limit)).unwrap_or(state.max_limit);
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

