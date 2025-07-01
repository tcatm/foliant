// Interactive shell navigation: REPL for browsing the index
// Maintains a current 'directory' prefix split by a delimiter
// Commands: ls, cd, pwd, help, exit/quit

use rustyline::{
    completion::{Completer, Pair},
    error::ReadlineError,
    highlight::Highlighter,
    hint::Hinter,
    validate::Validator,
    Context, Editor, Helper,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use ctrlc;
use foliant::{Database, Entry, Streamer, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, LazyTagFilter, LazySearchFilter, ComposedFilter};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{channel, Receiver},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Help text for interactive shell commands and Ctrl-C behavior.
const HELP_TEXT: &str = "\
Available commands:
  ls [-rck] [-d DELIM] [prefix] [#tag1 !tag2 ...] [-q term1 -q term2 -! excl1 ...]
    -r       recursive (no delimiter grouping)
    -c       only directories (common prefixes)
    -k       only keys
    -d <c>   one-off custom delimiter character
    #tag     include entries with tag (AND mode)
    !tag     exclude entries with tag
    -q <str> include entries containing substring (AND mode)
    -! <str> exclude entries containing substring
  cd [dir]                        change directory
  search <query>                  substring search using n-gram index
  tags                            list all tags with counts
  val <id> [shard]                lookup key by raw pointer ID
  val <shard>:<id>                (use shard index when multiple shards)
  pwd                             print working directory
  exit, quit                      exit shell
  help                            show this help

Ctrl-C once aborts a running ls; twice within 2s exits the shell

Debug mode: Start shell with --debug to show command execution times
";

/// Result of printing a stream: either completed or aborted via Ctrl-C.
enum PrintResult {
    Count(usize),
    Aborted,
}

/// Print entries from the given stream according to the flags, optionally abortable.
fn print_entries<S, V>(
    prefix: &str,
    stream: &mut S,
    only_prefix: bool,
    only_keys: bool,
    abort_rx: Option<&Receiver<()>>,
) -> Result<PrintResult, Box<dyn std::error::Error>>
where
    S: Streamer<Item = Entry<V>>,
    V: DeserializeOwned + Serialize,
{
    let mut printed = 0usize;
    loop {
        // Check for abort
        if let Some(rx) = abort_rx {
            if let Ok(_) = rx.try_recv() {
                println!("\nListing aborted");
                return Ok(PrintResult::Aborted);
            }
        }
        // Fetch next entry
        match stream.next() {
            Some(Entry::CommonPrefix(s, count)) if !only_keys => {
                let s = s.strip_prefix(prefix).unwrap_or(&s);
                // Color directory listings in blue with count if available
                if let Some(count) = count {
                    println!("\x1b[34m📁 {} ({})\x1b[0m", s, count);
                } else {
                    println!("\x1b[34m📁 {}\x1b[0m", s);
                }
                printed += 1;
            }
            Some(Entry::Key(orig, ptr, val_opt)) if !only_prefix => {
                let s = orig.strip_prefix(prefix).unwrap_or(&orig);
                // Print the raw pointer in subtle gray (bright black)
                if let Some(val) = val_opt {
                    let val_str = serde_json::to_string(&val)?;
                    println!("📄 {} \x1b[90m#{}\x1b[0m \x1b[2m{}\x1b[0m", s, ptr, val_str);
                } else {
                    println!("📄 {} \x1b[90m#{}\x1b[0m", s, ptr);
                }
                printed += 1;
            }
            Some(_) => {
                // Skip other entries
            }
            None => break,
        }
    }
    Ok(PrintResult::Count(printed))
}

/// Prints a summary for a completed entry listing: if any entries were printed,
/// emits a trailing line with the total count.
fn handle_print_result(res: Result<PrintResult, Box<dyn std::error::Error>>) {
    if let Ok(PrintResult::Count(printed)) = res {
        if printed > 0 {
            println!(
                "\n{} {} listed",
                printed,
                if printed == 1 { "entry" } else { "entries" }
            );
        }
    }
}

struct ShellState<V: DeserializeOwned + 'static> {
    db: Database<V>,
    delim: char,
    cwd: String,
    debug: bool,
}

struct ShellHelper<V: DeserializeOwned + 'static> {
    state: Rc<RefCell<ShellState<V>>>,
}

impl<V: DeserializeOwned + 'static> Helper for ShellHelper<V> {}

impl<V: DeserializeOwned + 'static> Completer for ShellHelper<V> {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let before = &line[..pos];
        let start = before.rfind(' ').map(|i| i + 1).unwrap_or(0);
        let word = &before[start..];
        let mut candidates = Vec::new();
        if start == 0 {
            let cmds = [
                "cd", "ls", "search", "pwd", "val", "tags", "shards", "exit", "quit",
                "help",
            ];
            for &cmd in &cmds {
                if cmd.starts_with(word) {
                    // Commands that take an argument get a trailing space
                    let replacement =
                        if ["cd", "ls", "search", "val"].contains(&cmd) {
                            format!("{} ", cmd)
                        } else {
                            cmd.to_string()
                        };
                    candidates.push(Pair {
                        display: cmd.to_string(),
                        replacement,
                    });
                }
            }
            return Ok((start, candidates));
        }
        let state = self.state.borrow();
        let cwd = &state.cwd;
        let delim = state.delim;
        let first_cmd = line.split_whitespace().next();
        if first_cmd == Some("ls") && (word.starts_with('#') || word.starts_with('!')) {
            let mut seen_tags = HashSet::new();
            let prefix_opt = if cwd.is_empty() {
                None
            } else {
                Some(cwd.as_str())
            };
            match state.db.list_tags(prefix_opt) {
                Ok(mut tag_stream) => {
                    while let Some((tag, _count)) = tag_stream.next() {
                        // For ls command: support '#' and '!' prefixes
                        let (marker, rest) = if word.starts_with('#') || word.starts_with('!') {
                            (Some(word.chars().next().unwrap()), &word[1..])
                        } else {
                            (None, word)
                        };
                        
                        if tag.starts_with(rest) && seen_tags.insert(tag.clone()) {
                            let mut display = String::new();
                            let mut replacement = String::new();
                            if let Some(m) = marker {
                                display.push(m);
                                replacement.push(m);
                            }
                            display.push_str(&tag);
                            replacement.push_str(&tag);
                            replacement.push(' ');
                            candidates.push(Pair {
                                display,
                                replacement,
                            });
                        }
                    }
                }
                Err(_) => return Ok((start, Vec::new())),
            }
            return Ok((start, candidates));
        }
        // Split word into parent path and fragment after last delimiter
        let (parent, frag) = if let Some(idx) = word.rfind(delim) {
            (&word[..idx], &word[idx + 1..])
        } else {
            ("", word)
        };
        // Build the FST search prefix: literal cwd plus any parent segment (no extra slash for cwd alone)
        let mut prefix = cwd.clone();
        if !parent.is_empty() {
            if !prefix.is_empty() {
                prefix.push(delim);
            }
            prefix.push_str(parent);
        }
        let mut stream = match state.db.list(&prefix, Some(delim)) {
            Ok(s) => s,
            Err(_) => return Ok((start, Vec::new())),
        };
        let mut seen = HashSet::new();
        // If the user is completing after 'cd', only offer directory prefixes
        let suggest_dirs_only = line.split_whitespace().next() == Some("cd");

        while let Some(entry) = stream.next() {
            match entry {
                Entry::CommonPrefix(s, _) => {
                    // Directory candidate
                    let rem = &s[prefix.len()..];
                    if let Some(seg) = rem.trim_end_matches(delim).split(delim).next() {
                        if seg.starts_with(frag) && seen.insert(seg.to_string()) {
                            let mut rep = String::new();
                            if !parent.is_empty() {
                                rep.push_str(parent);
                                rep.push(delim);
                            }
                            rep.push_str(seg);
                            rep.push(delim);
                            candidates.push(Pair {
                                display: rep.clone(),
                                replacement: rep,
                            });
                        }
                    }
                }
                Entry::Key(s, _ptr, _) if !suggest_dirs_only => {
                    // File candidate (skip when completing 'cd')
                    let rem = &s[prefix.len()..];
                    if let Some(seg) = rem.split(delim).next() {
                        if seg.starts_with(frag) && seen.insert(seg.to_string()) {
                            let mut rep = String::new();
                            if !parent.is_empty() {
                                rep.push_str(parent);
                                rep.push(delim);
                            }
                            rep.push_str(seg);
                            candidates.push(Pair {
                                display: rep.clone(),
                                replacement: rep,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
        Ok((start, candidates))
    }
}

// Disable inline hinting to avoid blocking on large trees
impl<V: DeserializeOwned + 'static> Hinter for ShellHelper<V> {
    // No inline hints
    type Hint = String;
}
// Use default highlighter (no-op)
impl<V: DeserializeOwned + 'static> Highlighter for ShellHelper<V> {}

impl<V: DeserializeOwned + 'static> Validator for ShellHelper<V> {}

/// Internal helper to process one shell command; returns true to exit shell
fn handle_cmd<V: DeserializeOwned + Serialize + 'static>(
    state: &Rc<RefCell<ShellState<V>>>,
    help_text: &str,
    abort_rx: Option<&Receiver<()>>,
    raw: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let line = raw.trim();
    if line.is_empty() {
        return Ok(false);
    }
    
    // Start timing if debug mode is enabled
    let debug_mode = state.borrow().debug;
    let start_time = if debug_mode {
        Some(std::time::Instant::now())
    } else {
        None
    };
    
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap();
    let result = match cmd {
        "ls" => {
            let mut recursive = false;
            let mut only_prefix = false;
            let mut only_keys = false;
            let mut custom_delim: Option<char> = None;
            let mut prefix_str = String::new();
            let mut include_tags = Vec::new();
            let mut exclude_tags = Vec::new();
            let mut include_queries = Vec::new();
            let mut exclude_queries = Vec::new();
            let mut prefix_set = false;
            
            while let Some(tok) = parts.next() {
                if tok == "-d" {
                    if let Some(dtok) = parts.next() {
                        if dtok.chars().count() == 1 {
                            custom_delim = dtok.chars().next();
                        } else {
                            println!("Invalid delimiter '{}'; must be single char", dtok);
                        }
                    } else {
                        println!("Flag -d requires a delimiter argument");
                    }
                    continue;
                } else if tok == "-q" {
                    if let Some(query) = parts.next() {
                        include_queries.push(query.to_string());
                    } else {
                        println!("Flag -q requires a query argument");
                    }
                    continue;
                } else if tok == "-!" {
                    if let Some(query) = parts.next() {
                        exclude_queries.push(query.to_string());
                    } else {
                        println!("Flag -! requires a query argument");
                    }
                    continue;
                } else if tok.starts_with("-d") {
                    let ds: Vec<char> = tok.chars().collect();
                    if ds.len() == 3 {
                        custom_delim = Some(ds[2]);
                    } else {
                        println!("Invalid -d flag '{}'; use -d<DELIM>", tok);
                    }
                    continue;
                } else if tok.starts_with('-') && tok.len() > 1 {
                    for ch in tok.chars().skip(1) {
                        match ch {
                            'r' => recursive = true,
                            'c' => only_prefix = true,
                            'k' => only_keys = true,
                            _ => println!("Unknown flag -{}", ch),
                        }
                    }
                    continue;
                } else if tok.starts_with('#') && tok.len() > 1 {
                    // Include tag
                    include_tags.push(tok[1..].to_string());
                    continue;
                } else if tok.starts_with('!') && tok.len() > 1 {
                    // Exclude tag
                    exclude_tags.push(tok[1..].to_string());
                    continue;
                } else if !prefix_set {
                    prefix_str = tok.to_string();
                    prefix_set = true;
                    continue;
                } else {
                    println!("Unexpected argument '{}' - use #tag to include or !tag to exclude", tok);
                    return Ok(false);
                }
            }
            
            if only_prefix && only_keys {
                println!("Cannot use -c and -k together");
                return Ok(false);
            }
            
            let (list_prefix, list_delim) = {
                let state_ref = state.borrow();
                let mut p = state_ref.cwd.clone();
                if !prefix_str.is_empty() {
                    p.push_str(&prefix_str);
                }
                let d = if recursive {
                    None
                } else if let Some(cd) = custom_delim {
                    Some(cd)
                } else {
                    Some(state_ref.delim)
                };
                (p, d)
            };
            
            let state_ref = state.borrow();
            
            // Create tag filter config if tags were specified
            let tag_config = if !include_tags.is_empty() || !exclude_tags.is_empty() {
                Some(TagFilterConfig {
                    include_tags,
                    exclude_tags,
                    mode: TagMode::And,
                })
            } else {
                None
            };
            
            // Try to create composed filter if both search and tag filters are needed
            let has_search_filter = !include_queries.is_empty() || !exclude_queries.is_empty();
            let has_tag_filter = tag_config.is_some();
            
            let filter: Option<Box<dyn foliant::multi_list::LazyShardFilter<V, _>>> = if has_search_filter && has_tag_filter {
                // Check if search index exists
                let has_search_index = state_ref.db.shards().iter().any(|s| s.has_search_index());
                if !has_search_index && has_search_filter {
                    println!("Warning: No search index found. Run 'foliant build-search' to enable search.");
                }
                
                // Try to compose both filters
                let mut filters: Vec<Box<dyn foliant::multi_list::LazyShardFilter<V, _>>> = Vec::new();
                
                if has_search_index && has_search_filter {
                    // Create search filter with multiple included/excluded terms
                    let search_filter = if include_queries.is_empty() && exclude_queries.len() == 1 {
                        // Simple exclusion filter
                        LazySearchFilter::new("".to_string())
                            .exclude(exclude_queries[0].clone())
                    } else if include_queries.len() == 1 && exclude_queries.is_empty() {
                        // Simple inclusion filter
                        LazySearchFilter::new(include_queries[0].clone())
                    } else {
                        // Complex filter with multiple terms
                        LazySearchFilter::with_terms(include_queries.clone(), exclude_queries.clone())
                    };
                    filters.push(Box::new(search_filter) as Box<dyn foliant::multi_list::LazyShardFilter<V, _>>);
                }
                
                if let Some(cfg) = tag_config.clone() {
                    filters.push(Box::new(LazyTagFilter::from_config(&cfg)) as Box<dyn foliant::multi_list::LazyShardFilter<V, _>>);
                }
                
                // Create the appropriate filter
                if filters.len() > 1 {
                    // Use ComposedFilter for multiple filters
                    Some(Box::new(ComposedFilter::new(filters)) as Box<dyn foliant::multi_list::LazyShardFilter<V, _>>)
                } else if filters.len() == 1 {
                    // Use the single filter
                    filters.into_iter().next()
                } else {
                    None
                }
            } else if has_search_filter {
                // Only search filter
                let has_search_index = state_ref.db.shards().iter().any(|s| s.has_search_index());
                if !has_search_index {
                    println!("Warning: No search index found. Run 'foliant build-search' to enable search.");
                    None
                } else {
                    // Create search filter with multiple included/excluded terms
                    let search_filter = if include_queries.is_empty() && exclude_queries.len() == 1 {
                        // Simple exclusion filter
                        LazySearchFilter::new("".to_string())
                            .exclude(exclude_queries[0].clone())
                    } else if include_queries.len() == 1 && exclude_queries.is_empty() {
                        // Simple inclusion filter
                        LazySearchFilter::new(include_queries[0].clone())
                    } else {
                        // Complex filter with multiple terms
                        LazySearchFilter::with_terms(include_queries, exclude_queries)
                    };
                    Some(Box::new(search_filter) as Box<dyn foliant::multi_list::LazyShardFilter<V, _>>)
                }
            } else if let Some(cfg) = tag_config {
                // Only tag filter
                Some(Box::new(LazyTagFilter::from_config(&cfg)) as Box<dyn foliant::multi_list::LazyShardFilter<V, _>>)
            } else {
                None
            };

            // Use MultiShardListStreamer with optional filter
            let mut stream: Box<dyn Streamer<Item = Entry<_>, Cursor = Vec<u8>>> = if let Some(flt) = filter {
                match MultiShardListStreamer::new_with_filter(
                    &state_ref.db,
                    list_prefix.as_bytes().to_vec(),
                    list_delim.map(|c| c as u8),
                    Some(flt),
                ) {
                    Ok(s) => Box::new(s),
                    Err(e) => {
                        println!("Error creating filtered stream: {}", e);
                        return Ok(false);
                    }
                }
            } else {
                match state_ref.db.list(&list_prefix, list_delim) {
                    Ok(s) => s,
                    Err(e) => {
                        println!("Error listing {}: {}", list_prefix, e);
                        return Ok(false);
                    }
                }
            };
            
            handle_print_result(print_entries(
                &list_prefix,
                &mut stream,
                only_prefix,
                only_keys,
                abort_rx,
            ));
            Ok(false)
        }
        "tags" => {
            // No arguments expected for tags command
            if parts.next().is_some() {
                println!("tags command doesn't take any arguments");
                return Ok(false);
            }
            
            let state_ref = state.borrow();
            let prefix_opt = if state_ref.cwd.is_empty() {
                None
            } else {
                Some(state_ref.cwd.as_str())
            };
            
            match state_ref.db.list_tags(prefix_opt) {
                Ok(mut tag_stream) => {
                    while let Some((tag, count)) = tag_stream.next() {
                        println!(
                            "🏷️  \x1b[35m{:<32}\x1b[0m \x1b[90m{:>12}\x1b[0m",
                            tag, count
                        );
                    }
                }
                Err(e) => {
                    println!("Error listing tags: {}", e);
                }
            }
            Ok(false)
        }
        "shards" => {
            let state_ref = state.borrow();
            let shards = state_ref.db.shards();
            println!("{} shard(s) loaded", shards.len());
            for (i, shard) in shards.iter().enumerate() {
                println!(
                    "shard {}: {}, {} keys, common_prefix=\"{}\"",
                    i,
                    shard.idx_path().display(),
                    shard.len(),
                    shard.common_prefix(),
                );
            }
            Ok(false)
        }
        "search" => {
            let query = match parts.next() {
                Some(q) => q,
                None => {
                    println!("Usage: search <query>");
                    return Ok(false);
                }
            };
            let state_ref = state.borrow();
            let cwd_owned = state_ref.cwd.clone();
            
            // Check if search index exists
            let has_search_index = state_ref.db.shards().iter().any(|s| s.has_search_index());
            if !has_search_index {
                println!("Warning: No search index found. Run 'foliant build-search' to enable search.");
                return Ok(false);
            }
            
            // Create search filter
            let filter: Box<dyn foliant::multi_list::LazyShardFilter<V, _>> = 
                Box::new(LazySearchFilter::new(query.to_string()));
            
            let mut stream = match state_ref.db.list_with_filter(&cwd_owned, None, filter) {
                Ok(s) => s,
                Err(e) => {
                    println!("Error running search: {}", e);
                    return Ok(false);
                }
            };
            handle_print_result(print_entries(
                cwd_owned.as_str(),
                &mut stream,
                false,
                true,
                abort_rx,
            ));
            Ok(false)
        }
        "val" => {
            let id_str = match parts.next() {
                Some(i) => i,
                None => {
                    let shard_count = state.borrow().db.shards().len();
                    if shard_count > 1 {
                        println!("Usage: val <numeric_id> [shard_index]");
                        println!("       val <shard_index>:<numeric_id>");
                        println!("Available shards: 0-{}", shard_count - 1);
                    } else {
                        println!("Usage: val <numeric_id>");
                    }
                    return Ok(false);
                }
            };
            
            let state_ref = state.borrow();
            let shard_count = state_ref.db.shards().len();
            
            // Parse input - could be "id" or "shard:id" or "id shard"
            let (shard_idx, ptr) = if let Some((shard_str, id_str)) = id_str.split_once(':') {
                // Format: "shard:id"
                match (shard_str.parse::<usize>(), id_str.parse::<u64>()) {
                    (Ok(s), Ok(id)) => (Some(s), id),
                    _ => {
                        println!("Invalid format. Use: val <shard>:<id>");
                        return Ok(false);
                    }
                }
            } else if let Ok(id) = id_str.parse::<u64>() {
                // Check if there's a second argument for shard
                if let Some(shard_str) = parts.next() {
                    match shard_str.parse::<usize>() {
                        Ok(s) => (Some(s), id),
                        Err(_) => {
                            println!("Invalid shard index: {}", shard_str);
                            return Ok(false);
                        }
                    }
                } else {
                    (None, id)
                }
            } else {
                println!("Invalid ID format");
                return Ok(false);
            };
            
            // Get the entry
            let entry_opt = if let Some(shard) = shard_idx {
                if shard >= shard_count {
                    println!("Shard index {} out of range (0-{})", shard, shard_count - 1);
                    return Ok(false);
                }
                state_ref.db.get_key_from_shard(shard, ptr)?
            } else if shard_count == 1 {
                // If only one shard, default to shard 0
                state_ref.db.get_key_from_shard(0, ptr)?
            } else {
                // Multiple shards require explicit shard index
                println!("Multiple shards loaded. Please specify shard index:");
                println!("  val <id> <shard>");
                println!("  val <shard>:<id>");
                println!("Available shards: 0-{}", shard_count - 1);
                return Ok(false);
            };
            
            match entry_opt {
                Some(Entry::Key(key, actual_ptr, val_opt)) => {
                    let shard_info = if let Some(s) = shard_idx {
                        format!(" \x1b[93m[shard {}]\x1b[0m", s)
                    } else {
                        String::new()
                    };
                    
                    if let Some(val) = val_opt {
                        let val_str = serde_json::to_string(&val)?;
                        println!(
                            "📄 {} \x1b[90m#{}\x1b[0m{} \x1b[2m{}\x1b[0m",
                            key, actual_ptr, shard_info, val_str
                        );
                    } else {
                        println!("📄 {} \x1b[90m#{}\x1b[0m{}", key, actual_ptr, shard_info);
                    }
                }
                _ => {
                    if let Some(s) = shard_idx {
                        println!("No key found for id {} in shard {}", ptr, s);
                    } else {
                        println!("No key found for id {}", ptr);
                    }
                }
            }
            Ok(false)
        }
        "cd" => {
            let arg = parts.next().unwrap_or("");
            let mut state_mut = state.borrow_mut();
            let delim_char = state_mut.delim;
            if arg.is_empty() {
                state_mut.cwd.clear();
            } else if arg == ".." {
                if let Some(end) = state_mut.cwd.rfind(|c| c != delim_char) {
                    if let Some(idx) = state_mut.cwd[..=end].rfind(delim_char) {
                        state_mut.cwd.truncate(idx + 1);
                    } else {
                        state_mut.cwd.clear();
                    }
                } else {
                    state_mut.cwd.clear();
                }
            } else {
                if state_mut.cwd.is_empty() {
                    state_mut.cwd = arg.to_string();
                } else {
                    state_mut.cwd.push_str(arg);
                }
            }
            Ok(false)
        }
        "pwd" => {
            println!("{}", state.borrow().cwd);
            Ok(false)
        }
        "help" => {
            println!("{}", help_text);
            Ok(false)
        }
        "exit" | "quit" => Ok(true),
        _ => {
            println!("Unknown command: {}", cmd);
            Ok(false)
        }
    };
    
    // Print execution time if debug mode is enabled
    if let Some(start) = start_time {
        let elapsed = start.elapsed();
        println!("\x1b[90mCommand execution time: {:.3} ms\x1b[0m", elapsed.as_secs_f64() * 1000.0);
    }
    
    result
}

pub fn run_shell<V: DeserializeOwned + Serialize + 'static>(
    db: Database<V>,
    delim: char,
    debug: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Rc::new(RefCell::new(ShellState {
        db,
        delim,
        cwd: String::new(),
        debug,
    }));
    let helper = ShellHelper {
        state: Rc::clone(&state),
    };
    let mut rl = Editor::new()?;
    rl.set_helper(Some(helper));
    // Setup Ctrl-C handler: first press aborts listing, second within 2s exits shell
    let (sig_tx, sig_rx) = channel::<()>();
    let last_sig = Arc::new(AtomicU64::new(0));
    {
        let sig_tx = sig_tx.clone();
        let last_sig = Arc::clone(&last_sig);
        ctrlc::set_handler(move || {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let prev = last_sig.swap(now, Ordering::SeqCst);
            if now.saturating_sub(prev) < 2000 {
                std::process::exit(0);
            }
            let _ = sig_tx.send(());
        })?;
    }

    let num_entries = {
        let state_ref = state.borrow();
        state_ref.db.len()
    };
    println!("Database contains {} entries", num_entries);
    // Database handle will be borrowed from state as needed within command handlers
    // Show shell help text
    println!("{}", HELP_TEXT);

    loop {
        // Build prompt: virtual leading delimiter; show cwd between delimiters
        let prompt = format!("{}> ", state.borrow().cwd);

        match rl.readline(&prompt) {
            Ok(line) => {
                let raw = line.trim();
                if raw.is_empty() {
                    continue;
                }
                rl.add_history_entry(raw);

                if handle_cmd(&state, HELP_TEXT, Some(&sig_rx), raw)? {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Handle double Ctrl-C at prompt: exit if within 2s
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let prev = last_sig.swap(now, Ordering::SeqCst);
                if now.saturating_sub(prev) < 2000 {
                    break;
                }
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

/// Execute a single shell command non-interactively.
pub fn run_shell_commands<V: DeserializeOwned + Serialize + 'static>(
    db: Database<V>,
    delim: char,
    debug: bool,
    commands: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Rc::new(RefCell::new(ShellState {
        db,
        delim,
        cwd: String::new(),
        debug,
    }));
    // Join remaining args into one command
    let line = commands.join(" ");
    // Dispatch without abort support
    let _ = handle_cmd(&state, HELP_TEXT, None, &line)?;
    Ok(())
}
