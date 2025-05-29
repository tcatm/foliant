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
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{channel, Receiver},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

/// Help text for interactive shell commands and Ctrl-C behavior.
const HELP_TEXT: &str = "\
Available commands:
  ls [-rck] [-d DELIM] [prefix]   list entries
    -r       recursive (no delimiter grouping)
    -c       only directories (common prefixes)
    -k       only keys
    -d <c>   one-off custom delimiter character
  cd [dir]                        change directory
  find <regex>                    search entries matching regex
  tags [-a|-o] <tag1> [tag2...]   list entries with tags (default: all)
    -a       match all tags (AND, default)
    -o       match any tag (OR)
  val <id>                        lookup key by raw pointer ID
  pwd                             print working directory
  exit, quit                      exit shell
  help                            show this help

Ctrl-C once aborts a running ls; twice within 2s exits the shell
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
            Some(Entry::CommonPrefix(s)) if !only_keys => {
                let s = s.strip_prefix(prefix).unwrap_or(&s);
                // Color directory listings in blue
                println!("\x1b[34m📁 {}\x1b[0m", s);
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

struct ShellState<V: DeserializeOwned> {
    db: Database<V>,
    delim: char,
    cwd: String,
}

struct ShellHelper<V: DeserializeOwned> {
    state: Rc<RefCell<ShellState<V>>>,
}

impl<V: DeserializeOwned> Helper for ShellHelper<V> {}

impl<V: DeserializeOwned> Completer for ShellHelper<V> {
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
            let cmds = ["cd", "ls", "find", "pwd", "val", "tags", "exit", "quit", "help"];
            for &cmd in &cmds {
                if cmd.starts_with(word) {
                    // Commands that take an argument get a trailing space
                    let replacement = if ["cd", "ls", "find", "val", "tags"].contains(&cmd) {
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
                Entry::CommonPrefix(s) => {
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
impl<V: DeserializeOwned> Hinter for ShellHelper<V> {
    // No inline hints
    type Hint = String;
}
// Use default highlighter (no-op)
impl<V: DeserializeOwned> Highlighter for ShellHelper<V> {}

impl<V: DeserializeOwned> Validator for ShellHelper<V> {}

/// Internal helper to process one shell command; returns true to exit shell
fn handle_cmd<V: DeserializeOwned + Serialize>(
    state: &Rc<RefCell<ShellState<V>>>,
    help_text: &str,
    abort_rx: Option<&Receiver<()>>,
    raw: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let line = raw.trim();
    if line.is_empty() {
        return Ok(false);
    }
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap();
    match cmd {
        "ls" => {
            let mut recursive = false;
            let mut only_prefix = false;
            let mut only_keys = false;
            let mut custom_delim: Option<char> = None;
            let mut prefix_str = String::new();
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
                } else {
                    prefix_str = tok.to_string();
                    break;
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
            let mut stream = match state_ref.db.list(&list_prefix, list_delim) {
                Ok(s) => s,
                Err(e) => {
                    println!("Error listing {}: {}", list_prefix, e);
                    return Ok(false);
                }
            };
            match print_entries(&list_prefix, &mut stream, only_prefix, only_keys, abort_rx) {
                Ok(PrintResult::Aborted) | Err(_) => {}
                Ok(PrintResult::Count(printed)) if printed > 0 => {
                    println!("\n{} {} listed", printed, if printed == 1 { "entry" } else { "entries" });
                }
                _ => {}
            }
        }
        "tags" => {
            let mut tag_mode = TagMode::And;
            let mut tags = Vec::new();
            for tok in parts {
                if tok == "-a" {
                    tag_mode = TagMode::And;
                } else if tok == "-o" {
                    tag_mode = TagMode::Or;
                } else if tok.starts_with('-') {
                    println!("Unknown flag: {}", tok);
                    continue;
                } else {
                    tags.push(tok);
                }
            }
            if tags.is_empty() {
                println!("Usage: tags [-a|-o] <tag1> [tag2...]");
                return Ok(false);
            }
            let tags_refs: Vec<&str> = tags.iter().map(|s| &**s).collect();
            let cwd_owned = state.borrow().cwd.clone();
            let prefix_opt = if cwd_owned.is_empty() { None } else { Some(cwd_owned.as_str()) };
            let state_ref = state.borrow();
            let mut stream = match state_ref.db.list_by_tags(&tags_refs, tag_mode, prefix_opt) {
                Ok(s) => s,
                Err(e) => {
                    println!("Error listing by tags: {}", e);
                    return Ok(false);
                }
            };
            match print_entries(&state_ref.cwd, &mut stream, false, true, abort_rx) {
                Ok(PrintResult::Aborted) | Err(_) => {}
                _ => {}
            }
        }
        "find" => {
            let pattern = match parts.next() {
                Some(p) => p,
                None => {
                    println!("Usage: find <regex>");
                    return Ok(false);
                }
            };
            let state_ref = state.borrow();
            let cwd_owned = state_ref.cwd.clone();
            let prefix_opt = if cwd_owned.is_empty() { None } else { Some(cwd_owned.as_str()) };
            let mut stream = match state_ref.db.grep(prefix_opt, pattern) {
                Ok(s) => s,
                Err(e) => {
                    println!("Error running find: {}", e);
                    return Ok(false);
                }
            };
            match print_entries(cwd_owned.as_str(), &mut stream, false, true, abort_rx) {
                Ok(PrintResult::Aborted) | Err(_) => {}
                _ => {}
            }
        }
        "val" => {
            let id_str = match parts.next() {
                Some(i) => i,
                None => {
                    println!("Usage: val <numeric_id>");
                    return Ok(false);
                }
            };
            match id_str.parse::<u64>() {
                Ok(id) => match state.borrow().db.get_key(id)? {
                    Some(Entry::Key(key, ptr, val_opt)) => {
                        if let Some(val) = val_opt {
                            let val_str = serde_json::to_string(&val)?;
                            println!("📄 {} \x1b[90m#{}\x1b[0m \x1b[2m{}\x1b[0m", key, ptr, val_str);
                        } else {
                            println!("📄 {} \x1b[90m#{}\x1b[0m", key, ptr);
                        }
                    }
                    _ => println!("No key found for id {}", id),
                },
                Err(_) => println!("Usage: val <numeric_id>"),
            }
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
        }
        "pwd" => {
            println!("{}", state.borrow().cwd);
        }
        "help" => {
            println!("{}", help_text);
        }
        "exit" | "quit" => return Ok(true),
        _ => println!("Unknown command: {}", cmd),
    }
    Ok(false)
}

pub fn run_shell<V: DeserializeOwned + Serialize>(
    db: Database<V>,
    delim: char,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Rc::new(RefCell::new(ShellState {
        db,
        delim,
        cwd: String::new(),
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
pub fn run_shell_commands<V: DeserializeOwned + Serialize>(
    db: Database<V>,
    delim: char,
    commands: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let state = Rc::new(RefCell::new(ShellState { db, delim, cwd: String::new() }));
    // Join remaining args into one command
    let line = commands.join(" ");
    println!("> {}", line);
    // Dispatch without abort support
    let _ = handle_cmd(&state, HELP_TEXT, None, &line)?;
    Ok(())
}

