// Interactive shell navigation: REPL for browsing the index
// Maintains a current 'directory' prefix split by a delimiter
// Commands: ls, cd, pwd, help, exit/quit

use rustyline::{Editor, error::ReadlineError, completion::{Completer, Pair}, hint::Hinter, highlight::Highlighter, validate::Validator, Helper, Context};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashSet;

use foliant::{Database, Entry, Streamer};

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
    fn complete(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let before = &line[..pos];
        let start = before.rfind(' ').map(|i| i + 1).unwrap_or(0);
        let word = &before[start..];
        let mut candidates = Vec::new();
        if start == 0 {
            let cmds = ["cd", "ls", "pwd", "exit", "quit", "help"];
            for cmd in cmds {
                if cmd.starts_with(word) {
                    candidates.push(Pair { display: cmd.to_string(), replacement: cmd.to_string() });
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
        let mut prefix = cwd.clone();
        if !prefix.is_empty() && !prefix.ends_with(delim) {
            prefix.push(delim);
        }
        if !parent.is_empty() {
            prefix.push_str(parent);
            if !parent.ends_with(delim) {
                prefix.push(delim);
            }
        }
        let mut stream = state.db.list(&prefix, Some(delim));
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
                            candidates.push(Pair { display: rep.clone(), replacement: rep });
                        }
                    }
                }
                Entry::Key(s, _) if !suggest_dirs_only => {
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
                            candidates.push(Pair { display: rep.clone(), replacement: rep });
                        }
                    }
                }
                _ => {}
            }
        }
        Ok((start, candidates))
    }
}

impl<V: DeserializeOwned> Hinter for ShellHelper<V> {
    type Hint = String;
    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}
impl<V: DeserializeOwned> Highlighter for ShellHelper<V> {}
impl<V: DeserializeOwned> Validator for ShellHelper<V> {}

pub fn run_shell<V: DeserializeOwned + Serialize>(db: Database<V>, delim: char) -> Result<(), Box<dyn std::error::Error>> {
    let state = Rc::new(RefCell::new(ShellState { db, delim, cwd: String::new() }));
    let helper = ShellHelper { state: Rc::clone(&state) };
    let mut rl = Editor::new()?;
    rl.set_helper(Some(helper));

    loop {
        // Build prompt: virtual leading delimiter; show cwd between delimiters
        let (cwd, delim_char) = {
            let state_ref = state.borrow();
            (state_ref.cwd.clone(), state_ref.delim)
        };
        let prompt = if cwd.is_empty() {
            // At root: show single delimiter
            format!("{}> ", delim_char)
        } else {
            // Non-root: /cwd/
            format!("{}{}{}> ", delim_char, cwd, delim_char)
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line);

                let mut parts = line.split_whitespace();
                let cmd = parts.next().unwrap();

                match cmd {
                    "ls" => {
                        // Read-only borrow for state
                        let (prefix, delim) = {
                            let state_ref = state.borrow();
                            // Build listing prefix
                            let mut p = state_ref.cwd.clone();
                            if !p.is_empty() && p.chars().last() != Some(state_ref.delim) {
                                p.push(state_ref.delim);
                            }
                            if let Some(target) = parts.next() {
                                if !target.is_empty() {
                                    p.push_str(target);
                                    if target.chars().last() != Some(state_ref.delim) {
                                        p.push(state_ref.delim);
                                    }
                                }
                            }
                            (p, state_ref.delim)
                        };

                        // Perform listing
                        let state_borrow = state.borrow();
                        let mut stream = state_borrow.db.list(&prefix, Some(delim));
                        let mut printed = 0usize;
                        while let Some(entry) = stream.next() {
                            match entry {
                                Entry::Key(s, val_opt) => {
                                    // print key, followed by optional CBOR-decoded JSON Value in dim color
                                    if let Some(val) = val_opt {
                                        let val_str = serde_json::to_string(&val)?;
                                        println!("📄 {} \x1b[2m{}\x1b[0m", s, val_str);
                                    } else {
                                        println!("📄 {}", s);
                                    }
                                }
                                Entry::CommonPrefix(s) => println!("📁 {}", s),
                            }
                            printed += 1;
                        }

                        if printed > 0 {
                            println!("\n{} {} listings", printed, if printed == 1 { "entry" } else { "entries" });
                        } else if let Some(target) = parts.next() {
                            println!("No entries found for prefix '{}'", target);
                        } else if !prefix.is_empty() {
                            println!("No entries found for prefix '{}'", prefix);
                        } else {
                            println!("No entries found");
                        }
                    }
                    "cd" => {
                        // Change current working prefix
                        let arg = parts.next().unwrap_or("");
                        let mut state_mut = state.borrow_mut();
                        let delim_char = state_mut.delim;
                        if arg == ".." {
                            // Up one level
                            if let Some(idx) = state_mut.cwd.rfind(delim_char) {
                                state_mut.cwd.truncate(idx);
                            } else {
                                state_mut.cwd.clear();
                            }
                        } else if arg.is_empty() {
                            // Root
                            state_mut.cwd.clear();
                        } else {
                            // Append the specified segment to the current prefix
                            let segment = arg.trim_matches(delim_char);
                            if state_mut.cwd.is_empty() {
                                state_mut.cwd = segment.to_string();
                            } else {
                                state_mut.cwd.push(delim_char);
                                state_mut.cwd.push_str(segment);
                            }
                        }
                    }
                    "pwd" => {
                        println!("{}", state.borrow().cwd);
                    }
                    "help" => {
                        println!("Available commands: ls [path], cd [dir], pwd, exit, quit, help");
                    }
                    "exit" | "quit" => break,
                    _ => println!("Unknown command: {}", cmd),
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}