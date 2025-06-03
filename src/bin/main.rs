use clap::{Parser, Subcommand};
use foliant::IndexError;
use foliant::SegmentInfo;
use foliant::Streamer;
use foliant::TagMode;
use foliant::{Database, DatabaseBuilder, Entry, TagIndexBuilder};
use fst::map::Map;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde_json::{self, Value};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod shell;
use shell::{run_shell, run_shell_commands};

// Cross-platform resident set size (RSS) in KB; uses getrusage on UNIX.
#[cfg(unix)]
fn get_rss_kb() -> Option<usize> {
    use libc::{getrusage, rusage, RUSAGE_SELF};
    unsafe {
        let mut usage: rusage = std::mem::zeroed();
        if getrusage(RUSAGE_SELF, &mut usage) != 0 {
            return None;
        }
        #[cfg(target_os = "linux")]
        {
            // Linux: ru_maxrss is in kilobytes
            Some(usage.ru_maxrss as usize)
        }
        #[cfg(target_os = "macos")]
        {
            // macOS: ru_maxrss is in bytes
            Some((usage.ru_maxrss as usize) / 1024)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            // Other UNIX: assume bytes
            Some((usage.ru_maxrss as usize) / 1024)
        }
    }
}

#[cfg(not(unix))]
fn get_rss_kb() -> Option<usize> {
    None
}
/// A simple CLI for building and querying a Trie index.
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build a new trie index from input lines
    Index {
        /// Path to write the serialized index
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// Input file (defaults to stdin)
        #[arg(long, value_name = "INPUT")]
        input: Option<PathBuf>,
        /// Interpret each line as JSON and extract this field as the key
        #[arg(short, long, value_name = "KEYNAME")]
        json: Option<String>,
        /// JSON field name containing array-of-strings tags (two-pass; run tag-index after indexing)
        #[arg(long, alias = "tag-index", value_name = "TAGFIELD")]
        tag_field: Option<String>,
        /// Prefix to prepend to all keys
        #[arg(short, long, value_name = "PREFIX")]
        prefix: Option<String>,
    },
    /// Generate or update the tag index (.tags) for an existing database by scanning JSON payloads
    TagIndex {
        /// Path to the serialized index (file or directory)
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// JSON field name containing array-of-strings tags
        #[arg(long, value_name = "TAGFIELD")]
        tag_field: String,
    },
    List {
        /// Path to the serialized index
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// Comma-separated tags to include or exclude (prefix with '-' or '!' to exclude)
        #[arg(long, value_name = "TAGS")]
        tags: Option<String>,
        /// Combine positive tags with AND or OR logic (applied before exclusion)
        #[arg(long, value_name = "TAG_MODE", default_value = "and")]
        tag_mode: TagMode,
        /// Prefix to list (default is empty)
        #[arg(value_name = "PREFIX", default_value = "")]
        prefix: String,
        /// Optional delimiter character for grouping
        #[arg(short, long, value_name = "DELIM")]
        delimiter: Option<char>,
    },
    /// Interactive shell for browsing the index (or run commands non-interactively)
    Shell {
        /// Path to the serialized index
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// Delimiter character for grouping (default: '/')
        #[arg(short, long, value_name = "DELIM", default_value = "/")]
        delimiter: char,
        /// Shell commands to execute non-interactively (skips REPL)
        #[arg(value_name = "CMD", num_args = 0.., last = true)]
        commands: Vec<String>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Helper to open either a single database or a directory of shards, with optional delimiter
    fn load_db<V>(path: &std::path::PathBuf) -> Result<Database<V>, IndexError>
    where
        V: DeserializeOwned + 'static,
    {
        let db = Database::<V>::open(path)?;
        Ok(db)
    }

    // Helper to build a tag index with a progress bar; returns elapsed time
    fn run_tag_index(
        index: &PathBuf,
        tag_field: &str,
    ) -> Result<Duration, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let idx_path = index.with_extension("idx");
        let idx_file = File::open(&idx_path)?;
        let idx_mmap = unsafe { Mmap::map(&idx_file)? };
        let idx_map = Map::new(idx_mmap)?;
        let total = idx_map.len() as u64;
        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] [{eta_precise}] {pos}/{len} ({percent}%) {msg}"
            )?
            .progress_chars("#>-")
        );
        let pb_clone = pb.clone();
        TagIndexBuilder::build_index(index, tag_field, Some(Arc::new(move |_| pb_clone.inc(1))))?;
        pb.finish();
        Ok(start.elapsed())
    }

    match cli.command {
        Commands::Index {
            index,
            input,
            json,
            tag_field,
            prefix,
        } => {
            // Build the database on-disk via builder, measuring throughput
            let mut builder = DatabaseBuilder::<Value>::new(&index)?;
            // Setup progress bar using indicatif
            let total_bytes = input
                .as_ref()
                .and_then(|p| std::fs::metadata(p).ok())
                .map(|m| m.len());
            let pb = if let Some(total) = total_bytes {
                let pb = ProgressBar::new(total);
                pb.set_style(
                    ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] [{eta_precise}] {bytes}/{total_bytes} ({percent}%) {msg}")?
                        .progress_chars("#>-")
                );
                pb
            } else {
                let pb = ProgressBar::new_spinner();
                pb.enable_steady_tick(Duration::from_millis(100));
                let style = ProgressStyle::with_template("{spinner:.green} {bytes} bytes {msg}")?;
                pb.set_style(style);
                pb
            };
            let reader: Box<dyn BufRead> = if let Some(input_path) = input {
                Box::new(BufReader::new(File::open(input_path)?))
            } else {
                Box::new(BufReader::new(io::stdin()))
            };
            let start = Instant::now();
            let report_interval = Duration::from_millis(20);
            let mut last_report = Instant::now();
            let mut entries = 0usize;
            let mut bytes_in = 0usize;
            // Capture any JSON parsing errors to exit gracefully after saving index
            let mut parse_error: Option<String> = None;
            for line in reader.lines() {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                bytes_in += line.len();
                entries += 1;

                if let Some(ref keyname) = json {
                    // Attempt to parse JSON and extract the key; on error, record and break
                    let res = (|| -> Result<(), String> {
                        let mut jv: serde_json::Value = serde_json::from_str(&line)
                            .map_err(|e| format!("JSON parse error: {}", e))?;
                        let obj = jv
                            .as_object_mut()
                            .ok_or_else(|| "expected JSON object per line".to_string())?;
                        let key_val = obj
                            .remove(keyname)
                            .ok_or_else(|| format!("missing key field '{}'", keyname))?;
                        let key_str = key_val
                            .as_str()
                            .ok_or_else(|| format!("key field '{}' is not a string", keyname))?;
                        // Prepend CLI prefix if provided
                        let mut full_key = String::with_capacity(
                            prefix.as_ref().map_or(0, |p| p.len()) + key_str.len(),
                        );
                        if let Some(pref) = prefix.as_ref() {
                            full_key.push_str(pref);
                        }
                        full_key.push_str(key_str);
                        builder.insert(&full_key, Some(jv));
                        Ok(())
                    })();
                    if let Err(err) = res {
                        parse_error = Some(err);
                        break;
                    }
                } else {
                    // Prepend CLI prefix if provided
                    let mut full_key =
                        String::with_capacity(prefix.as_ref().map_or(0, |p| p.len()) + line.len());
                    if let Some(pref) = prefix.as_ref() {
                        full_key.push_str(pref);
                    }
                    full_key.push_str(&line);
                    builder.insert(&full_key, None);
                }

                // periodic throttled progress update
                let now = Instant::now();
                if now.duration_since(last_report) >= report_interval {
                    let elapsed = now.duration_since(start).as_secs_f64();
                    let eps = if elapsed > 0.0 {
                        (entries as f64 / elapsed).round()
                    } else {
                        0.0
                    };
                    pb.set_position(bytes_in as u64);
                    let mem_kb = get_rss_kb().unwrap_or(0);
                    let mem_str = if mem_kb >= 1_048_576 {
                        format!("{:.2} GB", mem_kb as f64 / 1_048_576.0)
                    } else if mem_kb >= 1024 {
                        format!("{:.2} MB", mem_kb as f64 / 1024.0)
                    } else {
                        format!("{} KB", mem_kb)
                    };
                    pb.set_message(format!(
                        "{} entries, {:.0}/s, mem {}",
                        entries, eps, mem_str
                    ));
                    last_report = now;
                }
            }
            // finish progress bar
            pb.finish();
            let duration = start.elapsed();
            // Write out database files (.idx and .payload)
            // show progress for writing the index
            let idx_pb = ProgressBar::new(entries as u64);
            idx_pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] [{eta_precise}] \
                     {pos}/{len} ({percent}%, {per_sec:.0}) {msg}",
                )?
                .progress_chars("#>-"),
            );
            idx_pb.set_message("Writing index");
            let write_start = Instant::now();
            let idx_pb2 = idx_pb.clone();
            builder = builder.with_write_progress(move || idx_pb2.inc(1));
            // print segment count and size before final merge
            builder = builder.with_segment_stats(|stats: &[SegmentInfo]| {
                let count = stats.len();
                let sum: usize = stats.iter().map(|s| s.size_bytes).sum();
                let avg = if count > 0 {
                    sum as f64 / count as f64
                } else {
                    0.0
                };
                let stddev = if count > 0 {
                    (stats
                        .iter()
                        .map(|s| {
                            let diff = s.size_bytes as f64 - avg;
                            diff * diff
                        })
                        .sum::<f64>()
                        / count as f64)
                        .sqrt()
                } else {
                    0.0
                };
                eprintln!(
                    "Merging {} segment(s): avg size {:.0} bytes, stddev {:.0} bytes",
                    count, avg, stddev
                );
            });
            builder.close()?;
            idx_pb.finish();
            // If a JSON parsing error occurred, exit now after saving indexed data
            if let Some(err_msg) = parse_error {
                eprintln!("Error processing entry {}: {}", entries, err_msg);
                std::process::exit(1);
            }
            let write_duration = write_start.elapsed();
            // Metrics
            let secs = duration.as_secs_f64();
            let eps = entries as f64 / secs;
            let bps = bytes_in as f64 / secs;
            let idx_path = index.with_extension("idx");
            let payload_path = index.with_extension("payload");
            let idx_size = std::fs::metadata(&idx_path)?.len();
            let payload_size = std::fs::metadata(&payload_path)?.len();
            eprintln!(
                "Indexed {} entries ({} bytes in) in {:.3} ms: {:.0} entries/s, {:.0} bytes/s",
                entries,
                bytes_in,
                secs * 1000.0,
                eps,
                bps
            );
            eprintln!(
                "Wrote index file {} bytes and payload file {} bytes in {:.3} ms",
                idx_size,
                payload_size,
                write_duration.as_secs_f64() * 1000.0
            );
            // report memory usage after build
            if let Some(mem_after) = get_rss_kb() {
                eprintln!("Memory usage: {} KB", mem_after);
            }

            // Generate tag index if requested (two-pass)
            if let Some(tf) = tag_field {
                let ti_dur = run_tag_index(&index, &tf)?;
                eprintln!(
                    "Tag index generated in {:.3} ms",
                    ti_dur.as_secs_f64() * 1000.0
                );
            }
        }
        Commands::TagIndex { index, tag_field } => {
            let dur = run_tag_index(&index, &tag_field)?;
            eprintln!(
                "Tag index generated in {:.3} ms",
                dur.as_secs_f64() * 1000.0
            );
        }
        Commands::List {
            index,
            tags,
            tag_mode,
            prefix,
            delimiter,
        } => {
            // Open read-only database or sharded directory via mmap
            let load_start = Instant::now();
            let db_handle: Database<Value> = load_db(&index)?;
            let load_duration = load_start.elapsed();
            // Choose tag-filtered or plain listing
            let stream_start = Instant::now();
            let mut stream = if let Some(tag_strs) = tags.as_deref() {
                // Split into include/exclude sets: prefix '-' or '!' to exclude
                let mut include_tags = Vec::new();
                let mut exclude_tags = Vec::new();
                for raw in tag_strs.split(',') {
                    if let Some(t) = raw.strip_prefix('-') {
                        exclude_tags.push(t);
                    } else if let Some(t) = raw.strip_prefix('!') {
                        exclude_tags.push(t);
                    } else if let Some(t) = raw.strip_prefix('+') {
                        include_tags.push(t);
                    } else if !raw.is_empty() {
                        include_tags.push(raw);
                    }
                }
                db_handle.list_by_tags(&include_tags, &exclude_tags, tag_mode, Some(&prefix))?
            } else {
                db_handle.list(&prefix, delimiter)?
            };
            let stream_duration = stream_start.elapsed();
            let mut printed = 0usize;
            let list_start = Instant::now();

            while let Some(entry) = stream.next() {
                match entry {
                    Entry::Key(s, _ptr, val_opt) => {
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

            let list_duration = list_start.elapsed();
            // Output combined metrics
            let total_duration = load_start.elapsed();
            eprintln!(
                "\nLoad: {:.3} ms, Stream: {:.3} ms, List: {:.3} ms, Total: {:.3} ms, Printed {} entries",
                load_duration.as_secs_f64() * 1000.0,
                stream_duration.as_secs_f64() * 1000.0,
                list_duration.as_secs_f64() * 1000.0,
                total_duration.as_secs_f64() * 1000.0,
                printed,
            );
        }
        Commands::Shell {
            index,
            delimiter,
            commands,
        } => {
            // Open single DB or sharded directory for interactive shell or batch commands
            let db_handle: Database<Value> = load_db(&index)?;
            if commands.is_empty() {
                run_shell(db_handle, delimiter)?;
            } else {
                run_shell_commands(db_handle, delimiter, &commands)?;
            }
        }
    }

    Ok(())
}
