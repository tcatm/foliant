use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Instant, Duration};
use foliant::{Database, DatabaseBuilder, Entry};
use foliant::Streamer;
use serde_json::{self, Value};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

mod shell;
use shell::run_shell;

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
fn get_rss_kb() -> Option<usize> { None }
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
    },
    /// List entries in an existing index
    List {
        /// Path to the serialized index
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// Prefix to list (default is empty)
        #[arg(value_name = "PREFIX", default_value = "")]
        prefix: String,
        /// Optional delimiter character for grouping
        #[arg(short, long, value_name = "DELIM")]
        delimiter: Option<char>,
    },
    /// Interactive shell for browsing the index
    Shell {
        /// Path to the serialized index
        #[arg(short, long, value_name = "FILE")]
        index: PathBuf,
        /// Delimiter character for grouping
        #[arg(short, long, value_name = "DELIM")]
        delimiter: char,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Index { index, input, json } => {
            // Build the database on-disk via builder, measuring throughput
            let mut builder = DatabaseBuilder::<Value>::new(&index)?;
            // Setup progress bar using indicatif
            let total_bytes = input.as_ref().and_then(|p| std::fs::metadata(p).ok()).map(|m| m.len());
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
                        let obj = jv.as_object_mut()
                            .ok_or_else(|| "expected JSON object per line".to_string())?;
                        let key_val = obj.remove(keyname)
                            .ok_or_else(|| format!("missing key field '{}'", keyname))?;
                        let key_str = key_val.as_str()
                            .ok_or_else(|| format!("key field '{}' is not a string", keyname))?;
                        builder.insert(key_str, Some(jv));
                        Ok(())
                    })();
                    if let Err(err) = res {
                        parse_error = Some(err);
                        break;
                    }
                } else {
                    builder.insert(&line, None);
                }

                // periodic throttled progress update
                let now = Instant::now();
                if now.duration_since(last_report) >= report_interval {
                    let elapsed = now.duration_since(start).as_secs_f64();
                    let eps = if elapsed > 0.0 { (entries as f64 / elapsed).round() } else { 0.0 };
                    pb.set_position(bytes_in as u64);
                    pb.set_message(format!("{} entries, {:.0}/s, mem {}KB", entries, eps, get_rss_kb().unwrap_or(0)));
                    last_report = now;
                }
            }
            // finish progress bar
            pb.finish();
            let duration = start.elapsed();
            // Write out database files (.idx and .payload)
            eprintln!("Writing index...");
            let write_start = Instant::now();
            builder.close()?;
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
        }
        Commands::List { index, prefix, delimiter } => {
            // Open read-only database via mmap
            let load_start = Instant::now();
            let db = Database::<Value>::open(&index)?;
            let load_duration = load_start.elapsed();
            // Stream and print entries with realtime progress
            let stream_start = Instant::now();
            let mut stream = db.list(&prefix, delimiter);
            let stream_duration = stream_start.elapsed();
            let mut printed = 0usize;
            let list_start = Instant::now();

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
        Commands::Shell { index, delimiter } => {
            let db = Database::<Value>::open(&index)?;
            run_shell(db, delimiter)?;
        }
    }

    Ok(())
}