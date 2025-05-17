use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::{Instant, Duration};
// On-disk index support moved to library

use index::{Entry, Trie};

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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Index { index, input } => {
            // Build the trie, measuring throughput
            let mut trie = Trie::new();
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
            for line in reader.lines() {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                bytes_in += line.len();
                entries += 1;
                trie.insert(&line);
                // periodic progress report
                let now = Instant::now();
                if now.duration_since(last_report) >= report_interval {
                    let total = now.duration_since(start);
                    let secs = total.as_secs_f64();
                    let eps = entries as f64 / secs;
                    let bps = bytes_in as f64 / secs;
                    // Carriage-return progress line
                    eprint!(
                        "\rProgress: {} entries, {} bytes, elapsed {:.3} ms, {:.0} entries/s, {:.0} bytes/s",
                        entries,
                        bytes_in,
                        secs * 1000.0,
                        eps,
                        bps
                    );
                    io::stderr().flush()?;
                    last_report = now;
                }
            }
            // finish progress line
            eprintln!();
            let duration = start.elapsed();
            // Serialize compressed radix trie to binary
            let write_start = Instant::now();
            let mut writer = io::BufWriter::new(File::create(&index)?);
            trie.write_radix(&mut writer)?;
            writer.flush()?;
            let write_duration = write_start.elapsed();
            // Metrics
            let secs = duration.as_secs_f64();
            let eps = entries as f64 / secs;
            let bps = bytes_in as f64 / secs;
            let idx_size = std::fs::metadata(&index)?.len();
            eprintln!(
                "Indexed {} entries ({} bytes in) in {:.3} ms: {:.0} entries/s, {:.0} bytes/s",
                entries,
                bytes_in,
                secs * 1000.0,
                eps,
                bps
            );
            eprintln!(
                "Wrote index file {} bytes in {:.3} ms",
                idx_size,
                write_duration.as_secs_f64() * 1000.0
            );
        }
        Commands::List {
            index,
            prefix,
            delimiter,
        } => {
            // Load compressed radix trie from disk via mmap
            let load_start = Instant::now();
            let trie = Trie::load_radix(&index)?;
            let load_duration = load_start.elapsed();
            // Listing timing
            let list_start = Instant::now();
            let entries = trie.list(&prefix, delimiter);
            let list_duration = list_start.elapsed();
            // Print entries
            for entry in entries {
                match entry {
                    Entry::Key(s) => println!("📄 {}", s),
                    Entry::CommonPrefix(s) => println!("📁 {}", s),
                }
            }
            // Output metrics
            let idx_size = std::fs::metadata(&index)?.len();
            eprintln!("Index size: {} bytes", idx_size);
            eprintln!(
                "Load time: {:.3} ms, List time: {:.3} ms",
                load_duration.as_secs_f64() * 1000.0,
                list_duration.as_secs_f64() * 1000.0
            );
        }
    }

    Ok(())
}