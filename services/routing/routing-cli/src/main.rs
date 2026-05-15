//! routing-cli: command-line frontend for routing-core.
//!
//! Two modes:
//!   - Single route: --graph + --origin + --destination, prints one JSON object.
//!   - Fixtures: --graph + --fixtures, reads JSONL route requests from stdin,
//!     prints JSONL results. Used by Airflow's run_route_tests to validate
//!     the routing implementation against a freshly built graph.

mod output;
mod run;

use std::process::ExitCode;

use clap::Parser;

/// Exit codes. Keep these stable; Airflow's run_route_tests task will
/// distinguish "route not found" (a meaningful test failure) from
/// "graph load error" (an infrastructure problem).
const EXIT_OK: u8 = 0;
const EXIT_BAD_ARGS: u8 = 1;
const EXIT_GRAPH_LOAD: u8 = 2;
const EXIT_NO_ROUTE: u8 = 3;
const EXIT_INTERNAL: u8 = 4;
const EXIT_FIXTURES_HAD_FAILURES: u8 = 5;

/// Lat,lon coordinate pair as a single argument.
#[derive(Clone, Debug)]
struct LatLon {
    lat: f32,
    lon: f32,
}

impl std::str::FromStr for LatLon {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (lat, lon) = s
            .split_once(',')
            .ok_or_else(|| format!("expected LAT,LON (comma-separated), got {s:?}"))?;
        let lat: f32 = lat
            .trim()
            .parse()
            .map_err(|e| format!("invalid latitude {lat:?}: {e}"))?;
        let lon: f32 = lon
            .trim()
            .parse()
            .map_err(|e| format!("invalid longitude {lon:?}: {e}"))?;
        Ok(LatLon { lat, lon })
    }
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Bike-routing CLI for the loci routing graph format.",
    long_about = "Runs the same routing logic as the deployed Lambda, against a local graph file. \
                  Use --origin/--destination for a single route, or --fixtures to drive a batch."
)]
struct Args {
    /// Path to the gzip-compressed routing graph (.bin.gz).
    #[arg(long, value_name = "PATH")]
    graph: std::path::PathBuf,

    /// Origin coordinate as LAT,LON. Required unless --fixtures is given.
    #[arg(long, value_name = "LAT,LON", conflicts_with = "fixtures")]
    origin: Option<LatLon>,

    /// Destination coordinate as LAT,LON. Required unless --fixtures is given.
    #[arg(
        long,
        value_name = "LAT,LON",
        alias = "dest",
        conflicts_with = "fixtures"
    )]
    destination: Option<LatLon>,

    /// Run in fixtures mode: read JSONL route requests from stdin, write
    /// JSONL results to stdout. Each input line is an object with
    /// {"origin": [lat, lon], "destination": [lat, lon]} and optionally
    /// other fields (preserved on output as "request").
    #[arg(long)]
    fixtures: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let graph = match run::load_graph(&args.graph) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("graph load failed: {e}");
            return ExitCode::from(EXIT_GRAPH_LOAD);
        }
    };

    if args.fixtures {
        match run::run_fixtures(&graph) {
            Ok(0) => ExitCode::from(EXIT_OK),
            Ok(_) => ExitCode::from(EXIT_FIXTURES_HAD_FAILURES),
            Err(e) => {
                eprintln!("fixtures mode failed: {e}");
                ExitCode::from(EXIT_INTERNAL)
            }
        }
    } else {
        let (origin, dest) = match (args.origin, args.destination) {
            (Some(o), Some(d)) => (o, d),
            _ => {
                eprintln!("--origin and --destination are required unless --fixtures is set");
                return ExitCode::from(EXIT_BAD_ARGS);
            }
        };
        match run::run_single(&graph, origin.lat, origin.lon, dest.lat, dest.lon) {
            Ok(json) => {
                println!("{json}");
                ExitCode::from(EXIT_OK)
            }
            Err(run::RunError::NoRoute) => {
                eprintln!("no route found between origin and destination");
                ExitCode::from(EXIT_NO_ROUTE)
            }
            Err(e) => {
                eprintln!("routing failed: {e}");
                ExitCode::from(EXIT_INTERNAL)
            }
        }
    }
}
