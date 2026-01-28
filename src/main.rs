use std::{
    collections::{HashSet, VecDeque},
    io::Read,
    path::{Path, PathBuf},
    time::{self, Duration},
};

use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::GzDecoder;
use futures_util::{
    StreamExt, TryFutureExt, TryStreamExt,
    stream::{self},
};
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::{sync::mpsc, time::sleep};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Mapzen's Terrain Tiles are publicly available as part of the Amazon Sustainability Data Initiative.
/// https://aws.amazon.com/blogs/publicsector/announcing-terrain-tiles-on-aws-a-qa-with-mapzen/
const MAPZEN_PUBLIC_S3: &str = "s3://elevation-tiles-prod/skadi";

/// Downloads Tilezen's elevation tiles from arbitrary S3 and decompresses them on the fly.
#[derive(Parser)]
struct Cli {
    /// S3 location in form s3://bucket/prefix
    #[arg(long, default_value = MAPZEN_PUBLIC_S3)]
    s3: String,

    /// Output directory where elevation tiles will be written
    #[arg(short = 'o', long, value_name = "DIR")]
    outdir: PathBuf,

    /// Overwrite existing files (otherwise skip)
    #[arg(long = "force")]
    force: bool,

    /// Concurrency (number of in-flight downloads)
    #[arg(short = 'p', long = "concurrency", default_value_t = 16)]
    concurrency: usize,

    /// Retry attempts per tile
    #[arg(long = "retries", default_value_t = 5)]
    retries: u64,

    /// Download tiles intersecting the given bbox (minX,minY,maxX,maxY)
    #[arg(long = "bbox", value_name = "MINX,MINY,MAXX,MAXY", value_parser = parse_bbox, conflicts_with = "graph")]
    bbox: Option<[f64; 4]>,

    /// Download tiles intersecting tiles in a Valhalla graph directory
    #[arg(long = "graph", value_name = "DIR", conflicts_with = "bbox")]
    graph: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut tiles_to_download = match (cli.bbox, cli.graph) {
        (None, None) => tiles_in_bbox([-180.0, -90.0, 180.0, 90.0]),
        (Some(bbox), None) => tiles_in_bbox(bbox),
        (None, Some(graph)) => graph_tiles(&graph),
        _ => unreachable!("clap ensures that bbox or graph are mutually exclusive"),
    };

    // Remove all already existing tiles if not forcing
    if !cli.force {
        let existing = glob(&format!("{}/**/*.hgt", cli.outdir.display()))?
            .flatten()
            .filter_map(|path| path.file_name().map(|s| s.to_owned()))
            .collect::<HashSet<_>>();
        if !existing.is_empty() {
            tiles_to_download.retain(|tile| {
                !existing.contains(std::ffi::OsStr::new(
                    tile.name().rsplit_once('/').unwrap().1,
                ))
            });
        }
    }

    if tiles_to_download.is_empty() {
        println!("No tiles to download");
        return Ok(());
    }
    println!("Found {} tiles to download", tiles_to_download.len());

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_task = tokio::spawn(progress_tracker_task(progress_rx, tiles_to_download.len()));

    let s3 = S3::new(&cli.s3).await;
    stream::iter(tiles_to_download)
        .map(Result::<_>::Ok)
        .try_for_each_concurrent(cli.concurrency, |tile| {
            s3.download_tile(tile, &cli.outdir, cli.retries)
                .and_then(|stats| async {
                    let now = time::Instant::now();
                    let _ = progress_tx.send((now, stats));
                    Ok(())
                })
        })
        .await?;

    drop(progress_tx); // Signal progress tracker to finish
    progress_task.await.unwrap();

    Ok(())
}

/// Coordinates of a single elevation tile
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ElevationTileId {
    lat: i16,
    lon: i16,
}

impl ElevationTileId {
    /// Get the filename for this tile that can be used in S3 keys, e.g. "N37/N37E122.hgt"
    fn name(&self) -> String {
        let lat_prefix = if self.lat >= 0 { 'N' } else { 'S' };
        let lon_prefix = if self.lon >= 0 { 'E' } else { 'W' };
        format!(
            "{lat_prefix}{:02}/{lat_prefix}{:02}{lon_prefix}{:03}.hgt",
            self.lat.abs(),
            self.lat.abs(),
            self.lon.abs()
        )
    }
}

// Helper to parse and validate bbox
fn parse_bbox(s: &str) -> Result<[f64; 4], String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 4 {
        return Err("bbox must have 4 comma-separated numbers: minX,minY,maxX,maxY".into());
    }
    let mut vals = [0.0f64; 4];
    for (i, p) in parts.iter().enumerate() {
        vals[i] = p
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("invalid number: {}", p))?;
    }
    let [min_x, min_y, max_x, max_y] = vals;
    if !(min_x <= max_x
        && min_x >= -180.0
        && max_x <= 180.0
        && min_y <= max_y
        && min_y >= -90.0
        && max_y <= 90.0)
    {
        return Err(format!("invalid bbox: {:?}", vals));
    }
    Ok(vals)
}

/// Coverts a bbox in lon/lat coordinates to a list of elevation tiles that intersect it.
fn tiles_in_bbox(bbox: [f64; 4]) -> Vec<ElevationTileId> {
    let [min_lon, min_lat, max_lon, max_lat] = bbox;

    // Convert to inclusive tile indices
    let lon_start = (min_lon.floor() as i16).clamp(-180, 180);
    let lon_end = ((max_lon.ceil() as i16) - 1).clamp(-180, 180);
    let lat_start = (min_lat.floor() as i16).clamp(-90, 90);
    let lat_end = ((max_lat.ceil() as i16) - 1).clamp(-90, 90);

    if lon_start > lon_end || lat_start > lat_end {
        return Vec::new();
    }

    let w = (lon_end - lon_start + 1) as usize;
    let h = (lat_end - lat_start + 1) as usize;
    let mut tiles = Vec::with_capacity(w * h);

    for lat in lat_start..=lat_end {
        for lon in lon_start..=lon_end {
            tiles.push(ElevationTileId { lon, lat });
        }
    }
    tiles
}

fn graph_tiles(graph_dir: &str) -> Vec<ElevationTileId> {
    let Ok(paths) = glob(&format!("{graph_dir}/2/**/*.gph")) else {
        return Vec::new();
    };
    paths
        .flatten()
        .filter_map(|path| parse_graph_tile_name(path.to_string_lossy().as_ref()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

fn parse_graph_tile_name(name: &str) -> Option<ElevationTileId> {
    // Graph tile size in degrees at level 2 (GraphLevel::Local)
    const TILES_IN_DEGREE: i32 = 4;
    const TILES_IN_ROW: i32 = 360 * TILES_IN_DEGREE;

    let tile_id = name
        .rsplit_once("/2/")?
        .1
        .strip_suffix(".gph")?
        .replace("/", "")
        .parse::<i32>()
        .ok()?;

    Some(ElevationTileId {
        lat: (tile_id / TILES_IN_ROW / TILES_IN_DEGREE - 90) as i16,
        lon: (tile_id % TILES_IN_ROW / TILES_IN_DEGREE - 180) as i16,
    })
}

pub(crate) struct S3 {
    s3_client: aws_sdk_s3::Client,
    bucket: String,
    // Prefix always ends with a slash or is empty, so joining via `+` works correctly
    prefix: String,
}

impl S3 {
    /// s3_location: The S3 location in the format "s3://bucket-name/prefix"
    pub(crate) async fn new(s3_location: &str) -> Self {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if s3_location == MAPZEN_PUBLIC_S3 {
            // This public s3 bucket requires anonymous access (like `aws s3 --no-sign-request`) in us-east-1 region.
            config_loader = config_loader.no_credentials().region("us-east-1");
        }
        let config = config_loader.load().await;
        let s3_client = aws_sdk_s3::Client::new(&config);

        let s3_location = s3_location
            .trim_start_matches("s3://")
            .trim_end_matches('/');
        let (bucket, prefix) = s3_location.split_once('/').unwrap_or((s3_location, ""));

        Self {
            s3_client,
            bucket: bucket.to_string(),
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
        }
    }

    async fn get_obj(&self, s3_key: &str) -> Result<bytes::Bytes> {
        let resp = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(self.prefix.clone() + s3_key)
            .send()
            .await
            .context("Failed to get S3 object")?;

        let data = resp
            .body
            .collect()
            .await
            .context("Failed to read from S3 download stream")?
            .into_bytes();

        Ok(data)
    }

    /// Downloads a tile, decompresses it on the fly, and writes it to disk
    pub(crate) async fn download_tile(
        &self,
        tile: ElevationTileId,
        output_dir: &Path,
        retries: u64,
    ) -> Result<DownloadStats> {
        let tile_name = tile.name();
        let s3_key = format!("{}.gz", tile_name); // S3 files are gzipped

        let mut attempt = 0;
        let encoded_tile = loop {
            match self.get_obj(&s3_key).await {
                Ok(data) => break data,
                Err(e) if attempt == retries => {
                    return Err(anyhow::anyhow!(
                        "All {attempt} attempts failed for tile {tile_name}: {e:#}"
                    ));
                }
                Err(_) => {
                    attempt += 1;
                    let delay = Duration::from_secs(attempt);
                    sleep(delay).await;
                }
            }
        };

        // N37/N37E122.hgt -> dir_name = "N37", file_name = "N37E122.hgt"
        let (dir_name, file_name) = tile_name.split_once('/').unwrap();
        let output_dir_path = output_dir.join(dir_name);
        let output_file_path = output_dir_path.join(file_name);

        let downloaded = encoded_tile.len();

        // Decompress and write to disk in a blocking task to avoid blocking the async runtime
        let written = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&output_dir_path)
                .context("Failed to create output directory")?;

            let mut decoder = GzDecoder::new(encoded_tile.as_ref());
            let mut decompressed_data = Vec::new();
            decoder
                .read_to_end(&mut decompressed_data)
                .context("Failed to decompress gzip data")?;

            let written = decompressed_data.len();
            std::fs::write(&output_file_path, &decompressed_data).with_context(|| {
                format!(
                    "Failed to write decompressed data to file {}",
                    output_file_path.display()
                )
            })?;

            Ok::<_, anyhow::Error>(written)
        })
        .await
        .context("Writing tile failed")??;

        Ok(DownloadStats {
            downloaded,
            written,
        })
    }
}

#[derive(Debug)]
struct DownloadStats {
    downloaded: usize,
    written: usize,
}

fn calculate_rate(points: &VecDeque<(time::Instant, DownloadStats)>) -> (f64, f64) {
    if points.len() < 2 {
        return (0.0, 0.0);
    }

    let (downloaded, written) = points.iter().fold((0.0, 0.0), |acc, p| {
        (acc.0 + p.1.downloaded as f64, acc.1 + p.1.written as f64)
    });

    let time_span = points
        .back()
        .unwrap()
        .0
        .duration_since(points.front().unwrap().0);

    if time_span.as_secs_f64() > 0.0 {
        let download_rate = downloaded / time_span.as_secs_f64() / (1024.0 * 1024.0); // MB/s
        let write_rate = written / time_span.as_secs_f64() / (1024.0 * 1024.0); // MB/s
        (download_rate, write_rate)
    } else {
        (0.0, 0.0)
    }
}

async fn progress_tracker_task(
    mut progress_rx: mpsc::UnboundedReceiver<(time::Instant, DownloadStats)>,
    total_tiles: usize,
) {
    let pb = ProgressBar::new(total_tiles as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{bar:40.cyan/blue} {pos:>4}/{len:4} | DL: {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let begin = time::Instant::now();
    let mut events = VecDeque::new();
    let mut completed_tiles = 0;
    let mut total_downloaded = 0;
    let mut total_written = 0;

    let mut update_interval = tokio::time::interval(Duration::from_secs(1));
    update_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Handle progress events
            event = progress_rx.recv() => {
                match event {
                Some(event) => {
                    total_written += event.1.written;
                    total_downloaded += event.1.downloaded;

                    let cutoff = event.0 - Duration::from_secs(10);
                    events.push_back(event);
                    while events.front().is_some_and(|&(timestamp, _)| timestamp < cutoff) {
                        events.pop_front();
                    }

                    completed_tiles += 1;
                    pb.set_position(completed_tiles as u64);
                    if completed_tiles >= total_tiles {
                        break;
                    }
                },
                None => break, // Channel closed
                }
            }

            // Update progress bar every second
            _ = update_interval.tick() => {
                let (download_rate, write_rate) = calculate_rate(&events);
                let total_gb = total_written as f64 / (1024.0 * 1024.0 * 1024.0);

                pb.set_message(format!(
                    "{download_rate:.1} MiB/s | Write: {write_rate:.1} MiB/s | {total_gb:.2} GiB",
                ));
            }
        }
    }

    pb.finish_and_clear();

    println!(
        "Downloaded {} tiles in {:.1} seconds | Avg. download rate: {:.2} MiB/s | Avg. write rate: {:.2} MiB/s | Total written: {:.2} GiB",
        completed_tiles,
        begin.elapsed().as_secs_f64(),
        total_downloaded as f64 / (1024.0 * 1024.0) / begin.elapsed().as_secs_f64(),
        total_written as f64 / (1024.0 * 1024.0) / begin.elapsed().as_secs_f64(),
        total_written as f64 / (1024.0 * 1024.0 * 1024.0),
    );
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_parse_bbox() {
        assert_eq!(
            parse_bbox("-10.0,-20.0,10.0,20.0"),
            Ok([-10.0, -20.0, 10.0, 20.0])
        );
        assert_eq!(parse_bbox("1,2,3,4"), Ok([1.0, 2.0, 3.0, 4.0]));

        assert!(parse_bbox("10.0,-20.0,-10.0,20.0").is_err());
        assert!(parse_bbox("-200.0,-20.0,10.0,20.0").is_err());
        assert!(parse_bbox("-10.0,-100.0,10.0,20.0").is_err());
        assert!(parse_bbox("-10.0,-20.0,200.0,20.0").is_err());
        assert!(parse_bbox("-10.0,-20.0,10.0,100.0").is_err());
        assert!(parse_bbox("-10.0,-20.0,10.0").is_err());
        assert!(parse_bbox("-10.0,-20.0,10.0,20.0,30.0").is_err());
        assert!(parse_bbox("a,b,c,d").is_err());
    }

    #[test]
    fn elevation_tile_name() {
        assert_eq!(
            ElevationTileId { lat: 37, lon: 122 }.name(),
            "N37/N37E122.hgt"
        );
        assert_eq!(
            ElevationTileId {
                lat: -86,
                lon: -176
            }
            .name(),
            "S86/S86W176.hgt"
        );
        assert_eq!(ElevationTileId { lat: 0, lon: 0 }.name(), "N00/N00E000.hgt");
        assert_eq!(
            ElevationTileId { lat: -1, lon: 1 }.name(),
            "S01/S01E001.hgt"
        );
    }

    #[test]
    fn test_tiles_in_bbox() {
        let tiles = tiles_in_bbox([0.1, 0.1, 0.2, 0.2]);
        let expected = vec![ElevationTileId { lat: 0, lon: 0 }];
        assert_eq!(tiles, expected);

        let tiles = tiles_in_bbox([-10.5, -20.5, -8.1, -18.5]);
        let expected = vec![
            ElevationTileId { lat: -21, lon: -11 },
            ElevationTileId { lat: -21, lon: -10 },
            ElevationTileId { lat: -21, lon: -9 },
            ElevationTileId { lat: -20, lon: -11 },
            ElevationTileId { lat: -20, lon: -10 },
            ElevationTileId { lat: -20, lon: -9 },
            ElevationTileId { lat: -19, lon: -11 },
            ElevationTileId { lat: -19, lon: -10 },
            ElevationTileId { lat: -19, lon: -9 },
        ];
        assert_eq!(tiles, expected);

        let tiles = tiles_in_bbox([-1.3, -0.9, 1.9, 0.1]);
        let expected = vec![
            ElevationTileId { lat: -1, lon: -2 },
            ElevationTileId { lat: -1, lon: -1 },
            ElevationTileId { lat: -1, lon: 0 },
            ElevationTileId { lat: -1, lon: 1 },
            ElevationTileId { lat: 0, lon: -2 },
            ElevationTileId { lat: 0, lon: -1 },
            ElevationTileId { lat: 0, lon: 0 },
            ElevationTileId { lat: 0, lon: 1 },
        ];
        assert_eq!(tiles, expected);

        let tiles = tiles_in_bbox([-180.0, 55.5, 180.0, 55.5]);
        let expected = (-180..180)
            .map(|lon| ElevationTileId { lat: 55, lon })
            .collect::<Vec<_>>();
        assert_eq!(tiles, expected);
    }

    #[test]
    fn test_parse_graph_tile_name() {
        // Andorra is fully contained in a single elevation tile
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/762/485.gph"),
            Some(ElevationTileId { lat: 42, lon: 1 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/762/486.gph"),
            Some(ElevationTileId { lat: 42, lon: 1 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/763/925.gph"),
            Some(ElevationTileId { lat: 42, lon: 1 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/763/926.gph"),
            Some(ElevationTileId { lat: 42, lon: 1 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/763/927.gph"),
            Some(ElevationTileId { lat: 42, lon: 1 })
        );

        // Some tiles in Germany
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/789/881.gph"),
            Some(ElevationTileId { lat: 47, lon: 10 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/791/317.gph"),
            Some(ElevationTileId { lat: 47, lon: 9 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/791/319.gph"),
            Some(ElevationTileId { lat: 47, lon: 9 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/791/320.gph"),
            Some(ElevationTileId { lat: 47, lon: 10 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/791/323.gph"),
            Some(ElevationTileId { lat: 47, lon: 10 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/791/332.gph"),
            Some(ElevationTileId { lat: 47, lon: 13 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/792/750.gph"),
            Some(ElevationTileId { lat: 47, lon: 7 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/804.gph"),
            Some(ElevationTileId { lat: 60, lon: 21 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/805.gph"),
            Some(ElevationTileId { lat: 60, lon: 21 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/806.gph"),
            Some(ElevationTileId { lat: 60, lon: 21 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/807.gph"),
            Some(ElevationTileId { lat: 60, lon: 21 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/819.gph"),
            Some(ElevationTileId { lat: 60, lon: 24 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/864/820.gph"),
            Some(ElevationTileId { lat: 60, lon: 25 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/866/247.gph"),
            Some(ElevationTileId { lat: 60, lon: 21 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/866/248.gph"),
            Some(ElevationTileId { lat: 60, lon: 22 })
        );

        // Guyana
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/527/525.gph"),
            Some(ElevationTileId { lat: 1, lon: -59 })
        );
        assert_eq!(
            parse_graph_tile_name("tiles/2/000/566/401.gph"),
            Some(ElevationTileId { lat: 8, lon: -60 })
        );
    }

    #[tokio::test]
    async fn test_new() {
        let s3 = S3::new(MAPZEN_PUBLIC_S3).await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "skadi/");

        let s3 = S3::new("s3://elevation-tiles-prod/skadi").await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "skadi/");

        // With trailing slash
        let s3 = S3::new("s3://elevation-tiles-prod/skadi/").await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "skadi/");

        // Without prefix
        let s3 = S3::new("s3://elevation-tiles-prod").await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "");

        // Without prefix but with trailing slash
        let s3 = S3::new("s3://elevation-tiles-prod/").await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "");

        // Without s3 scheme
        let s3 = S3::new("elevation-tiles-prod/skadi").await;
        assert_eq!(s3.bucket, "elevation-tiles-prod");
        assert_eq!(s3.prefix, "skadi/");

        // All together
        let s3 = S3::new("aaaa-aaa-aaa-aa/bbb-bbb/ccc-ccc/ddd-ddd/eee-eee/").await;
        assert_eq!(s3.bucket, "aaaa-aaa-aaa-aa");
        assert_eq!(s3.prefix, "bbb-bbb/ccc-ccc/ddd-ddd/eee-eee/");
    }

    #[tokio::test]
    async fn download_tile() {
        let s3 = S3::new(MAPZEN_PUBLIC_S3).await;
        let downloaded = s3.get_obj("N00/N00E000.hgt.gz").await.unwrap();
        assert!(downloaded.len() > 1024 * 1024); // should be slightly bigger than 1MiB
    }
}
