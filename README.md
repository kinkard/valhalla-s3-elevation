# About

A small tool to fetch Mapzen/Tilezen elevation tiles (SRTM HGT) directly from S3 and write them as decompressed .hgt files on disk. It is a drop-in replacement for the download/decompress part of Valhalla’s Python script (valhalla_build_elevation) with high concurrency, retries, and a live progress display.

Key points

- Pulls tiles from any S3 bucket/prefix; defaults to the public ASDI bucket [s3://elevation-tiles-prod/skadi]( https://aws.amazon.com/blogs/publicsector/announcing-terrain-tiles-on-aws-a-qa-with-mapzen/)
- Writes decompressed .hgt files directly to disk (no .gz files kept)
- Concurrent downloads with retry and backoff
- Progress bar with instantaneous/average throughput
- Works with a bbox or a Valhalla graph to determine the tiles to fetch

## Build & Run

```sh
cargo run -- --help
Downloads Tilezen's elevation tiles from arbitrary S3 and decompresses them on the fly

Usage: valhalla-s3-elevation [OPTIONS] --outdir <DIR>

Options:
      --s3 <S3>                     S3 location in form s3://bucket/prefix [default: s3://elevation-tiles-prod/skadi]
  -o, --outdir <DIR>                Output directory where elevation tiles will be written
      --force                       Overwrite existing files (otherwise skip)
  -p, --concurrency <CONCURRENCY>   Concurrency (number of in-flight downloads) [default: 16]
      --retries <RETRIES>           Retry attempts per tile [default: 5]
      --bbox <MINX,MINY,MAXX,MAXY>  Download tiles intersecting the given bbox (minX,minY,maxX,maxY)
      --graph <DIR>                 Download tiles intersecting tiles in a Valhalla graph directory
  -h, --help                        Print help
```

Alternatively, pre-built Docker images are available on Docker Hub: [kinkard/valhalla-s3-elevation](https://hub.docker.com/r/kinkard/valhalla-s3-elevation)

```sh
docker run -v $PWD:/elevation --rm kinkard/valhalla-s3-elevation -o /elevation --bbox 5.6,49.5,6.6,50.2
```

## License

All code in this project is dual-licensed under either:

- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))
- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))

at your option.
