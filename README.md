# Overture Maps Road Speed Profile — Batch Processing with Apache Spark on AWS EMR

Final Project Part I — Big Data (ITESO, Autumn 2026)

A batch pipeline that reads the **Transportation** theme (**Segment** and **Connector** types) of the [Overture Maps](https://overturemaps.org) Open Map Data (release `2026-10-21.0`, 34.3 GiB). It analyzes the **attribute coverage, speed limits and network structure** (intersections and dead ends) of road segments by road class, and persists the results to Amazon S3 as partitioned Parquet.

## Files

| File | Description |
|---|---|
| `spark_utils_emr.py` | `SparkUtils` class. Creates the Spark session and contains every transformation and the persistence method. |
| `project.py` | Driver script. Parses the arguments, creates the `SparkUtils` object, reads both inputs and calls the methods in order. |

## Pipeline

| # | Method | Type | Description |
|---|---|---|---|
| 1 | `filter_road_segments` | Filtering | Keeps `subtype = road` and selects only the 6 columns needed (column pruning). |
| 2 | `clean_segments` | Data cleaning | Drops rows without `id`/`class`, normalizes text, fills missing `subclass` with `"none"`, converts empty names to null and removes duplicate IDs. |
| 3 | `derive_speed_columns` | Column derivation | Computes `max_speed_kmh` (mph converted to km/h, highest rule kept, out-of-range values nulled), `surface`, coverage flags (`has_speed_limit`, `has_surface`, `has_name`) and `speed_band`. |
| 4 | `explode_segment_connectors` | Filtering / derivation | Explodes the `connectors` array of road segments into one row per (`segment_id`, `connector_id`, `at`). |
| 5 | `prepare_connectors` | Data cleaning | Drops connectors without `id`, extracts `lon`/`lat` from `bbox` and removes duplicate IDs. |
| 6 | `build_connector_degree` | Join + aggregation | Joins segment–connector links with the connectors on `connector_id` and counts the road segments that meet at each connector (`degree`). |
| 7 | `build_segment_network` | Join + aggregation | Per segment: `n_connectors`, `n_intersections` (degree ≥ 3), `n_dead_ends` (degree = 1) and start point (`start_lat`, `start_lon`). |
| 8 | `join_network_metrics` | Join | Left join of the network metrics onto each segment (`id = segment_id`); segments without matches get 0. |
| 9 | `build_class_profile` | Aggregation | Per road class: segment count, % with speed limit / surface / name, average intersections per segment, and average, min and max speed limit. |
| 10 | `join_class_profile` | Join | Broadcast join of the class profile back onto each segment on `class`, plus `speed_diff_vs_class_kmh`. |
| 11 | `sort_class_profile` | Sorting | Orders the class profile from the most to the least common class. |
| — | `write_parquet` | Persistence (action) | `df.write.mode("overwrite").partitionBy(...).parquet(...)` |

All transformations are lazy; the job is triggered by the Parquet write. The segment DataFrame with network metrics is cached because it feeds both outputs.

## Input and output

| | Path |
|---|---|
| Input (segment, 11.1 GiB) | `s3://iteso-bigdata-sebastian/data/project/segment/` |
| Input (connector, 23.2 GiB) | `s3://iteso-bigdata-sebastian/data/project/connector/` |
| Output — segments, partitioned by `class` | `s3://iteso-bigdata-sebastian/output/project/segments_speed_profile/` |
| Output — class profile | `s3://iteso-bigdata-sebastian/output/project/class_profile/` |

## Running on AWS EMR

1. Upload both files to S3:

   ```bash
   aws s3 cp spark_utils_emr.py s3://iteso-bigdata-sebastian/src/
   aws s3 cp project.py s3://iteso-bigdata-sebastian/src/
   ```

2. Add a **Spark application** step to the EMR cluster (deploy mode: cluster), equivalent to:

   ```bash
   spark-submit --deploy-mode cluster \
     --py-files s3://iteso-bigdata-sebastian/src/spark_utils_emr.py \
     s3://iteso-bigdata-sebastian/src/project.py \
     --input s3://iteso-bigdata-sebastian/data/project/segment/ \
     --connectors s3://iteso-bigdata-sebastian/data/project/connector/ \
     --output s3://iteso-bigdata-sebastian/output/project/
   ```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--input` | Yes | Folder with the segment Parquet files. |
| `--connectors` | Yes | Folder with the connector Parquet files. |
| `--output` | Yes | Base S3 path where both outputs are written. |

## Environment

- Amazon EMR 8.0.0 (Spark 4.0.2)
- 1 primary and 6 core nodes
- EC2 instance profile with read/write access to the bucket

## Dataset size

| Type | Parquet files | Size | Records |
|---|---|---|---|
| connector | 32 | 23.2 GiB | 423,889,001 |
| segment | 21 | 11.1 GiB | 58,618,049 |
| **Total** | **53** | **34.3 GiB** | **482,507,050** |

## Run summary

Results of the run on 2026-11-06 (EMR step `speed-profile`, status **Completed**):

- **Total uptime:** 12 minutes, with 16 completed jobs and 16 completed stages (43 skipped because their shuffle output was reused).
- **Connector read (Stage 4):** 11.4 GiB and 423,889,001 records processed by 192 tasks, with 19.2 GiB of shuffle write.
- **Segment–connector join (Stage 9):** 30.3 GiB of shuffle read processed by 1,000 tasks.
- **Output:** `segments_speed_profile/` contains one `class=<value>/` folder for each of the 17 road classes, plus the `_SUCCESS` marker.

> **Input size in the Spark UI:** Parquet is columnar, so Spark only reads the columns each transformation needs (column pruning). The input shown in the Spark UI is therefore lower than the 34.3 GiB stored in the bucket, even though both folders are read in full.

## Results

The `class_profile` output covers **58,388,368 road segments** in **17 road classes**. Main classes:

| Class | Segments | % with speed limit | % with name | Avg. speed limit (km/h) | Avg. intersections |
|---|---|---|---|---|---|
| residential | 24,229,385 | 8.78 | 68.61 | 40.6 | 1.79 |
| service | 13,755,234 | 1.07 | 2.31 | 29.9 | 0.28 |
| footway | 4,377,798 | 0.03 | 2.60 | 25.3 | 0.56 |
| tertiary | 3,208,038 | 23.26 | 83.02 | 51.7 | 1.99 |
| secondary | 1,947,424 | 39.25 | 84.06 | 60.3 | 2.00 |
| primary | 1,179,933 | 52.94 | 80.80 | 67.1 | 2.00 |
| trunk | 614,512 | 61.03 | 67.96 | 79.0 | 2.00 |
| motorway | 238,663 | 45.08 | 32.15 | 98.6 | 1.99 |

Key findings:

- **Coverage depends on the importance of the road.** Trunk, primary and motorway segments have the highest share of speed limits, while residential and service roads, about 65% of all segments, rarely have one.
- **Speed limits follow the road hierarchy**, from 98.6 km/h on motorways to 40.6 km/h on residential streets.
- **Main roads are highly connected** (about 2 intersections per segment), while service roads and tracks (0.28) usually end without connecting to another road.
- **Data quality:** most classes reach the 5 km/h lower bound of the cleaning step, and some have unrealistic maximums (177 km/h on residential, 200 km/h on service), which are probably tagging errors in the source data.

## Future improvements

- **Fewer small files:** each `class=<value>/` folder currently contains up to 500 small Parquet files, one per task of the last stage. Applying `df.repartition(n, "class")` before the write would group the rows of each class into fewer tasks and reduce the number of files per partition.
- **Outlier detection:** use per-class ranges or percentiles instead of the global 5–200 km/h range to detect wrong speed limits.

## Data license

Overture transportation data is published under the [ODbL](https://opendatacommons.org/licenses/odbl/) license, © OpenStreetMap contributors and Overture Maps Foundation.
