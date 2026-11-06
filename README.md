# Overture Maps Road Speed Profile — Batch Processing with Apache Spark on AWS EMR

Final Project Part I — Big Data (ITESO, Autumn 2026)

A batch pipeline that reads the **Transportation / Segment** data of the [Overture Maps](https://overturemaps.org) Open Map Data (release `2026-10-21.0`), analyzes the **attribute coverage and speed limits** of road segments by road class, and persists the results to Amazon S3 as partitioned Parquet.

## Files

| File | Description |
|---|---|
| `spark_utils_emr.py` | `SparkUtils` class. Creates the Spark session and contains every transformation and the persistence method. |
| `project.py` | Driver script. Parses the arguments, creates the `SparkUtils` object, reads the input and calls the methods in order. |

## Pipeline

| # | Method | Type | Description |
|---|---|---|---|
| 1 | `filter_road_segments` | Filtering | Keeps `subtype = road` and selects only the 6 columns needed (column pruning). |
| 2 | `clean_segments` | Data cleaning | Drops rows without `id`/`class`, normalizes text, fills missing `subclass` with `"none"`, converts empty names to null and removes duplicate IDs. |
| 3 | `derive_speed_columns` | Column derivation | Computes `max_speed_kmh` (mph converted to km/h, highest rule kept, out-of-range values nulled), `surface`, coverage flags (`has_speed_limit`, `has_surface`, `has_name`) and `speed_band`. |
| 4 | `build_class_profile` | Aggregation | Per road class: segment count, % with speed limit / surface / name, and average, min and max speed limit. |
| 5 | `join_class_profile` | Join | Broadcast join of the class profile back onto each segment on `class`, plus `speed_diff_vs_class_kmh`. |
| 6 | `sort_class_profile` | Sorting | Orders the class profile from the most to the least common class. |
| — | `write_parquet` | Persistence (action) | `df.write.mode("overwrite").partitionBy(...).parquet(...)` |

All transformations are lazy; the job is triggered by the Parquet write.

## Input and output

| | Path |
|---|---|
| Input (segment) | `s3://iteso-bigdata-sebastian/data/project/segment/` |
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
     --output s3://iteso-bigdata-sebastian/output/project/
   ```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--input` | Yes | Folder with the segment Parquet files. |
| `--output` | Yes | Base S3 path where both outputs are written. |

## Environment

- Amazon EMR 8.0.0 (Spark 4.0.2)
- 1 primary and 6 core nodes
- EC2 instance profile with read/write access to the bucket

## Data license

Overture transportation data is published under the [ODbL](https://opendatacommons.org/licenses/odbl/) license, © OpenStreetMap contributors and Overture Maps Foundation.
