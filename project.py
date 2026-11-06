import argparse

from spark_utils_emr import SparkUtils


def parse_args():
    parser = argparse.ArgumentParser(description="Overture road speed profile")
    parser.add_argument("--input", required=True, help="Segment Parquet folder")
    parser.add_argument("--output", required=True, help="Base output path (s3://...)")
    return parser.parse_args()


def main():
    args = parse_args()
    su = SparkUtils("overture-speed-profile")

    output = args.output.rstrip("/")
    segments_path = f"{output}/segments_speed_profile"
    profile_path = f"{output}/class_profile"

    try:
        df_raw = su.spark.read.parquet(args.input)
        df_roads = SparkUtils.filter_road_segments(df_raw)
        df_clean = SparkUtils.clean_segments(df_roads)
        df_speed = SparkUtils.derive_speed_columns(df_clean).cache()
        df_profile = SparkUtils.build_class_profile(df_speed)
        df_result = SparkUtils.join_class_profile(df_speed, df_profile)
        df_profile_sorted = SparkUtils.sort_class_profile(df_profile)

        SparkUtils.write_parquet(df_result, segments_path, "class")
        SparkUtils.write_parquet(df_profile_sorted, profile_path)
    finally:
        su.spark.stop()


if __name__ == "__main__":
    main()
