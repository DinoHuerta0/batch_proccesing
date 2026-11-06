import argparse

from spark_utils_emr import SparkUtils


def parse_args():
    parser = argparse.ArgumentParser(description="Overture road speed profile")
    parser.add_argument("--input", required=True, help="Segment Parquet folder")
    parser.add_argument("--output", required=True, help="Base output path (s3://...)")
    parser.add_argument("--report", default=None, help="Text report path (s3://...)")
    parser.add_argument("--rows", type=int, default=20, help="Rows per report sample")
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

        if args.report is not None:
            report = (su.debug_report(df_raw, title=f"Input: {args.input}", rows=5)
                      + su.debug_report(su.spark.read.parquet(profile_path),
                                        title=f"Output: {profile_path}", rows=args.rows)
                      + su.debug_report(su.spark.read.parquet(segments_path),
                                        title=f"Output: {segments_path}", rows=args.rows))
            su.write_text(args.report, report)

    except Exception as e:
        if args.report is not None:
            su.write_text(args.report, f"Input: {args.input}\n\n=== ERROR ===\n{type(e).__name__}: {e}\n")
        su.spark.stop()
        raise

    su.spark.stop()


if __name__ == "__main__":
    main()
