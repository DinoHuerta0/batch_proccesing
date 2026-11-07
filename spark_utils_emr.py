from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, TimestampType, BinaryType, StructField, ArrayType
from pyspark.sql.functions import (count, when, isnull, expr, lit, coalesce, trim, lower,
                                   avg, broadcast, explode, min as spark_min, max as spark_max,
                                   sum as spark_sum, round as spark_round)


class SparkUtils:

    def __init__(self, app_name, master_url=None, spark_jars=None, spark_packages=None):
        spark_builder = (
            SparkSession.builder
            .appName(app_name))

        if master_url is not None:
            spark_builder = spark_builder.master(master_url)

        if spark_jars is not None:
            spark_builder = spark_builder.config("spark.jars", spark_jars)

        if spark_packages is not None:
            spark_builder = spark_builder.config("spark.jars.packages", spark_packages)

        self._spark = spark_builder.getOrCreate()

    @property
    def spark(self):
        return self._spark

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
            "array_int": ArrayType(IntegerType()),
            "array_string": ArrayType(StringType()),
            "struct": StructType()
        }

        struct_fields = []
        for column_info in columns_info:
            col_name = column_info[0]
            col_type = column_info[1]

            if col_type == "struct":
                if len(column_info) < 3 or not isinstance(column_info[2], list):
                    raise ValueError(
                        f"Column '{col_name}' is declared as 'struct' but "
                        f"no sub-fields list was provided as the third element."
                    )
                nested_schema = SparkUtils.generate_schema(column_info[2])
                struct_field = StructField(col_name, nested_schema, True)

            elif col_type not in type_mapping:
                raise ValueError(f"Unsupported data type: '{col_type}' for column '{col_name}'")

            else:
                struct_field = StructField(column_info[0], type_mapping[column_info[1]], True)

            struct_fields.append(struct_field)

        return StructType(struct_fields)

    @staticmethod
    def count_nulls(df):
        return df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])

    @staticmethod
    def filter_road_segments(df):
        return (df
                .filter(df["subtype"] == "road")
                .select("id",
                        "class",
                        "subclass",
                        df["names"]["primary"].alias("name"),
                        "speed_limits",
                        "road_surface"))

    @staticmethod
    def clean_segments(df):
        df = df.dropna(subset=["id", "class"])
        df = df.withColumn("class", lower(trim(df["class"])))
        df = df.withColumn("subclass", coalesce(lower(trim(df["subclass"])), lit("none")))
        df = df.withColumn("name", when(trim(df["name"]) == "", lit(None)).otherwise(trim(df["name"])))
        return df.dropDuplicates(["id"])

    @staticmethod
    def derive_speed_columns(df):
        df = df.withColumn("max_speed_kmh", expr(
            "round(array_max(transform("
            "filter(speed_limits, s -> s.max_speed.value IS NOT NULL), "
            "s -> CASE WHEN lower(s.max_speed.unit) = 'mph' "
            "THEN s.max_speed.value * 1.609344 "
            "ELSE CAST(s.max_speed.value AS DOUBLE) END)), 1)"))
        df = df.withColumn("max_speed_kmh",
                           when((df["max_speed_kmh"] >= 5) & (df["max_speed_kmh"] <= 200),
                                df["max_speed_kmh"]))
        df = df.withColumn("surface", expr(
            "CASE WHEN size(road_surface) > 0 THEN lower(road_surface[0].value) END"))
        df = df.withColumn("has_speed_limit", df["max_speed_kmh"].isNotNull())
        df = df.withColumn("has_surface", df["surface"].isNotNull())
        df = df.withColumn("has_name", df["name"].isNotNull())
        df = df.withColumn("speed_band",
                           when(df["max_speed_kmh"].isNull(), "unknown")
                           .when(df["max_speed_kmh"] <= 30, "0-30")
                           .when(df["max_speed_kmh"] <= 60, "31-60")
                           .when(df["max_speed_kmh"] <= 90, "61-90")
                           .otherwise("91+"))
        return df.drop("speed_limits", "road_surface")
    
    @staticmethod
    def explode_segment_connectors(df):
        df = df.filter(df["subtype"] == "road")
        df = df.select(df["id"].alias("segment_id"),
                       explode(df["connectors"]).alias("c"))
        return df.select("segment_id",
                         df["c"]["connector_id"].alias("connector_id"),
                         df["c"]["at"].alias("at"))

    @staticmethod
    def prepare_connectors(df):
        df = df.dropna(subset=["id"])
        df = df.select(df["id"].alias("connector_id"),
                       df["bbox"]["xmin"].alias("lon"),
                       df["bbox"]["ymin"].alias("lat"))
        return df.dropDuplicates(["connector_id"])

    @staticmethod
    def build_connector_degree(df_links, df_connectors):
        df = df_links.join(df_connectors, on="connector_id", how="inner")
        return (df.groupBy("connector_id", "lon", "lat")
                  .agg(count("*").alias("degree")))

    @staticmethod
    def build_segment_network(df_links, df_degree):
        df = df_links.join(df_degree, on="connector_id", how="inner")
        return (df.groupBy("segment_id")
                  .agg(count("*").alias("n_connectors"),
                       spark_sum(when(df["degree"] >= 3, 1).otherwise(0)).alias("n_intersections"),
                       spark_sum(when(df["degree"] == 1, 1).otherwise(0)).alias("n_dead_ends"),
                       spark_max(when(df["at"] == 0, df["lat"])).alias("start_lat"),
                       spark_max(when(df["at"] == 0, df["lon"])).alias("start_lon")))

    @staticmethod
    def join_network_metrics(df_segments, df_network):
        df = df_segments.join(df_network,
                              df_segments["id"] == df_network["segment_id"],
                              how="left").drop("segment_id")
        return df.fillna(0, subset=["n_connectors", "n_intersections", "n_dead_ends"])

    @staticmethod
    def build_class_profile(df):
        return (df
                .groupBy("class")
                .agg(count("*").alias("segments_in_class"),
                     spark_round(avg(df["has_speed_limit"].cast("int")) * 100, 2).alias("pct_with_speed_limit"),
                     spark_round(avg(df["has_surface"].cast("int")) * 100, 2).alias("pct_with_surface"),
                     spark_round(avg(df["has_name"].cast("int")) * 100, 2).alias("pct_with_name"),
                     spark_round(avg("max_speed_kmh"), 1).alias("avg_speed_limit_kmh"),
                     spark_round(avg("n_intersections"), 2).alias("avg_intersections"),
                     spark_min("max_speed_kmh").alias("min_speed_limit_kmh"),
                     spark_max("max_speed_kmh").alias("max_speed_limit_kmh")))

    @staticmethod
    def join_class_profile(df_segments, df_profile):
        df = df_segments.join(broadcast(df_profile), on="class", how="left")
        return df.withColumn("speed_diff_vs_class_kmh",
                             spark_round(df["max_speed_kmh"] - df["avg_speed_limit_kmh"], 1))

    @staticmethod
    def sort_class_profile(df):
        return df.orderBy(df["segments_in_class"].desc(), df["class"].asc())

    @staticmethod
    def write_parquet(df, path, partition_col=None):
        writer = df.write.mode("overwrite")
        if partition_col is not None:
            writer = writer.partitionBy(partition_col)
        writer.parquet(path)
