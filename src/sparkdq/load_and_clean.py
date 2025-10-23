"""Phase 1: Load and clean data without DQ rules"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_and_clean(spark: SparkSession, csv_path: str):
    """
    Load CSV data and perform basic cleaning without DQ framework"""
    # Load CSV data
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Basic checks
    print("Schema:")
    df.printSchema()
    print(f"Total rows: {df.count()}")

    # Simple cleaning - remove null Year or negative values
    df_clean = df.filter(col("Year").isNotNull() & (col("Year") > 0))

    # Show sample output
    print("Cleaned Data Sample:")
    df_clean.show(5)

    return df_clean