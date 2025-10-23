"""Main entry point for crime statistics data quality pipeline"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sparkdq.load_and_clean import load_and_clean
from sparkdq.dq_apply import apply_dq_rules, get_dq_summary
import sys
import os


def create_sample_rules(spark):
    """Create sample DQ rules for local testing"""
    
    rules_data = [
        ("crime_dq", "offences", "row_dq", "year_not_null", "Year",
         "Year is not null", "drop", "completeness", 
         "Year must not be null", True, True, True),
        
        ("crime_dq", "offences", "row_dq", "year_valid_range", "Year",
         "Year >= 1950 AND Year <= 2023", "drop", "validity",
         "Year must be between 1950 and 2023", True, True, True),
        
        ("crime_dq", "offences", "row_dq", "total_crimes_positive", 
         "Total number of crimes",
         "`Total number of crimes` > 0", "drop", "validity",
         "Total crimes must be positive", True, True, True),
        
        ("crime_dq", "offences", "agg_dq", "no_duplicate_years", "Year",
         "count(*) = count(distinct Year)", "fail", "uniqueness",
         "Each year should appear once", True, True, True),
    ]
    
    rules_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("rule_type", StringType(), False),
        StructField("rule", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("expectation", StringType(), False),
        StructField("action_if_failed", StringType(), False),
        StructField("tag", StringType(), False),
        StructField("description", StringType(), False),
        StructField("enable_for_source_dq_validation", BooleanType(), False),
        StructField("enable_for_target_dq_validation", BooleanType(), False),
        StructField("is_active", BooleanType(), False),
    ])
    
    return spark.createDataFrame(rules_data, rules_schema)


def main():
    """Execute Phase 1 (no DQ) and Phase 2 (with DQ) locally"""
    
    spark = SparkSession.builder \
        .appName("CrimeStatistics_DQ_Pipeline_Local") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.warehouse.dir", "output/spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Clean up any existing tables from previous runs
    spark.sql("DROP TABLE IF EXISTS dq_stats")
    spark.sql("DROP TABLE IF EXISTS offences_cleaned_with_dq")
    spark.sql("DROP TABLE IF EXISTS offences_cleaned_with_dq_error")
    
    CSV_INPUT = "data/Total_Reported_offences_1950_2023.csv"
    OUTPUT_DIR = "output"
    PHASE1_OUTPUT = f"{OUTPUT_DIR}/offences_cleaned_no_dq"
    PHASE2_OUTPUT_TABLE = "offences_cleaned_with_dq"
    STATS_TABLE = "dq_stats"
    PRODUCT_ID = "crime_dq"
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        print("\n" + "="*80)
        print("PHASE 1: Basic Data Loading and Cleaning (No DQ)")
        print("="*80)
        
        if not os.path.exists(CSV_INPUT):
            print(f"Error: CSV file not found at {CSV_INPUT}")
            sys.exit(1)
        
        df_phase1 = load_and_clean(spark, CSV_INPUT)
        
        print(f"\nWriting Phase 1 output to: {PHASE1_OUTPUT}")
        df_phase1.write.format("parquet").mode("overwrite").save(PHASE1_OUTPUT)
        print(f"Phase 1 complete. Records: {df_phase1.count()}")
        
        print("\n" + "="*80)
        print("PHASE 2: Apply Data Quality Rules")
        print("="*80)
        
        rules_df = create_sample_rules(spark)
        print(f"Created {rules_df.count()} DQ rules")
        
        df_input = spark.read.format("parquet").load(PHASE1_OUTPUT)
        
        df_phase2 = apply_dq_rules(
            spark=spark,
            product_id=PRODUCT_ID,
            df_input=df_input,
            output_table=PHASE2_OUTPUT_TABLE,
            rules_df=rules_df,
            stats_table=STATS_TABLE
        )
        
        print(f"Phase 2 complete. Clean records: {df_phase2.count()}")
        
        print(f"\nSaving outputs to {OUTPUT_DIR}...")
        df_phase2.write.format("parquet").mode("overwrite").save(f"{OUTPUT_DIR}/offences_cleaned_with_dq")
        
        try:
            stats_df = spark.table(STATS_TABLE)
            stats_df.write.format("parquet").mode("overwrite").save(f"{OUTPUT_DIR}/dq_stats")
        except:
            print("Stats table not available")
        
        get_dq_summary(spark, STATS_TABLE, PRODUCT_ID)
        
        print("\nPIPELINE COMPLETED!")
        print(f"Output saved to: {OUTPUT_DIR}/")
        
    except Exception as e:
        print(f"\nPipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()