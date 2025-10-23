"""Phase 2: Apply data quality rules using Spark Expectations framework"""
from pyspark.sql import SparkSession, DataFrame
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from sparkdq.config import se_user_conf


def apply_dq_rules(
    spark: SparkSession,
    product_id: str,
    df_input: DataFrame,
    output_table: str,  # Changed to table name
    rules_df: DataFrame,
    stats_table: str  # Changed to table name
) -> DataFrame:
    """Apply data quality rules using Spark Expectations"""
    
    print(f"📊 Input record count: {df_input.count()}")
    print(f"📋 Loaded {rules_df.count()} DQ rules")
    
    df_input.createOrReplaceTempView("input_data")
    
    # Create writer configs
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    
    se = SparkExpectations(
        product_id=product_id,
        rules_df=rules_df,
        stats_table=stats_table,  # Now a table name
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        debugger=False,
    )
    
    @se.with_expectations(
        target_table=output_table,  # Now a table name
        write_to_table=True,
        write_to_temp_table=False,
        user_conf=se_user_conf,
        target_table_view="input_data"
    )
    def process_with_dq() -> DataFrame:
        return df_input
    
    print("🔍 Running data quality checks...")
    df_clean = process_with_dq()
    
    print(f"✅ Clean records: {df_clean.count()}")
    
    return df_clean


def get_dq_summary(spark: SparkSession, stats_table: str, product_id: str) -> None:
    """Display summary of latest DQ run"""
    try:
        stats_df = spark.table(stats_table)  # Read from table
        stats_df = stats_df.filter(f"product_id = '{product_id}'")
        
        if stats_df.count() == 0:
            print("No DQ statistics found")
            return
        
        print("\n" + "="*60)
        print("📊 DATA QUALITY SUMMARY")
        print("="*60)
        stats_df.select(
            "product_id", "input_count", "error_count", "output_count"
        ).show(truncate=False)
        
    except Exception as e:
        print(f"Could not load stats: {e}")