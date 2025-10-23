"""Configuration for Spark Expectations DQ framework"""
from spark_expectations.config.user_config import Constants as user_config

se_user_conf = {
    # Disable all notifications
    user_config.se_notifications_enable_email: False,
    user_config.se_notifications_enable_slack: False,
    user_config.se_notifications_enable_teams: False,
    user_config.se_notifications_on_start: False,
    user_config.se_notifications_on_completion: False,
    user_config.se_notifications_on_fail: False,
    
    # Disable error threshold notifications
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: False,
    
    # Enable basic error tracking
    user_config.se_enable_error_table: True,
    user_config.se_enable_query_dq_detailed_result: True,
    user_config.se_enable_agg_dq_detailed_result: True,
    
    # CRITICAL: Disable Databricks-specific features
    user_config.se_enable_streaming: False,
}

stats_table_writer_config = {
    "mode": "Overwrite",
    "format": "parquet",
}

target_and_error_table_writer_config = {
    "mode": "overwrite", 
    "format": "parquet"
}