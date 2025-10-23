"""DQ set up contains all dq rules for the crime statistics dataset"""
CREATE TABLE IF NOT EXISTS workspace.crimestatics.dq_rules (
    product_id STRING,
    table_name STRING,
    rule_type STRING,                           -- 'row_dq', 'agg_dq', or 'query_dq'
    rule STRING,                                -- Unique rule identifier
    column_name STRING,                         -- Column(s) being validated
    expectation STRING,                         -- The actual validation logic
    action_if_failed STRING,                    -- 'ignore', 'drop', or 'fail'
    tag STRING,                                 -- Category: 'validity', 'accuracy', 'completeness', etc.
    description STRING,                         -- Human-readable description
    enable_for_source_dq_validation BOOLEAN,    -- Run on source/input data
    enable_for_target_dq_validation BOOLEAN,    -- Run on target/output data
    is_active BOOLEAN                           -- Enable/disable rule
);

-- Row-level DQ: Year must not be null
INSERT INTO workspace.crimestatics.dq_rules VALUES
(
    'crime_dq',
    'total_reported_offences_1950_2023',
    'row_dq',
    'year_not_null',
    'Year',
    'Year is not null',
    'drop',
    'completeness',
    'Ensure Year column has no null values',
    true,
    true,
    true
);

-- Row-level DQ: Year must be within valid range (1950-2023)
INSERT INTO workspace.crimestatics.dq_rules VALUES
(
    'crime_dq',
    'total_reported_offences_1950_2023',
    'row_dq',
    'year_valid_range',
    'Year',
    'Year >= 1950 AND Year <= 2023',
    'drop',
    'validity',
    'Year must be between 1950 and 2023',
    true,
    true,
    true
);

-- Row-level DQ: Total crimes must be positive
INSERT INTO workspace.crimestatics.dq_rules VALUES
(
    'crime_dq',
    'total_reported_offences_1950_2023',
    'row_dq',
    'total_crimes_positive',
    'Total number of crimes',
    '`Total number of crimes` > 0',
    'drop',
    'validity',
    'Total number of crimes must be greater than zero',
    true,
    true,
    true
);

-- Aggregate DQ: Check total record count (should be 74 years: 1950-2023)
INSERT INTO workspace.crimestatics.dq_rules VALUES
(
    'crime_dq',
    'total_reported_offences_1950_2023',
    'agg_dq',
    'expected_year_count',
    'Year',
    'count(distinct Year) = 74',
    'ignore',
    'completeness',
    'Should have exactly 74 distinct years (1950-2023)',
    true,
    false,
    true
);

-- Aggregate DQ: No duplicate years
INSERT INTO workspace.crimestatics.dq_rules VALUES
(
    'crime_dq',
    'total_reported_offences_1950_2023',
    'agg_dq',
    'no_duplicate_years',
    'Year',
    'count(*) = count(distinct Year)',
    'fail',
    'uniqueness',
    'Each year should appear exactly once',
    true,
    true,
    true
);