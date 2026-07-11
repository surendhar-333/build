import pytest
from pyspark.sql import SparkSession
from src.recon_logic import reconcile_dataframes, derive_exception_cases

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("ReconLogicTests") \
        .getOrCreate()

def test_reconcile_exact_match(spark):
    internal_data = [("txn_1", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_1", "2023-10-01", "WEB", 100.0, "SETTLED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MATCHED"
    assert results[0].amount_diff == 0.0

def test_reconcile_amount_within_tolerance(spark):
    # Diff is 0.005 which is <= 0.01 tolerance
    internal_data = [("txn_2", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_2", "2023-10-01", "WEB", 99.995, "SETTLED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MATCHED"

def test_reconcile_amount_mismatch(spark):
    # Diff is 0.50 which is > 0.01 tolerance
    internal_data = [("txn_3", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_3", "2023-10-01", "WEB", 99.5, "SETTLED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MISMATCH_AMOUNT"
    assert results[0].amount_diff == 0.50

def test_reconcile_status_mismatch(spark):
    internal_data = [("txn_4", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_4", "2023-10-01", "WEB", 100.0, "FAILED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MISMATCH_STATUS"

def test_reconcile_mismatch_both(spark):
    internal_data = [("txn_5", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_5", "2023-10-01", "WEB", 90.0, "FAILED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MISMATCH_BOTH"

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def test_reconcile_unmatched_internal(spark):
    internal_data = [("txn_6", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = []
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    i_df = spark.createDataFrame(internal_data, schema)
    n_df = spark.createDataFrame(network_data, schema)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "UNMATCHED_INTERNAL"
    assert results[0].amount_diff is None

def test_reconcile_unmatched_network(spark):
    internal_data = []
    network_data = [("txn_7", "2023-10-01", "WEB", 100.0, "SETTLED")]
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    i_df = spark.createDataFrame(internal_data, schema)
    n_df = spark.createDataFrame(network_data, schema)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "UNMATCHED_NETWORK"
    assert results[0].amount_diff is None

def test_derive_exception_cases_auto_disposition(spark):
    # Setup MISMATCH_AMOUNT with diff 0.50 (<= 1.00 AUTO_RESOLVE_TOLERANCE)
    recon_data = [
        ("txn_8", "2023-10-01", "WEB", 100.0, 99.5, 0.50, "SETTLED", "SETTLED", "MISMATCH_AMOUNT", "reason")
    ]
    columns = ["txn_id", "business_date", "channel", "internal_amount", "network_amount", "amount_diff", "internal_status", "network_status", "match_status", "reason"]
    recon_df = spark.createDataFrame(recon_data, columns)

    exceptions_df = derive_exception_cases(recon_df)
    results = exceptions_df.collect()

    assert len(results) == 1
    assert results[0].case_type == "MISMATCH_AMOUNT"
    assert results[0].disposition == "AUTO"
    assert results[0].case_id.startswith("CASE-2023-10-01-")

def test_derive_exception_cases_manual_disposition(spark):
    # Setup MISMATCH_AMOUNT with diff 5.00 (> 1.00 AUTO_RESOLVE_TOLERANCE)
    recon_data = [
        ("txn_9", "2023-10-01", "WEB", 100.0, 95.0, 5.00, "SETTLED", "SETTLED", "MISMATCH_AMOUNT", "reason")
    ]
    columns = ["txn_id", "business_date", "channel", "internal_amount", "network_amount", "amount_diff", "internal_status", "network_status", "match_status", "reason"]
    recon_df = spark.createDataFrame(recon_data, columns)

    exceptions_df = derive_exception_cases(recon_df)
    results = exceptions_df.collect()

    assert len(results) == 1
    assert results[0].case_type == "MISMATCH_AMOUNT"
    assert results[0].disposition == "MANUAL"

def test_derive_exception_cases_manual_disposition_other_status(spark):
    # Setup MISMATCH_STATUS (always MANUAL)
    recon_data = [
        ("txn_10", "2023-10-01", "WEB", 100.0, 100.0, 0.0, "SETTLED", "FAILED", "MISMATCH_STATUS", "reason")
    ]
    columns = ["txn_id", "business_date", "channel", "internal_amount", "network_amount", "amount_diff", "internal_status", "network_status", "match_status", "reason"]
    recon_df = spark.createDataFrame(recon_data, columns)

    exceptions_df = derive_exception_cases(recon_df)
    results = exceptions_df.collect()

    assert len(results) == 1
    assert results[0].disposition == "MANUAL"

def test_reconcile_amount_within_tolerance_precision(spark):
    # Diff is 0.005 which is <= 0.01 tolerance
    internal_data = [("txn_2", "2023-10-01", "WEB", 100.0, "SETTLED")]
    network_data = [("txn_2", "2023-10-01", "WEB", 99.995, "SETTLED")]
    columns = ["txn_id", "business_date", "channel", "amount", "status"]

    i_df = spark.createDataFrame(internal_data, columns)
    n_df = spark.createDataFrame(network_data, columns)

    recon_df = reconcile_dataframes(i_df, n_df)
    results = recon_df.collect()

    assert len(results) == 1
    assert results[0].match_status == "MATCHED"
    assert results[0].amount_diff == 0.0 # Since PySpark's round rounds .005 to 0.0 (half to even)
