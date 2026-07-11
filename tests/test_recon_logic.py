import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from decimal import Decimal
from src.recon_logic import reconcile

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("ReconLogicTests") \
        .getOrCreate()

@pytest.fixture
def schema():
    return StructType([
        StructField("txn_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("status", StringType(), True),
    ])

def test_reconcile_exact_match(spark, schema):
    internal_data = [("1", "2023-01-01", "POS", Decimal("100.00"), "SETTLED")]
    network_data = [("1", "2023-01-01", "POS", Decimal("100.00"), "SETTLED")]

    internal_df = spark.createDataFrame(internal_data, schema=schema)
    network_df = spark.createDataFrame(network_data, schema=schema)

    result = reconcile(internal_df, network_df)
    row = result.filter(result.txn_id == "1").collect()[0]

    assert row.match_status == "MATCHED"

def test_reconcile_all_cases(spark):
    from pyspark.sql.types import DoubleType

    schema_d = StructType([
        StructField("txn_id", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])

    internal_data = [
        ("1", "2023-01-01", "POS", 100.00, "SETTLED"),   # Exact match
        ("2", "2023-01-01", "POS", 100.005, "SETTLED"),  # amount diff 0.005 -> MATCHED
        ("3", "2023-01-01", "POS", 100.50, "SETTLED"),   # amount diff 0.50 -> MISMATCH_AMOUNT (AUTO)
        ("4", "2023-01-01", "POS", 105.00, "SETTLED"),   # amount diff 5.00 -> MISMATCH_AMOUNT (MANUAL)
        ("5", "2023-01-01", "POS", 100.00, "SETTLED"),   # MISMATCH_STATUS
        ("6", "2023-01-01", "POS", 105.00, "SETTLED"),   # MISMATCH_BOTH
        ("7", "2023-01-01", "POS", 100.00, "SETTLED"),   # UNMATCHED_INTERNAL
        # "8" will be UNMATCHED_NETWORK
    ]

    network_data = [
        ("1", "2023-01-01", "POS", 100.00, "SETTLED"),
        ("2", "2023-01-01", "POS", 100.00, "SETTLED"),
        ("3", "2023-01-01", "POS", 100.00, "SETTLED"),
        ("4", "2023-01-01", "POS", 100.00, "SETTLED"),
        ("5", "2023-01-01", "POS", 100.00, "PENDING"),
        ("6", "2023-01-01", "POS", 100.00, "PENDING"),
        # "7" missing from network
        ("8", "2023-01-01", "POS", 100.00, "SETTLED"),   # UNMATCHED_NETWORK
    ]

    internal_df = spark.createDataFrame(internal_data, schema=schema_d)
    network_df = spark.createDataFrame(network_data, schema=schema_d)

    result = reconcile(internal_df, network_df)

    rows = {r.txn_id: r for r in result.collect()}

    # 1: Exact match -> MATCHED
    assert rows["1"].match_status == "MATCHED"

    # 2: amount diff of 0.005 -> MATCHED
    assert rows["2"].match_status == "MATCHED"

    # 3: amount diff of 0.50 -> MISMATCH_AMOUNT (AUTO)
    assert rows["3"].match_status == "MISMATCH_AMOUNT"
    assert rows["3"].disposition == "AUTO"
    assert abs(rows["3"].amount_diff - 0.50) < 0.001

    # 4: amount diff of 5.00 -> MISMATCH_AMOUNT (MANUAL)
    assert rows["4"].match_status == "MISMATCH_AMOUNT"
    assert rows["4"].disposition == "MANUAL"
    assert abs(rows["4"].amount_diff - 5.00) < 0.001

    # 5: MISMATCH_STATUS
    assert rows["5"].match_status == "MISMATCH_STATUS"

    # 6: MISMATCH_BOTH
    assert rows["6"].match_status == "MISMATCH_BOTH"

    # 7: UNMATCHED_INTERNAL
    assert rows["7"].match_status == "UNMATCHED_INTERNAL"

    # 8: UNMATCHED_NETWORK
    assert rows["8"].match_status == "UNMATCHED_NETWORK"
