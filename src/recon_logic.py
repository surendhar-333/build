from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Reconciliation tolerances
AMOUNT_TOLERANCE       = 0.01
AUTO_RESOLVE_TOLERANCE = 1.00

def reconcile_dataframes(internal_df, network_df, amount_tolerance=AMOUNT_TOLERANCE):
    """
    Given internal + network DataFrames, return a recon_results DataFrame.
    """
    i = internal_df.alias("i")
    n = network_df.alias("n")

    joined = i.join(n, on=F.col("i.txn_id") == F.col("n.txn_id"), how="fullouter")

    # Side-presence flags
    present_i = F.col("i.txn_id").isNotNull()
    present_n = F.col("n.txn_id").isNotNull()

    # Coalesced descriptive columns
    txn_id        = F.coalesce(F.col("i.txn_id"), F.col("n.txn_id"))
    business_date = F.coalesce(F.col("i.business_date"), F.col("n.business_date"))
    channel       = F.coalesce(F.col("i.channel"), F.col("n.channel"))

    internal_amount = F.col("i.amount")
    network_amount  = F.col("n.amount")
    internal_status = F.col("i.status")
    network_status  = F.col("n.status")

    amount_diff = F.when(
        present_i & present_n,
        F.round(F.col("i.amount").cast("double") - F.col("n.amount").cast("double"), 2),
    ).otherwise(F.lit(None).cast("double"))

    amount_mismatch = present_i & present_n & (F.abs(F.col("i.amount") - F.col("n.amount")) > amount_tolerance)
    status_mismatch = present_i & present_n & (F.col("i.status") != F.col("n.status"))

    match_status = (
        F.when(present_i & ~present_n, F.lit("UNMATCHED_INTERNAL"))
         .when(~present_i & present_n, F.lit("UNMATCHED_NETWORK"))
         .when(amount_mismatch & status_mismatch, F.lit("MISMATCH_BOTH"))
         .when(amount_mismatch & ~status_mismatch, F.lit("MISMATCH_AMOUNT"))
         .when(~amount_mismatch & status_mismatch, F.lit("MISMATCH_STATUS"))
         .otherwise(F.lit("MATCHED"))
    )

    recon = (
        joined
        .select(
            txn_id.alias("txn_id"),
            business_date.alias("business_date"),
            channel.alias("channel"),
            internal_amount.alias("internal_amount"),
            network_amount.alias("network_amount"),
            amount_diff.alias("amount_diff"),
            internal_status.alias("internal_status"),
            network_status.alias("network_status"),
            match_status.alias("match_status"),
        )
        .withColumn(
            "reason",
            F.when(F.col("match_status") == "UNMATCHED_INTERNAL",
                   F.lit("Transaction present in internal ledger but missing from network feed"))
             .when(F.col("match_status") == "UNMATCHED_NETWORK",
                   F.lit("Transaction present in network feed but missing from internal ledger"))
             .when(F.col("match_status") == "MISMATCH_BOTH",
                   F.concat(F.lit("Amount differs by "), F.col("amount_diff").cast("string"),
                            F.lit(" and status differs ("), F.col("internal_status"),
                            F.lit(" vs "), F.col("network_status"), F.lit(")")))
             .when(F.col("match_status") == "MISMATCH_AMOUNT",
                   F.concat(F.lit("Amount differs by "), F.col("amount_diff").cast("string"),
                            F.lit(" (tolerance "), F.lit(str(amount_tolerance)), F.lit(")")))
             .when(F.col("match_status") == "MISMATCH_STATUS",
                   F.concat(F.lit("Status differs ("), F.col("internal_status"),
                            F.lit(" vs "), F.col("network_status"), F.lit(")")))
             .otherwise(F.lit("Amount and status agree within tolerance"))
        )
    )
    return recon

def derive_exception_cases(recon_results_df, auto_resolve_tolerance=AUTO_RESOLVE_TOLERANCE):
    """
    Given recon_results DataFrame, derive exceptions and compute disposition.
    """
    non_matched = recon_results_df.filter(F.col("match_status") != "MATCHED")

    # Deterministic per-date row numbering (ordered by txn_id) for stable case ids
    w = Window.partitionBy("business_date").orderBy("txn_id")

    exceptions = (
        non_matched
        .withColumn("row_number", F.row_number().over(w))
        .withColumn(
            "case_id",
            F.concat(
                F.lit("CASE-"),
                F.col("business_date").cast("string"),
                F.lit("-"),
                F.lpad(F.col("row_number").cast("string"), 8, "0"),
            ),
        )
        .withColumn("case_type", F.col("match_status"))
        .withColumn(
            "disposition",
            F.when(
                (F.col("case_type") == "MISMATCH_AMOUNT")
                & (F.abs(F.col("amount_diff")) <= auto_resolve_tolerance),
                F.lit("AUTO"),
            ).otherwise(F.lit("MANUAL")),
        )
        .withColumn("created_ts", F.current_timestamp())
        .select(
            "case_id",
            "txn_id",
            "business_date",
            "channel",
            "case_type",
            "internal_amount",
            "network_amount",
            "amount_diff",
            "internal_status",
            "network_status",
            "disposition",
            "reason",
            "created_ts",
        )
    )
    return exceptions
