from pyspark.sql import DataFrame
import pyspark.sql.functions as F

AMOUNT_TOLERANCE = 0.01
AUTO_RESOLVE_TOLERANCE = 1.00

def reconcile(internal_df: DataFrame, network_df: DataFrame) -> DataFrame:
    """
    Given internal + network DataFrames, return a recon_results DataFrame with
    match_status, amount_diff, and disposition.
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

    amount_mismatch = present_i & present_n & (F.abs(F.col("i.amount") - F.col("n.amount")) > AMOUNT_TOLERANCE)
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
    )

    # Disposition is AUTO when match_status == MISMATCH_AMOUNT and abs(amount_diff) <= AUTO_RESOLVE_TOLERANCE, else MANUAL
    recon = recon.withColumn(
        "disposition",
        F.when(
            (F.col("match_status") == "MISMATCH_AMOUNT")
            & (F.abs(F.col("amount_diff")) <= AUTO_RESOLVE_TOLERANCE),
            F.lit("AUTO"),
        ).otherwise(F.lit("MANUAL")),
    )

    return recon
