from pyspark.sql import functions as F

ROWS = 10000

# ------------------------------------------------------------
# 1. CREATE CATALOG & SCHEMAS
# ------------------------------------------------------------
spark.sql("CREATE CATALOG IF NOT EXISTS aurora_fictional_bank")

schemas = ["core_banking", "credit", "cards", "master", "reference"]
for s in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS aurora_fictional_bank.{s}")

# ------------------------------------------------------------
# 2. CUSTOMERS (MASTER) – 10,000
# ------------------------------------------------------------
customers_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("customer_name", F.concat(F.lit("Customer_"), F.col("customer_id")))
    .withColumn("gender", F.when(F.col("customer_id") % 2 == 0, "Male").otherwise("Female"))
    .withColumn("age", (F.col("customer_id") % 45 + 21))
    .withColumn("city", F.concat(F.lit("City_"), (F.col("customer_id") % 100)))
    .withColumn("email", F.concat(F.lit("customer"), F.col("customer_id"), F.lit("@mail.com")))
    .withColumn("contact_number", F.concat(F.lit("9"), F.lpad(F.col("customer_id"), 9, "0")))
)

customers_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.master.customers")

# ------------------------------------------------------------
# 3. BRANCHES (REFERENCE) – 10,000
# ------------------------------------------------------------
branches_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "branch_id")
    .withColumn("branch_name", F.concat(F.lit("AURORA_BRANCH_"), F.col("branch_id")))
    .withColumn("city", F.concat(F.lit("City_"), (F.col("branch_id") % 100)))
    .withColumn("ifsc_code", F.concat(F.lit("AUR0"), F.lpad(F.col("branch_id"), 5, "0")))
)

branches_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.reference.branches")

# ------------------------------------------------------------
# 4. ACCOUNTS (CORE) – 10,000
# ------------------------------------------------------------
accounts_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "account_id")
    .withColumn("customer_id", (F.col("account_id") % ROWS + 1))
    .withColumn("account_type",
                F.when(F.col("account_id") % 2 == 0, "Savings").otherwise("Current"))
    .withColumn("balance", (F.rand() * 1_000_000).cast("double"))
    .withColumn("branch_id", (F.col("account_id") % ROWS + 1))
)

accounts_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.core_banking.accounts")

# ------------------------------------------------------------
# 5. TRANSACTIONS (CORE) – 10,000
# ------------------------------------------------------------
transactions_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "transaction_id")
    .withColumn("account_id", (F.col("transaction_id") % ROWS + 1))
    .withColumn(
        "transaction_type",
        F.expr("""
            CASE
                WHEN transaction_id % 5 = 0 THEN 'UPI'
                WHEN transaction_id % 5 = 1 THEN 'Withdrawal'
                WHEN transaction_id % 5 = 2 THEN 'Deposit'
                WHEN transaction_id % 5 = 3 THEN 'IMPS'
                ELSE 'NEFT'
            END
        """)
    )
    .withColumn("transaction_amount", (F.rand() * 150000).cast("double"))
    .withColumn(
        "transaction_date",
        F.date_sub(
            F.current_date(),
            (F.col("transaction_id") % 365).cast("int")
        )
    )
)

transactions_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.core_banking.transactions")


# ------------------------------------------------------------
# 6. LOANS (CREDIT) – 10,000
# ------------------------------------------------------------
loans_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "loan_id")
    .withColumn("customer_id", (F.col("loan_id") % ROWS + 1))
    .withColumn("loan_type",
                F.expr("CASE WHEN loan_id % 4 = 0 THEN 'Home Loan' "
                       "WHEN loan_id % 4 = 1 THEN 'Personal Loan' "
                       "WHEN loan_id % 4 = 2 THEN 'Auto Loan' "
                       "ELSE 'Education Loan' END"))
    .withColumn("loan_amount", (F.rand() * 5_000_000).cast("double"))
    .withColumn("loan_status",
                F.expr("CASE WHEN loan_id % 4 = 0 THEN 'Approved' "
                       "WHEN loan_id % 4 = 1 THEN 'Rejected' "
                       "WHEN loan_id % 4 = 2 THEN 'Active' "
                       "ELSE 'Closed' END"))
)

loans_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.credit.loans")

# ------------------------------------------------------------
# 7. CARD TRANSACTIONS (CARDS) – 10,000
# ------------------------------------------------------------
card_txns_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "card_txn_id")
    .withColumn("card_id", (F.col("card_txn_id") % ROWS + 1))
    .withColumn("transaction_amount", (F.rand() * 100_000).cast("double"))
    .withColumn(
        "transaction_date",
        F.date_sub(
            F.current_date(),
            (F.col("card_txn_id") % 365).cast("int")
        )
    )
)

card_txns_df.write.format("delta").mode("overwrite") \
    .saveAsTable("aurora_fictional_bank.cards.card_transactions")