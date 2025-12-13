from pyspark.sql import functions as F

ROWS = 5000

# ------------------------------------------------------------
# 1. CREATE CATALOG & SCHEMAS
# ------------------------------------------------------------
spark.sql("CREATE CATALOG IF NOT EXISTS zenithora_fictional_bank")

schemas = [
    "master", "core_banking", "payments",
    "credit", "cards", "reference"
]

for s in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS zenithora_fictional_bank.{s}")

# ------------------------------------------------------------
# 2. MASTER – CUSTOMERS
# ------------------------------------------------------------
customers_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("customer_name", F.concat(F.lit("Customer_"), F.col("customer_id")))
    .withColumn("age", (F.col("customer_id") % 45 + 21))
    .withColumn("city", F.concat(F.lit("City_"), F.col("customer_id") % 100))
    .withColumn("email", F.concat(F.lit("cust"), F.col("customer_id"), F.lit("@zenithora.com")))
)

customers_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.master.customers")

# ------------------------------------------------------------
# 3. MASTER – EMPLOYEES
# ------------------------------------------------------------
employees_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "employee_id")
    .withColumn("employee_name", F.concat(F.lit("Employee_"), F.col("employee_id")))
    .withColumn("designation",
                F.expr("CASE WHEN employee_id % 3 = 0 THEN 'Manager' "
                       "WHEN employee_id % 3 = 1 THEN 'Officer' "
                       "ELSE 'Clerk' END"))
)

employees_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.master.employees")

# ------------------------------------------------------------
# 4. REFERENCE – BRANCHES
# ------------------------------------------------------------
branches_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "branch_id")
    .withColumn("branch_name", F.concat(F.lit("ZEN_BRANCH_"), F.col("branch_id")))
    .withColumn("ifsc", F.concat(F.lit("ZEN0"), F.lpad(F.col("branch_id"), 5, "0")))
)

branches_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.reference.branches")

# ------------------------------------------------------------
# 5. CORE – ACCOUNTS
# ------------------------------------------------------------
accounts_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "account_id")
    .withColumn("customer_id", F.col("account_id"))
    .withColumn("account_type",
                F.when(F.col("account_id") % 2 == 0, "Savings").otherwise("Current"))
    .withColumn("balance", (F.rand() * 800000).cast("double"))
)

accounts_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.core_banking.accounts")

# ------------------------------------------------------------
# 6. CORE – TRANSACTIONS
# ------------------------------------------------------------
transactions_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "transaction_id")
    .withColumn("account_id", F.col("transaction_id"))
    .withColumn("txn_type",
                F.expr("CASE WHEN transaction_id % 4 = 0 THEN 'Deposit' "
                       "WHEN transaction_id % 4 = 1 THEN 'Withdrawal' "
                       "WHEN transaction_id % 4 = 2 THEN 'UPI' "
                       "ELSE 'IMPS' END"))
    .withColumn("amount", (F.rand() * 100000).cast("double"))
)

transactions_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.core_banking.transactions")

# ------------------------------------------------------------
# 7. CORE – DAILY BALANCES
# ------------------------------------------------------------
daily_bal_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "account_id")
    .withColumn("balance_date", F.current_date())
    .withColumn("end_balance", (F.rand() * 900000).cast("double"))
)

daily_bal_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.core_banking.daily_balances")

# ------------------------------------------------------------
# 8. PAYMENTS – UPI
# ------------------------------------------------------------
upi_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "upi_txn_id")
    .withColumn("account_id", F.col("upi_txn_id"))
    .withColumn("amount", (F.rand() * 50000).cast("double"))
)

upi_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.payments.upi_transactions")

# ------------------------------------------------------------
# 9. PAYMENTS – IMPS
# ------------------------------------------------------------
imps_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "imps_txn_id")
    .withColumn("account_id", F.col("imps_txn_id"))
    .withColumn("amount", (F.rand() * 70000).cast("double"))
)

imps_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.payments.imps_transactions")

# ------------------------------------------------------------
# 10. CREDIT – LOANS
# ------------------------------------------------------------
loans_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "loan_id")
    .withColumn("customer_id", F.col("loan_id"))
    .withColumn("loan_type",
                F.expr("CASE WHEN loan_id % 3 = 0 THEN 'Home' "
                       "WHEN loan_id % 3 = 1 THEN 'Personal' "
                       "ELSE 'Auto' END"))
    .withColumn("loan_amount", (F.rand() * 3000000).cast("double"))
)

loans_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.credit.loans")

# ------------------------------------------------------------
# 11. CREDIT – LOAN PAYMENTS
# ------------------------------------------------------------
loan_pay_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "payment_id")
    .withColumn("loan_id", F.col("payment_id"))
    .withColumn("amount", (F.rand() * 60000).cast("double"))
)

loan_pay_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.credit.loan_payments")

# ------------------------------------------------------------
# 12. CREDIT – CREDIT SCORES
# ------------------------------------------------------------
credit_score_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("credit_score", (F.rand() * 400 + 400).cast("int"))
)

credit_score_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.credit.credit_scores")

# ------------------------------------------------------------
# 13. CARDS – CARDS
# ------------------------------------------------------------
cards_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "card_id")
    .withColumn("customer_id", F.col("card_id"))
    .withColumn("card_type",
                F.when(F.col("card_id") % 2 == 0, "Debit").otherwise("Credit"))
)

cards_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.cards.cards")

# ------------------------------------------------------------
# 14. CARDS – CARD TRANSACTIONS
# ------------------------------------------------------------
card_txn_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "card_txn_id")
    .withColumn("card_id", F.col("card_txn_id"))
    .withColumn("amount", (F.rand() * 90000).cast("double"))
)

card_txn_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.cards.card_transactions")

# ------------------------------------------------------------
# 15. REFERENCE – ATM LOCATIONS
# ------------------------------------------------------------
atm_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "atm_id")
    .withColumn("city", F.concat(F.lit("City_"), F.col("atm_id") % 100))
)

atm_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.reference.atm_locations")

# ------------------------------------------------------------
# 16. MASTER – COMPLAINTS
# ------------------------------------------------------------
complaints_df = (
    spark.range(1, ROWS + 1)
    .withColumnRenamed("id", "complaint_id")
    .withColumn("customer_id", F.col("complaint_id"))
    .withColumn("status",
                F.expr("CASE WHEN complaint_id % 2 = 0 THEN 'Resolved' ELSE 'Open' END"))
)

complaints_df.write.format("delta").mode("overwrite") \
    .saveAsTable("zenithora_fictional_bank.master.complaints")
