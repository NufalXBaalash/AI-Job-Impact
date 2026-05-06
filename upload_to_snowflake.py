import snowflake.connector
import pandas as pd
import os

# ==============================
# Snowflake Connection
# ==============================
conn = snowflake.connector.connect(
    user="YOTARO2214",
    password="Bignufal14102005",
    account="vfhpeld-jx54807.eu-central-1",
)

cursor = conn.cursor()
print("✅ Connected to Snowflake")

# ==============================
# Setup Database & Schema
# ==============================
cursor.execute("CREATE DATABASE IF NOT EXISTS AI_JOB_DWH")
cursor.execute("USE DATABASE AI_JOB_DWH")
cursor.execute("CREATE SCHEMA IF NOT EXISTS STAR_SCHEMA")
cursor.execute("USE SCHEMA STAR_SCHEMA")
print("✅ Database & Schema Ready")

# ==============================
# Helper: Read Parquet Table
# ==============================
def read_parquet_table(table_name):
    base_path = f"/home/jovyan/work/data/warehouse/{table_name}"
    files = [f for f in os.listdir(base_path) if f.endswith(".parquet")]
    df = pd.read_parquet(os.path.join(base_path, files[0]))
    print(f"✅ Read {table_name}: {len(df)} rows")
    return df

# ==============================
# Helper: Upload DataFrame
# ==============================
def upload_table(df, table_name, create_sql):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(create_sql)

    cols = ", ".join(["%s"] * len(df.columns))
    rows = [tuple(row) for row in df.itertuples(index=False)]
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES ({cols})",
        rows
    )
    print(f"✅ Uploaded {table_name}: {len(df)} rows")

# ==============================
# 1. DIM_EMPLOYEE
# ==============================
dim_employee = read_parquet_table("dim_employee")
upload_table(dim_employee, "DIM_EMPLOYEE", """
    CREATE TABLE DIM_EMPLOYEE (
        EMPLOYEE_ID     VARCHAR(50),
        AGE             INT,
        GENDER          VARCHAR(20),
        EDUCATION_LEVEL VARCHAR(50)
    )
""")

# ==============================
# 2. DIM_JOB
# ==============================
dim_job = read_parquet_table("dim_job")
upload_table(dim_job, "DIM_JOB", """
    CREATE TABLE DIM_JOB (
        JOB_ROLE         VARCHAR(100),
        INDUSTRY         VARCHAR(100),
        YEARS_EXPERIENCE INT
    )
""")

# ==============================
# 3. DIM_AI
# ==============================
dim_ai = read_parquet_table("dim_ai")
upload_table(dim_ai, "DIM_AI", """
    CREATE TABLE DIM_AI (
        AI_ADOPTION_LEVEL  VARCHAR(50),
        AUTOMATION_RISK    VARCHAR(50),
        UPSKILLING_REQUIRED VARCHAR(10)
    )
""")

# ==============================
# 4. FACT_TABLE
# ==============================
fact_table = read_parquet_table("fact_table")
upload_table(fact_table, "FACT_TABLE", """
    CREATE TABLE FACT_TABLE (
        EMPLOYEE_ID            VARCHAR(50),
        JOB_ROLE               VARCHAR(100),
        AI_ADOPTION_LEVEL      VARCHAR(50),
        JOB_STATUS             VARCHAR(50),
        SALARY_BEFORE_AI       FLOAT,
        SALARY_AFTER_AI        FLOAT,
        SALARY_CHANGE          FLOAT,
        SALARY_CHANGE_PERCENT  FLOAT,
        PRODUCTIVITY_CHANGE    FLOAT,
        WORK_HOURS_PER_WEEK    FLOAT
    )
""")

# ==============================
# Validation
# ==============================
print("\n📊 Snowflake Validation:")
for table in ["DIM_EMPLOYEE", "DIM_JOB", "DIM_AI", "FACT_TABLE"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  ✅ {table}: {count} rows")

print("\n🎉 All Data Loaded to Snowflake Successfully!")
cursor.close()
conn.close()
