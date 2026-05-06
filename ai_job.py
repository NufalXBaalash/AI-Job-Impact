from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():

    # ==============================
    # 1. Create Spark Session
    # ==============================
    spark = SparkSession.builder \
        .appName("AI Job Impact ETL") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("-> Spark Session Started")

    # ==============================
    # 2. Read Data from HDFS
    # ==============================
    df = spark.read.csv(
        "hdfs://hadoop-namenode:9000/data/raw/ai_job_impact.csv",
        header=True,
        inferSchema=True
    )

    print("-> Data Loaded")

    # ==============================
    # 3. Data Cleaning
    # ==============================
    df = df.dropDuplicates()

    df = df.fillna({
        "Job_Satisfaction": 0,
        "Productivity_Change_%": 0
    })

    print("Data Cleaned")

    # ==============================
    # 4. Feature Engineering
    # ==============================
    df = df.withColumn(
        "Salary_Change",
        col("Salary_After_AI") - col("Salary_Before_AI")
    )

    df = df.withColumn(
        "Salary_Change_Percent",
        (col("Salary_Change") / col("Salary_Before_AI")) * 100
    )

    print("-> Features Created")

    # ==============================
    # 5. Optimize Partitions (Important)
    # ==============================
    df = df.repartition(2)

    # ==============================
    # 6. Star Schema
    # ==============================

    # Dimension: Employee
    dim_employee = df.select(
        "Employee_ID",
        "Age",
        "Gender",
        "Education_Level"
    ).dropDuplicates()

    # Dimension: Job
    dim_job = df.select(
        "Job_Role",
        "Industry",
        "Years_Experience"
    ).dropDuplicates()

    # Dimension: AI
    dim_ai = df.select(
        "AI_Adoption_Level",
        "Automation_Risk",
        "Upskilling_Required"
    ).dropDuplicates()

    # Fact Table
    fact_table = df.select(
        "Employee_ID",
        "Job_Role",
        "AI_Adoption_Level",
        "Job_Status",
        "Salary_Before_AI",
        "Salary_After_AI",
        "Salary_Change",
        "Salary_Change_Percent",
        "Productivity_Change_%",
        "Work_Hours_Per_Week"
    )

    print("-> Star Schema Created")

    # ==============================
    # 7. Write to Data Warehouse (Parquet)
    # ==============================

    base_path = "data/warehouse/"

    dim_employee.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_employee")
    print("-> dim_employee saved")

    dim_job.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_job")
    print("-> dim_job saved")

    dim_ai.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_ai")
    print("-> dim_ai saved")

    fact_table.coalesce(1).write.mode("overwrite").parquet(base_path + "fact_table")
    print("-> fact_table saved")

    # ==============================
    # 8. Validation
    # ==============================
    print("Validation:")
    fact_table.groupBy("Job_Status").count().show()

    print("🎉 ETL Job Completed Successfully")

    spark.stop()


# ==============================
# Entry Point
# ==============================
if __name__ == "__main__":
    main()
