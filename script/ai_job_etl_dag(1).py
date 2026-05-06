from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# ==============================
# Default Arguments
# ==============================
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ==============================
# DAG Definition
# ==============================
with DAG(
    dag_id="ai_job_impact_etl",
    default_args=default_args,
    description="ETL Pipeline for AI Job Impact Dataset",
    schedule_interval="0 6 * * *",   # كل يوم الساعة 6 الصبح
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "spark", "ai-job"],
) as dag:

    # ==============================
    # Task 1: Health Check
    # ==============================
    health_check = BashOperator(
        task_id="health_check",
        bash_command="""
            echo "🔍 Checking environment..."
            which spark-submit || (echo "❌ spark-submit not found!" && exit 1)
            echo "✅ spark-submit found"
            ls /root/work/data/raw/ai_job_impact.csv || (echo "❌ Data file not found!" && exit 1)
            echo "✅ Data file exists"
            echo "✅ Health Check Passed"
        """,
    )

    # ==============================
    # Task 2: Run ETL with Spark
    # ==============================
    run_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            echo "🚀 Starting Spark ETL..."
            spark-submit \
                --master local[*] \
                --driver-memory 2g \
                --conf spark.sql.shuffle.partitions=4 \
                /root/work/ai_job.py
            echo "✅ Spark ETL Completed"
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # ==============================
    # Task 3: Validate Output
    # ==============================
    def validate_warehouse():
        """Check that all warehouse tables were created"""
        base_path = "/root/work/data/warehouse"
        expected_tables = ["dim_employee", "dim_job", "dim_ai", "fact_table"]
        
        missing = []
        for table in expected_tables:
            table_path = os.path.join(base_path, table)
            if not os.path.exists(table_path):
                missing.append(table)
            else:
                files = os.listdir(table_path)
                parquet_files = [f for f in files if f.endswith(".parquet")]
                print(f"✅ {table}: {len(parquet_files)} parquet file(s) found")
        
        if missing:
            raise ValueError(f"❌ Missing tables: {missing}")
        
        print("🎉 All warehouse tables validated successfully!")

    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_warehouse,
    )

    # ==============================
    # Task 4: Done Notification
    # ==============================
    notify_done = BashOperator(
        task_id="notify_done",
        bash_command="""
            echo "========================================="
            echo "🎉 ETL Pipeline Completed Successfully!"
            echo "📅 Date: $(date)"
            echo "📦 Tables:"
            ls /root/work/data/warehouse/
            echo "========================================="
        """,
    )

    # ==============================
    # Pipeline Flow
    # ==============================
    health_check >> run_etl >> validate_output >> notify_done
