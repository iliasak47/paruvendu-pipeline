"""
DAG principal : Pipeline ParuVendu Rouen
Orchestration du flux complet :
scrape → parse → transform → historize → dbt_run → dbt_test
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# ---------------------------------------------------------------------
# CONFIGURATION DU DAG
# ---------------------------------------------------------------------
default_args = {
    "owner": "cytech_student",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="paruvendu_pipeline",
    description="Pipeline complet ParuVendu Rouen (Scrape → Parse → Transform → Historize → dbt)",
    default_args=default_args,
    schedule="0 22 * * 0",  # tous les dimanches à 22h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["paruvendu", "data-engineering", "aws", "spark", "dbt"],
) as dag:

    # -----------------------------------------------------------------
    # 1. Scraping → Raw
    # -----------------------------------------------------------------
    scrape = BashOperator(
        task_id="scrape",
        bash_command=(
            "python /opt/airflow/dags/scripts/S3_scrape_paruvendu.py "
            "&& echo ' Scraping terminé avec succès.'"
        ),
    )

    # -----------------------------------------------------------------
    # 2. Parsing HTML → Silver
    # -----------------------------------------------------------------
    parse = BashOperator(
        task_id="parse",
        bash_command=(
            "python /opt/airflow/dags/scripts/S3_parse_paruvendu.py "
            "&& echo ' Parsing terminé avec succès.'"
        ),
    )

    # -----------------------------------------------------------------
    # 3. Transformation Silver → Gold (PySpark)
    # -----------------------------------------------------------------
    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command=(
            "python /opt/airflow/dags/scripts/S3_transform_paruvendu.py "
            "&& echo ' Transformation Gold terminée avec succès.'"
        ),
    )

    # -----------------------------------------------------------------
    # 4. Historisation Gold → History (PySpark)
    # -----------------------------------------------------------------
    historize = BashOperator(
        task_id="historize",
        bash_command=(
            "python /opt/airflow/dags/scripts/S3_historize_paruvendu.py "
            "&& echo ' Historisation terminée avec succès.'"
        ),
    )

    # -----------------------------------------------------------------
    # 5. Modélisation, tests et freshness de données dbt sur Athena
    # -----------------------------------------------------------------
    dbt_freshness = BashOperator(
        task_id="dbt_freshness",
        bash_command=(
            "cd /opt/airflow/dags/dbt && "
            "dbt source freshness --profiles-dir /opt/airflow/.dbt "
            "&& echo ' dbt source freshness terminé avec succès.'"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dags/dbt && "
            "dbt run --profiles-dir /opt/airflow/.dbt "
            "&& echo ' dbt run terminé avec succès.'"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dags/dbt && "
            "dbt test --profiles-dir /opt/airflow/.dbt "
            "&& echo ' dbt test terminé avec succès.'"
        ),
    )

    # -----------------------------------------------------------------
    # DÉPENDANCES ENTRE LES TÂCHES
    # -----------------------------------------------------------------
    scrape >> parse >> transform_gold >> historize >> dbt_freshness >> dbt_run >> dbt_test
