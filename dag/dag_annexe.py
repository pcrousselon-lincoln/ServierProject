from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.decorators import task
import pendulum

from src.main import main_compute

log = logging.getLogger(__name__)


default_args = {
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': "paulcamille.rousselon@mel.alten.fr"
}

with DAG(
    'DRUG_CITATION',
    schedule_interval='0 8 * * *',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    doc_md=open('../README.md', 'r').read()
) as dag:
    @task(task_id="post_extraction")
    def post_extraction(ds=None, **kwargs):
        """

        """
        log.info("starting process")
        post_extraction("../output/test.json", "../output/test.json")
        log.info("The process has ended successfully")

    run_this = post_extraction()
