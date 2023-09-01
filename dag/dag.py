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
    @task(task_id="full_ETL_pipeline")
    def full_etl_pipeline(ds=None, **kwargs):
        """
        All the dag is done in a single task. 
        The dag construction will depend on the technology used to compute and mostly the need. 
        i.e. The preprocessed data are usually computed by batch/stream and stored in a partitioned temp output.
        In this case, I wanted to keep a simple code and avoid multiple write/read of temp dataframes 
        or passing huge amount of data through the xcom memory.  
        """
        log.info("starting process")
        main_compute("../data/", "../output/test.json")
        log.info("The process has ended successfully")

    run_this = full_etl_pipeline()
