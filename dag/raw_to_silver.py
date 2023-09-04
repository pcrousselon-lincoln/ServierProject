from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.decorators import task
import pendulum

from src.main import raw_to_silver

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
    @task(task_id="raw_to_silver_pubmed")
    def raw_to_silver_pubmed(ds=None, **kwargs):
        """
        preprocess the raw data and store it in silver environment for further use
        """
        log.info("starting process")
        raw_to_silver("../data/", "../silver/", "pubmed")
        log.info("The process has ended successfully")
        
    raw_to_silver_pubmed = raw_to_silver_pubmed()
    
    @task(task_id="raw_to_silver_clinical_trials")
    def raw_to_silver_clinical_trials(ds=None, **kwargs):
        """
        preprocess the raw data and store it in silver environment for further use
        """
        log.info("starting process")
        raw_to_silver("../data/", "../silver/", "clinical_trials")
        log.info("The process has ended successfully")

    raw_to_silver_clinical_trials = raw_to_silver_clinical_trials()

    @task(task_id="raw_to_silver_drugs")
    def raw_to_silver_drugs(ds=None, **kwargs):
        """
        Drugs should already be in the silver or gold environment anyway sice it's a ref table. 
        """
        log.info("starting process")
        raw_to_silver("../data/", "../silver/", "drugs")
        log.info("The process has ended successfully")

    raw_to_silver_drugs = raw_to_silver_drugs()
    
