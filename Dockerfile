# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.10-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/home/airflow

ENV PYTHONPATH=/home/airflow/servierproject

# Install pip libs
COPY ./requirements.txt /home/airflow/servierproject/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /home/airflow/servierproject/requirements.txt

# Init airflow
RUN airflow db init \
    && airflow users create --username admin --password admin --firstname Will --lastname Smith --role Admin --email wm@c.org

WORKDIR /home

COPY ./airflow_start_docker.sh /home/airflow/airflow_start_docker.sh
ENTRYPOINT [ "/home/airflow/airflow_start_docker.sh" ]
