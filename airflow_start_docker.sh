#!/bin/bash
airflow scheduler -D &
airflow webserver --port 8080 -D &

tail -f /dev/null & wait
