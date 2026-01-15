FROM astrocrpublic.azurecr.io/runtime:3.1-9

USER root
WORKDIR /usr/local/airflow
COPY requirements.txt /usr/local/airflow/requirements.txt
COPY dbt_project /usr/local/airflow/dbt_project

RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt && \
    python -m venv /usr/local/airflow/dbt_venv && \
    . /usr/local/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir -r /usr/local/airflow/dbt_project/dbt-requirements.txt && \
    cd /usr/local/airflow/dbt_project && dbt deps && cd /usr/local/airflow && \
    deactivate
