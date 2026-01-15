FROM quay.io/astronomer/astro-runtime:12.11.0-python-3.12-base

USER root
WORKDIR /usr/local/airflow
COPY dbt_project ./dbt_project

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt && \
    cd dbt_project && dbt deps && cd .. && \
    deactivate
