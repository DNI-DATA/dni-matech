FROM apache/airflow:3.0.1-python3.12

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
