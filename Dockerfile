FROM apache/airflow:2.8.1-python3.11
ENV PYTHONPATH /opt/airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
