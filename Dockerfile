FROM apache/airflow:3.0.6
ENV PYTHONPATH /opt/airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
