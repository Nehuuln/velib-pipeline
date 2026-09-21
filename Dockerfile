FROM apache/airflow:3.3.2

COPY requirements.txt /requirements.txt
RUN PY_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && pip install --no-cache-dir -r /requirements.txt \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-${PY_VERSION}.txt"
