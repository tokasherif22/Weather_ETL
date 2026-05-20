FROM apache/airflow:2.8.1

USER root

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    requests \
    psycopg2-binary \
    pandas \
    pyarrow