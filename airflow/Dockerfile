FROM apache/airflow:2.10.2-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    netcat-openbsd \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]