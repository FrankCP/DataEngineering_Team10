FROM apache/airflow:2.10.2-python3.10

USER root

# Install tools
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Temurin Java 11 (Spark-compatible)
RUN curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" \
       > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 🔴 CRITICAL ENV VARS
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
