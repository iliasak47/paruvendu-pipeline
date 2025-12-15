# Étend l'image officielle Airflow
FROM apache/airflow:3.1.0

# Passe à l'utilisateur root pour installer Java
USER root

# Installe OpenJDK 17 et le paquet ps (nécessaire à Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Définit JAVA_HOME pour Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Reviens à l'utilisateur airflow
USER airflow

# Installe les librairies Python nécessaires
RUN pip install --no-cache-dir pyspark==3.5.2 boto3

# Install dbt-core and dbt-athena
RUN pip install --no-cache-dir dbt-core==1.10.13 dbt-athena-community==1.9.5


