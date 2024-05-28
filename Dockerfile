FROM quay.io/astronomer/astro-runtime:11.3.0

USER root
RUN apt-get update && apt-get install -y libgomp1
USER airflow
