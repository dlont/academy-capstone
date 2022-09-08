FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
FROM python:3.8-slim-buster

USER root

RUN pip install boto3 && \
      pip install aws-secretsmanager-caching && \
      pip install pyspark==3.1.2

WORKDIR .

COPY src .

RUN python src/1_3_sf.py
