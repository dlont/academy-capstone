FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

USER root

RUN pip install boto3 && \
      pip install aws-secretsmanager-caching && \
      pip install pyspark==3.1.2

WORKDIR /app

COPY src .

CMD [ "python3", "/app/1_3_sf.py"]
