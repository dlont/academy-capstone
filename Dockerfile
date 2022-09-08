FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1


USER root
# Install apt packages and clean up cached files
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# Install the AWS CLI and clean up tmp files
RUN wget https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -O ./awscliv2.zip && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf ./aws awscliv2.zip

RUN pip install boto3 && \
      pip install aws-secretsmanager-caching && \
      pip install pyspark==3.1.2

WORKDIR .
COPY src .

RUN python src/1_3_sf.py
