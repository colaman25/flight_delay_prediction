FROM apache/spark:3.5.1

USER root
WORKDIR /opt/spark/app

RUN curl -fLo /opt/spark/jars/hadoop-aws.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fLo /opt/spark/jars/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash", "-c", "/opt/spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 train_models.py"]