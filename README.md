# Realtime Data Streaming | End-to-End Data Engineering Project

## Table of Contents

- [Introduction](#introduction)
- [시스템 아키텍처](#시스템아키텍처)
- [사용스택](#사용스택)
- [Watch the Video Tutorial](#watch-the-video-tutorial)
- [주요명령어](#주요명령어)

## Introduction

음 ETL 파이프라인을 구축하는데에있어서, 데이터 수집부터 처리, 그리고 최종 저장에 이르는 엔드 투 엔드 데이터 엔지니어링 파이프라인 대한 포괄적인 가이드를 제공합니다. 

python을 기반으로 Apache Airflow, Apache Kafka, Apache Zookeeper, Apache Spark, Cassandra 등으로 구성된 강력한 기술 스택을 활용하며, 모든 구성 요소는 Docker를 사용하여 컨테이너화되어 배포와 확장성을 고려하였습니다.

## 시스템아키텍처

![System Architecture](https://github.com/airscholar/e2e-data-engineering/blob/main/Data%20engineering%20architecture.png)


## 사용스택

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- Docker


## Watch the Video Tutorial

[관련 유튜브 링크](https://www.youtube.com/watch?v=GqAcTrqKcrY).

## 유의사항

1. localhost:9092 -> broker:29092 부분 치환

   1. producer부분
     
      ```bash
      producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
      ```

   2. spark_stream.py 부분

      ```bash
      spark_df = (
         spark_conn.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "broker:29092") #V 
         .option("subscribe", "userscreated")
         .load()
      )

      ```


## 주요명령어

1. 실행
   
   ```bash
   python spark_stream.py
   
   spark-submit --master spark://localhost:7077 spark_stream.py
   ```

2. 키스페이스확인
   
   ```bash
   DESCRIBE KEYSPACES;
   ```

3. cassandra 접속

   ```bash
   docker exec -it cassandra cqlsh -u cassandra localhost 9042
   
   docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
   ```

4. 테이블속성확인 [ created_users ]
   ```bash
   describe spark_streams.created_users;
   DESCRIBE TABLE spark_streams.created_users;
   ```
5. 키스페이스 사용 [ spark_streams ]
   ```bash
   USE spark_streams;
   ```

6. 테이블확인
   ```bash
   DESCRIBE TABLES;
   
   SELECT * FROM spark_streams.created_users;
   ```

7. 테이블드랍
   ```bash
   DROP TABLE created_users;
   ```

8. 테이블 ROWS counts 확인
   ```bash
   SELECT COUNT(*) FROM spark_streams.created_users;
   ``` 

9. 종속성확인
   ```bash
   spark-submit --version
   ```

10. 종속성 삽입 실행
      ```bash
      spark-submit \
         --master spark://localhost:7077 \
         --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,org.apache.commons:commons-pool2:2.11.1 \
         spark_stream.py


      ```

11. #producer확인 로그
      ```bash
      docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic userscreated --from-beginning

      ```
 
12. #실행환경확인
   ```bash 
   echo $PYTHONPATH
   ```
