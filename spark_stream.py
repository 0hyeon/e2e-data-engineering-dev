import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )
    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
        """
    )
    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")
    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                kwargs.get("id"),
                kwargs.get("first_name"),
                kwargs.get("last_name"),
                kwargs.get("gender"),
                kwargs.get("address"),
                kwargs.get("post_code"),
                kwargs.get("email"),
                kwargs.get("username"),
                kwargs.get("registered_date"),
                kwargs.get("phone"),
                kwargs.get("picture"),
            ),
        )
        logging.info(
            f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}"
        )
    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")


def create_spark_connection():
    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars",
                "./spark-sql-kafka-0-10_2.12-3.5.4.jar,./kafka-clients-3.5.1.jar",
            )
            .config(
                "spark.driver.extraClassPath",
                "./spark-sql-kafka-0-10_2.12-3.5.4.jar:./kafka-clients-3.5.1.jar",
            )
            .config(
                "spark.executor.extraClassPath",
                "./spark-sql-kafka-0-10_2.12-3.5.4.jar:./kafka-clients-3.5.1.jar",
            )
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Kafka DataFrame created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    print(sel)
    return sel


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create Spark session
    spark_conn = create_spark_connection()
    if not spark_conn:
        logging.error("Spark connection could not be established.")
        exit(1)

    # Connect to Kafka
    spark_df = connect_to_kafka(spark_conn)
    if not spark_df:
        logging.error("Kafka DataFrame could not be created.")
        exit(1)

    # Parse Kafka data into a structured DataFrame
    selection_df = create_selection_df_from_kafka(spark_df)

    # Connect to Cassandra
    cassandra_session = create_cassandra_connection()
    if not cassandra_session:
        logging.error("Cassandra connection could not be established.")
        exit(1)

    # Create keyspace and table
    create_keyspace(cassandra_session)
    create_table(cassandra_session)


# 1.created확인
# python spark_stream.py

# 카산드라 열확인 테이블 확인
# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
# SELECT * FROM  spark_streams.created_users;
# describe spark_streams.created_users;

# docker exec -it cassandra cqlsh 172.17.0.2 9042


# DESCRIBE KEYSPACES;


# 키가 있는지 확인하고싶다고
# spark-submit --master spark://localhost:7077 spark_stream.py

# 안돼서 밑에꺼
# spark-submit \
#     --master spark://localhost:7077 \
#     --jars /path/to/jars/spark-cassandra-connector_2.13-3.4.1.jar \
#     spark_stream.py


# pyspark --version
# java -version

# spark-submit \
#   --master spark://localhost:7077 \
#   spark_stream.py

# spark-submit \
#   --master spark://localhost:7077 \
#   --jars /path/to/spark-cassandra-connector_2.12-3.4.0.jar \
#   spark_stream.py

# spark-submit \
#     --master spark://localhost:7077 \
#     --jars ./spark-cassandra-connector_2.13-3.4.1.jar,./spark-sql-kafka-0-10_2.13-3.4.1.jar,./kafka-clients-3.4.0.jar,./guava-31.0.1-jre.jar,./slf4j-api-1.7.36.jar,./slf4j-simple-1.7.36.jar \
#     spark_stream.py


# spark-submit \
#   --master spark://localhost:7077 \
#   --jars ./spark-cassandra-connector_2.12-3.4.0.jar,./spark-sql-kafka-0-10_2.12-3.4.1.jar,./kafka-clients-3.4.0.jar,./guava-31.0.1-jre.jar,./slf4j-api-1.7.36.jar,./metrics-core-4.1.22.jar \
#   spark_stream.py


# (venv) (venv) gim-yeonghyeon_geulinbeuligseu@GB-mb001-wsjang e2e-data-engineering-dev % brew install apache-spark

# ==> Downloading https://formulae.brew.sh/api/formula.jws.json
# ########################################################################################################################################### 100.0%
# ==> Downloading https://formulae.brew.sh/api/cask.jws.json
# ########################################################################################################################################### 100.0%
# ==> Downloading https://ghcr.io/v2/homebrew/core/apache-spark/manifests/3.5.4
# Already downloaded: /Users/gim-yeonghyeon_geulinbeuligseu/Library/Caches/Homebrew/downloads/16f6fb8c53dc9d1ddea203d91c3830461aefb2932d9ac20777fe4abd0c70504c--apache-spark-3.5.4.bottle_manifest.json
# ==> Fetching apache-spark
# ==> Downloading https://ghcr.io/v2/homebrew/core/apache-spark/blobs/sha256:ad48e3e85e40bbe41a8174cdd9d00ea8c2fe19dbb15bfd1dbe23c2d39dd9fe4b
# Already downloaded: /Users/gim-yeonghyeon_geulinbeuligseu/Library/Caches/Homebrew/downloads/3b46d164137a4b7ecff2d1a41035038e2f65e85f295125af4a17f02bbce427b6--apache-spark--3.5.4.all.bottle.tar.gz
# ==> Pouring apache-spark--3.5.4.all.bottle.tar.gz
# Error: The `brew link` step did not complete successfully
# The formula built, but is not symlinked into /opt/homebrew
# Could not symlink bin/docker-image-tool.sh
# Target /opt/homebrew/bin/docker-image-tool.sh
# already exists. You may want to remove it:
#   rm '/opt/homebrew/bin/docker-image-tool.sh'

# To force the link and overwrite all conflicting files:
#   brew link --overwrite apache-spark

# To list all files that would be deleted:
#   brew link --overwrite apache-spark --dry-run

# Possible conflicting files are:
# /opt/homebrew/bin/docker-image-tool.sh
# /opt/homebrew/bin/find-spark-home
# /opt/homebrew/bin/load-spark-env.sh
# /opt/homebrew/bin/pyspark
# /opt/homebrew/bin/run-example
# /opt/homebrew/bin/spark-class
# /opt/homebrew/bin/spark-connect-shell
# /opt/homebrew/bin/spark-shell
# /opt/homebrew/bin/spark-sql
# /opt/homebrew/bin/spark-submit
# /opt/homebrew/bin/sparkR
# ==> Summary
# 🍺  /opt/homebrew/Cellar/apache-spark/3.5.4: 1,823 files, 423.7MB
# ==> Running `brew cleanup apache-spark`...
# Disable this behaviour by setting HOMEBREW_NO_INSTALL_CLEANUP.
# Hide these hints with HOMEBREW_NO_ENV_HINTS (see `man brew`).
# (venv) (venv) gim-yeonghyeon_geulinbeuligseu@GB-mb001-wsjang e2e-data-engineering-dev % brew info apache-spark

# ==> apache-spark: stable 3.5.4 (bottled), HEAD
# Engine for large-scale data processing
# https://spark.apache.org/
# Installed
# /opt/homebrew/Cellar/apache-spark/3.5.4 (1,823 files, 423.7MB)
#   Poured from bottle using the formulae.brew.sh API on 2025-01-14 at 16:12:32
# From: https://github.com/Homebrew/homebrew-core/blob/HEAD/Formula/a/apache-spark.rb
# License: Apache-2.0
# ==> Dependencies
# Required: openjdk@17 ✔
# ==> Options
# --HEAD
#         Install HEAD version
# ==> Analytics
# install: 2,678 (30 days), 6,592 (90 days), 26,563 (365 days)
# install-on-request: 2,676 (30 days), 6,587 (90 days), 26,543 (365 days)
# build-error: 0 (30 days)
# (venv) (venv) gim-yeonghyeon_geulinbeuligseu@GB-mb001-wsjang e2e-data-engineering-dev % export SPARK_HOME=/opt/homebrew/opt/apache-spark
# export PATH=$SPARK_HOME/bin:$PATH

# (venv) (venv) gim-yeonghyeon_geulinbeuligseu@GB-mb001-wsjang e2e-data-engineering-dev % echo 'export SPARK_HOME=/opt/homebrew/opt/apache-spark' >> ~/.zshrc
# echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
# source ~/.zshrc

# (venv) (venv) gim-yeonghyeon_geulinbeuligseu@GB-mb001-wsjang e2e-data-engineering-dev % spark-submit --version
# pyspark --version

# /opt/homebrew/opt/apache-spark/bin/load-spark-env.sh: line 2: /opt/homebrew/Cellar/apache-spark/3.5.4/libexec/bin/load-spark-env.sh: Permission denied
# /opt/homebrew/opt/apache-spark/bin/load-spark-env.sh: line 2: exec: /opt/homebrew/Cellar/apache-spark/3.5.4/libexec/bin/load-spark-env.sh: cannot execute: Undefined error: 0
# /opt/homebrew/opt/apache-spark/bin/load-spark-env.sh: line 2: /opt/homebrew/Cellar/apache-spark/3.5.4/libexec/bin/load-spark-env.sh: Permission denied
# /opt/homebrew/opt/apache-spark/bin/load-spark-env.sh: line 2: exec: /opt/homebrew/Cellar/apache-spark/3.5.4/libexec/bin/load-spark-env.sh: cannot execute: Undefined error: 0
