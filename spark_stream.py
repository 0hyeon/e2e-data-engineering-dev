import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cassandra.policies import RoundRobinPolicy


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
        id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """
    )

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    postcode = kwargs.get("post_code")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                user_id,
                first_name,
                last_name,
                gender,
                address,
                postcode,
                email,
                username,
                dob,
                registered_date,
                phone,
                picture,
            ),
        )
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"could not insert data due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.kafka:kafka-clients:3.4.0,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,"
                "org.apache.commons:commons-pool2:2.11.1",
            )
            .config("spark.cassandra.connection.host", "localhost ")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            # .option("kafka.bootstrap.servers", "localhost:29092")
            .option("subscribe", "userscreated")  # 토픽명과 정확히 일치해야
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        # cluster = Cluster(["localhost"])
        cluster = Cluster(
            contact_points=["localhost"], load_balancing_policy=RoundRobinPolicy()
        )

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
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
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")
            try:
                streaming_query = (
                    selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("keyspace", "spark_streams")
                    .option("table", "created_users")
                    .start()
                )
                streaming_query.awaitTermination()
            except Exception as e:
                logging.error(f"Streaming query failed: {e}")

            # streaming_query.awaitTermination()


# python spark_stream.py
# 키스페이스확인
# DESCRIBE KEYSPACES;

# docker exec -it cassandra cqlsh -u cassandra localhost 9042
# describe spark_streams.created_users;
# USE spark_streams;

# 테이블확인
# DESCRIBE TABLES;

# DROP TABLE created_users;

# DESCRIBE TABLE spark_streams.created_users;

# SELECT COUNT(*) FROM spark_streams.created_users;


# 종속성확인
# spark-submit --version
# spark-submit --master spark://localhost:7077 spark_stream.py
# 에러확인
# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
# SELECT * FROM spark_streams.created_users;


# spark-submit \
#     --master spark://localhost:7077 \
#     --packages \
# com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,\
# org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
# org.apache.kafka:kafka-clients:3.4.0,\
# org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,\
# org.apache.commons:commons-pool2:2.11.1 \
#     spark_stream.py


# producer확인 로그
# docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic userscreated --from-beginning
