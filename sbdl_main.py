# sbdl_main.py

import sys
import uuid
import os

from pyspark.sql.functions import col, struct, to_json

from lib import Utils
from lib import ConfigLoader
from lib import DataLoader
from lib import Transformations
from lib.logger import Log4j


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]
    job_run_id = "SBDL-" + str(uuid.uuid4())

    print("Initializing SBDL Job in " + job_run_env + " Job ID : " + job_run_id)

    conf = ConfigLoader.get_config(job_run_env)

    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.info("SBDL Job Started")
    logger.info("Job Run ID : " + job_run_id)
    logger.info("Environment : " + job_run_env)
    logger.info("Load Date : " + load_date)

    logger.info("Reading SBDL Account DF")
    accounts_df = DataLoader.read_accounts(spark, job_run_env, enable_hive, hive_db)
    contract_df = Transformations.get_contract(accounts_df)

    logger.info("Reading SBDL Party DF")
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    relations_df = Transformations.get_relations(parties_df)

    logger.info("Reading SBDL Address DF")
    address_df = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)
    relation_address_df = Transformations.get_address(address_df)

    logger.info("Join Party Relations and Address")
    party_address_df = Transformations.join_party_address(
        relations_df,
        relation_address_df
    )

    logger.info("Join Account and Parties")
    data_df = Transformations.join_contract_party(
        contract_df,
        party_address_df
    )

    logger.info("Apply Header and create Event")
    final_df = Transformations.apply_header(spark, data_df)

    logger.info("Preparing to send data to Kafka")

    kafka_kv_df = final_df.select(
        col("payload.contractIdentifier.newValue").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )

    kafka_kv_df.show(5, False)

    api_key = os.getenv("KAFKA_API_KEY", conf.get("kafka.api_key", ""))
    api_secret = os.getenv("KAFKA_API_SECRET", conf.get("kafka.api_secret", ""))

    kafka_write = kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"])

    if conf.get("kafka.security.protocol", "") != "":
        kafka_write = kafka_write.option(
            "kafka.security.protocol",
            conf["kafka.security.protocol"]
        )

    if conf.get("kafka.sasl.mechanism", "") != "":
        kafka_write = kafka_write.option(
            "kafka.sasl.mechanism",
            conf["kafka.sasl.mechanism"]
        )

    if conf.get("kafka.client.dns.lookup", "") != "":
        kafka_write = kafka_write.option(
            "kafka.client.dns.lookup",
            conf["kafka.client.dns.lookup"]
        )

    if api_key != "" and api_secret != "":
        jaas_config = (
            'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{api_key}" password="{api_secret}";'
        )

        kafka_write = kafka_write.option(
            "kafka.sasl.jaas.config",
            jaas_config
        )

    kafka_write.save()

    logger.info("SBDL Job Completed Successfully")

    spark.stop()