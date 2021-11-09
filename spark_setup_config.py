import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = set_spark_config()

    spark = SparkSession \
        .builder \
        .config(conf=conf)\
        .getOrCreate()

logger = Log4j(spark)

logger.info("Starting Spark config program")

configurations = spark.sparkContext.getConf().getAll()
for conf in configurations:
    print(conf)

logger.info("SUCCESS: Finished spark config setup done")
spark.stop()
