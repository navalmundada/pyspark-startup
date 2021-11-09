from unittest import TestCase
from pyspark.sql import SparkSession

class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("spark_config_test") \
            .getOrCreate()

    def test_spark_configs(self):
        configurations = self.spark.sparkContext.getConf().getAll()
        final_conf= {key:val for key, val in configurations}
        result=final_conf.get("spark.master")
        self.assertEqual(result, "local[3]", "as per conf file it sould be local[3]")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
