import unittest
from pyspark.sql import SparkSession

class PySparkUnitTestBase(unittest.TestCase):
    def setUpClass(self):
        spark = SparkSession\
        .builder\
        .appName('Unit Testing in Pyspark Application')\
        .master('local[*]')\
        .getOrCreate()
        self.spark = spark
    
    def tearDownClass(self):
        self.spark.stop()