from PySparkUnitTestBase import PySparkUnitTestBase
from SparkApp import SparkApp

class PysparkUnitTest(PySparkUnitTestBase):
    def _create_a_SparkSession(self):
        import pyspark
        import findspark
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as f

        spark=SparkSession.builder.appName('Customer Details').getOrCreate()
        return spark
    '''def test_data(self,df1, df2):
        data1 = df1.collect()
        data2 = df2.collect()
        return set(data1) == set(data2)'''
    def test_load_data_case(self,customer,product,transactions,output):
        
        spark = self._create_a_SparkSession()
        expected_output_df = spark.read.csv(output,inferSchema=True,header=True)
        output_df = SparkApp().load_output(customer,product,transactions)
        output_df = output_df.withColumn('purchase_count_per_product_id',f.round(output_df["purchase_count_per_product_id"]).cast('integer'))
        #expected_output_df.show()
        #output_df.show()
        self.assertEqual( output_df.collect(),expected_output_df.collect())

PysparkUnitTest().test_load_data_case('customers_test-1.csv','products_test-1.csv','transactions_test-1.json','output_test-1.csv')

PysparkUnitTest().test_load_data_case('customers_test-2.csv','products_test-2.csv','transactions_test-2.json','output_test-2.csv')