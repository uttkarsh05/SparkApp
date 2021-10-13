class SparkApp:
    
    def load_data(self,customers,products,transactions):
        '''
            customers: file name in form of string and in csv format (e.g-'customers.csv')
            products: file name in form of string and in csv format (e.g-'products.csv')
            transactions: file name in form of string and in json format (e.g-'transactions.csv')
            
            The files should be in following schema:
            customers:root
                         |-- customer_id: string (nullable = true)
                         |-- loyalty_score: integer (nullable = true)
            products:root
                         |-- product_id: string (nullable = true)
                         |-- product_category: integer (nullable = true)
            transactions:root
                         |-- basket: array (nullable = true)
                         |    |-- element: struct (containsNull = true)
                         |    |    |-- price: long (nullable = true)
                         |    |    |-- product_id: string (nullable = true)
                         |-- customer_id: string (nullable = true)
                         |-- date_of_purchase: string (nullable = true)

            
            creates a new spark session 
            returns the list of three dataframes [df_customers,df_products,df_transactions]
        '''
        
        spark = self._create_a_SparkSession()

        #reading the data
        df_customers = spark.read.csv(customers,inferSchema = True , header = True)
        df_products = spark.read.csv(products,inferSchema = True , header = True)
        df_transactions = spark.read.json(transactions,multiLine=True)
        
        return [df_customers,df_products,df_transactions]
    
    def _create_a_SparkSession(self):

        import pyspark
        import findspark
        from pyspark.sql import SparkSession
        

        spark=SparkSession.builder.appName('Customer Details').getOrCreate()
        return spark
        
    def flatten(self,df,column):
        '''
            flatten the branched structure of json file 
            returns the updated dataframe
        '''
        df = df.withColumn(column,explode(column))
        return df
    
    def count_duplicate_rows(self,df):
        '''
          counts no of duplicate rows and add a count column in the same dataframe
          returns updated dataframe  
        '''
        df = df.groupBy(df.columns)\
                .count()\
                .where(f.col('count') > 0)
        return df
        
    def rename_column(self,df,original,new):
        '''
            returns updated dataframe with new column name
        '''
        df = df.withColumnRenamed(original,new)
        return df

    def outer_join(self,df1,df2,col):
        '''
            returns outer merged dataframe df1 and df2 on col 
        '''
        df_new =  df1.join(df2,on=col,how='outer')
        return df_new
    
    def sort(self,df,col_1,col_2):
        '''
            return sorted dataframe first on col_1 and second on col_2
        '''
        df = df.sort(f.col(col_1),f.col(col_2))
        return df
        
    def load_output(self,customers,products,transactions):
        '''
            customers: file name in form of string and in csv format (e.g-'customers.csv')
            products: file name in form of string and in csv format (e.g-'products.csv')
            transactions: file name in form of string and in json format (e.g-'transactions.csv')
            
            The files should be in following schema:
            customers:root
                         |-- customer_id: string (nullable = true)
                         |-- loyalty_score: integer (nullable = true)
            products:root
                         |-- product_id: string (nullable = true)
                         |-- product_category: integer (nullable = true)
            transactions:root
                         |-- basket: array (nullable = true)
                         |    |-- element: struct (containsNull = true)
                         |    |    |-- price: long (nullable = true)
                         |    |    |-- product_id: string (nullable = true)
                         |-- customer_id: string (nullable = true)
                         |-- date_of_purchase: string (nullable = true)
                         
            returns a new dataframe (df_output) with columns in order = ['customer_id','loyalty_score','product_id','product_category','purchase_count_per_product_id']

        '''
        
        #loading the data first
        df_customers,df_products,df_transactions = self.load_data(customers,products,transactions)
        
        #importing necessary functions
        import pyspark.sql.functions as f
        from pyspark.sql.functions import explode

        #cleaning the df_products
        df_products= df_products.drop('product_description')

        # flattening the data from df_transaction 
        df_trans_flat = self.flatten(df_transactions,'basket').drop('date_of_purchase')

        # rearranging counting the no of duplicate rows in df_trans_flat dataframe
        df_trans_flat = df_trans_flat.select('customer_id','basket.*').drop('price')
        
        #counting the no of duplicate rows in df_trans_flat dataframe
        df_trans_flat = self.count_duplicate_rows(df_trans_flat)

        #renaming the column count to purchase_count_per_product_id
        df_trans_flat = self.rename_column(df_trans_flat,'count','purchase_count_per_product_id')
        
        #creating an intermediate dataframe to outer merge the dataframes df_customers and df_trans_flat on customer_id
        df_customer_trans = self.outer_join(df_customers,df_trans_flat,'customer_id')

        #creating the final output dataframe by outer merging df_products and the intermediate dataframe on product_id
        df_output = self.outer_join(df_customer_trans,df_products,'product_id')

        #rearranging the output dataframe and sorting the data first on customer_id and then on product_id
        df_output = df_output.select('customer_id','loyalty_score','product_id','product_category','purchase_count_per_product_id')
        
        df_output = self.sort(df_output,'customer_id','product_id')

        #returning the required data
        return df_output
