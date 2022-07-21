from pyspark import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import array, col, lit, struct, udf,lpad,concat,substring

import string

class LoadFileTxt():
    def __init__(self,spark,url_path_model:string,url:string):
        self.spark = spark
        self.url_path_model   = url_path_model
        self.url = url
      

    def apply(self):
        
        columns = []
        data = self.applyTransformColumnManyColumns()
        schemaFields = self.getColumns()

        for row in schemaFields:

            columns.append(
                col(row['name']).cast(TypesStantardToSQl(row['type'])).alias(row['name'])
            )

        data = data.select(*columns)

        return data

    def loadSchema(self):
        return self.spark.read.option("multiline", "true").json(path=self.url) 

    def getColumns(self):
        schema = self.loadSchema()
        columns= schema.collect()  
        return map(lambda x: x.asDict(), columns)

    def loadDataFromRepository(self):
        data  =  self.spark.read.option("multiline", "true").text(self.url)  
        return data

    def TypesStantardToSQl(key:string):
   
        ds = [
            {
                "key": "string", 
                "value":StringType()
            },
            {
                "key": "double", 
                "value":DecimalType(10,2)
            },
            {
                "key": "int", 
                "value":IntegerType()
            },
            {
                "key": "date", 
                "value":DateType()
            },
            ]
            
        for ts in ds:
            if ts['key'] == key:
                return ts['value']

    def applyTransformColumnManyColumns(self):    
        loadedData:DataFrame = self.loadDataFromRepository()
        columns = self.getColumns()

        loadedData = loadedData.select(
            *[
                substring(
                    str='value',
                    pos= row['from'],
                    len= row['length']
                ).alias(row['name'])
                for row in columns
            ])

        return loadedData 

    @staticmethod
    def create(spark,url_path_model:string,url:string):    
        return LoadFileTxt(spark, url_path_model, url)