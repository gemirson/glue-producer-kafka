

from logging import exception
import string
import uuid

from pymysql import STRING
from producer_exception import DataFrameIsNull, DataFrameNotFoundColumns
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import array, col, lit, struct, udf
from pyspark.sql.types import NullType, StringType
from pyspark.sql.utils import AnalysisException

class ProducerKafka():
    

    def __init__(self,data:DataFrame,topic:string, servers:string ,pathSchema:string) -> None:
        self.data = data
        self.topic =topic
        self.servers= servers
        self.pathSchema = pathSchema

    def apply(self):
         

        kafka_with_headers = None
        kafka_stantard_Model=None

        try:

            kafka_with_headers = self.TransformHeaders(self.data)

            if kafka_with_headers is None:    
                raise DataFrameIsNull("DataFrame não pode ser nulo ou vazio") 

            kafka_stantard_Model = self.TransformModelKafkaSpark(kafka_with_headers,self.pathSchema)
        
            if kafka_stantard_Model is None:    
                raise DataFrameIsNull("DataFrame não pode ser nulo ou vazio") 

            kafka_stantard_Model.write\
                                .format("kafka")\
                                .option("kafka.bootstrap.servers", self.servers)\
                                .option("topic",self.topic)\
                                .save()

        except AnalysisException:
            return None                         
 
    def TransformHeaders(cls,data:DataFrame)->DataFrame:
        try:
           
            if not type(data) is DataFrame:
                raise  DataFrameIsNull  
          
            print("TransformHeaders")
            data.printSchema()

            if data.schema.simpleString().find('data') < 0:
                raise DataFrameNotFoundColumns

            result = data.schema.simpleString().find('data')
           
            if result > 0 :
                random_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic() 
                data_headers = data.withColumn("correlationId",random_udf())\
                                .withColumn("correlationIdKey",lit('correlationId'))\
                                .withColumn("transactionId",random_udf())\
                                .withColumn("transactionIdKey",lit('transactionId'))

                kafka_header = data_headers.withColumn("headers", 
                array([
                    struct(col("correlationIdKey").alias("key"),
                            col("correlationId").cast("Binary").alias("value")
                            ),
                    struct( col("transactionIdKey").alias("key"),
                            col("transactionId").cast("Binary").alias("value"),
                        )
                            
                ])).drop("correlationId","correlationIdKey","transactionId","transactionIdKey")

                return   kafka_header  
           
        except DataFrameIsNull:  
            exception("DataFrame não pode ser nulo ou vazio")
            return None
        except  DataFrameNotFoundColumns:
            exception("O DataFrame não possui uma coluna  data válida, para criação dos headers")  
            return None    
        except AnalysisException:
            return None          

    def TransformModelKafkaSpark(cls,data:DataFrame,pathSchema:string)->DataFrame: 
        try:
            print("TransformModelKafkaSpark")
           
            if not type(data) is DataFrame:
                raise TypeError("DataFrame não pode ser nulo ou vazio")
                
            if data==None:
                exception("DataFrame não pode ser nulo ou vazio")
                return

            data.printSchema()

            result = data.schema.simpleString().find('data')
            if result > 0 :

                kafka_data= data.withColumn("value", 
                        struct(col("data").alias("data"))).drop("data")
                schema = cls.loadSchema(pathSchema) 
                kafka_data = kafka_data.select(kafka_data["headers"],to_avro('value',schema).alias("value"))  

                return  kafka_data 
        except AnalysisException:
            return None      

    def loadSchema(cls,path:string):
       return open(path,'r').read()

    def has_column(cls,df, col):
        try:
            df[col]
            return True
        except AnalysisException:
            return False    

    class Factory():

        @staticmethod
        def create(data:DataFrame,topic:string, servers:string,pathSchema:string):
           return ProducerKafka(data,topic,servers,pathSchema) 