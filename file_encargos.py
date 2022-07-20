import string
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import array, col, lit, struct, udf,lpad,concat


from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from file_parameters import FileParameters

class FileStructeredEncargos():
     
     listColumnTranslateLeft = []
     listColumns = []

     def __init__(self,data:DataFrame,fileparameters: FileParameters):
        self.data= data
        self.fileparameters= fileparameters

     def  apply(self,glueContext:GlueContext):
          
          translateData= self.translateLeft()

          data_concated = self.transformToModel(translateData)

          data_concated.printSchema()
          data_concated.show()

          glueDF = DynamicFrame.fromDF(data_concated,glueContext,name="data_concated")

          glueDF.write(connection_type = "s3", 
                       connection_options = {
                        "path": f"s3://{self.fileparameters.bucket}/{self.fileparameters.name}.{self.fileparameters.formatfile}"
                        }, 
                      format = self.fileparameters.formatfile, 
                      format_options={
                        "withHeader": False,
                        "quoteChar": -1, 
                        "escaper": "\n\r"
                     })

          print("Dynamic Frame")
          glueDF.printSchema()

     def translateLeft(self)->DataFrame:

         data_translate= self.data
         data_translate = data_translate.select(*self.listColumnTranslateLeft)
         
        
         data_union = self.data.drop(*data_translate.schema.fieldNames())
         data_union = self.data.select(*self.listColumnTranslateLeft,*data_union.schema.fieldNames())
        
         return data_union

     def addColumnTranslateLeft(self,name:string,size:int,caracter:string):
         self.listColumnTranslateLeft.append( lpad(self.data[name], size, caracter).alias(name))
         return self

     def transformToModel(self, data:DataFrame): 
         
         self.generateColumns(data) 

         data_canceted = data.select(concat(*self.listColumns).alias("data")) 

         return data_canceted 

     def generateColumns(self,data:DataFrame): 

         for col in data.schema.fieldNames():
             self.listColumns.append(data[col]) 

     @staticmethod
     def create(data:DataFrame):
         return FileStructeredEncargos(data)