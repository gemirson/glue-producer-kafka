
class FileParameters():
    def __ini__(self,bucket:string, name:string,format:string):
        self.bucket = bucket
        self.name= name
        self.formatfile= format

    @staticmethod    
    def create(bucket:string, name:string,format:string):
        return FileParameters(bucket=bucket,name=name,format=format)    