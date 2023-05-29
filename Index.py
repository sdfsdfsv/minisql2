

class Index:
      
      def __init__(self,indexName:str,tableName:str,attributeName:str,blockNum:int=0,rootNum:int=0):
            self.indexName = indexName
            self.tableName = tableName
            self.attributeName = attributeName
            self.blockNum = blockNum
            self.rootNum = rootNum
      
      def __str__(self):
            return f"Index(index:{self.indexName},\
                  table:{self.tableName},\
                  attributeName:{self.attributeName},\
                  blockNum:{self.blockNum},\
                  rootNum:{self.rootNum})"
      