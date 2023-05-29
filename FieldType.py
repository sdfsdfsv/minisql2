
class FieldType:
      CHARSIZE = 1
      INTSIZE = 4
      FLOATSIZE = 4
      
      def __init__(self, type, length=1):
            self.type = type  
            self.length = length
            
            
      def getLength(self):
            if self.type == "CHAR":
                  return self.length * FieldType.CHARSIZE
            elif self.type == "INT":
                  return FieldType.INTSIZE
            elif self.type == "FLOAT":
                  return FieldType.FLOATSIZE
                  
            return 0
      
      def __str__(self) -> str:
            return str(self.type)
      
if __name__ == "__main__":
      a=FieldType("CHAR",1)
      print(a.getLength())