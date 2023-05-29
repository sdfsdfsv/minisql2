from Attribute import Attribute
from Index import Index
class Table:
      
      def __init__(self, tableName:str, primaryKey:str, attributes:list[Attribute], index:list[Index]=[], rowNum:int=0):
            self.tableName = tableName
            self.primaryKey = primaryKey
            self.attributes = attributes
            self.index = index
            self.rowNum = rowNum
            self.rowLength = sum(a.type.getLength() for a in self.attributes)
      
      def __str__(self):
            return f"TABLE----\nname: %s" % self.tableName  + f"\nprimary: %s" % self.primaryKey \
                  + f"\nattributes: %s" % ' '.join(str(a) for a in self.attributes) +\
                  f"\nindex: %s" % ' '.join(str(a) for a in self.index) \
                  + f"\nrowNum: %s" % self.rowNum + f"\nrowLength: %s" % self.rowLength
            
if __name__ == "__main__":
      print(Table("table","id",[],[]))
