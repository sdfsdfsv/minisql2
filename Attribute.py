from FieldType import FieldType     


class Attribute:

      def __init__(self,attributeName, type, length=1, isUnique=False):
            self.attributeName = attributeName
            self.type = FieldType(type, length)
            self.isUnique = isUnique
      
      def __str__(self):
            return f"( %s, %s, %s )" % (self.attributeName, self.type, self.isUnique)