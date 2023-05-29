

class TableRow:
    
	def __init__(self, attributeValue:list() = []) -> None:
		self.attributeValue = attributeValue

	def addAttributeValue(self, attributeValue):
		self.attributeValue.append(attributeValue)

	def getAttributeValue(self, index):
		return self.attributeValue[index]

	def getAttributeSize(self):
		return len(self.attributeValue)

	def __str__(self) -> str:
		return ' '.join(str(a) for a in self.attributeValue)
		# str( type(self.attributeValue))+