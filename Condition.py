from CatalogManager import CatalogManager


class Condition:
	def __init__(self, name=None, operator="", value=None):
		self.name = name
		self.operator = operator
		self.value = value

	def satisfy(self, tableName, row=None):
		index = CatalogManager.getAttributeIndex(tableName, self.name)

		lhs = row.getAttributeValue(index)
  
		rhs = self.value
		# print(rhs,lhs,self.operator)
		if self.operator == "=":
			return lhs == rhs
		elif self.operator == "<>" or self.operator == "!=":
			return lhs != rhs
		elif self.operator == ">":
			return lhs > rhs
		elif self.operator == "<":
			return lhs < rhs
		elif self.operator == "<=":
			return lhs <= rhs
		elif self.operator == ">=":
			return lhs >= rhs
		else:
			return False

	def __str__(self) -> str:
		return f"Condition({self.name} {self.operator} {self.value})"
