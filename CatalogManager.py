import struct
import os
import pickle
from collections import *
from Index import Index
from Attribute import Attribute
from Table import Table


class CatalogManager:

      tables = {}
      indexes = {}

      tableFilename = "table.db"
      indexFilename = "index.db"

      def __init__(self):
            pass

      @staticmethod
      def initialCatalog():
            CatalogManager.initialTable()
            CatalogManager.initialIndex()

      @staticmethod
      def getTables():
            return CatalogManager.tables

      @staticmethod
      def getIndexes():
            return CatalogManager.indexes

      @staticmethod
      def initialTable():
            with open(CatalogManager.tableFilename, 'wb'):
                  pass
            with open(CatalogManager.tableFilename, 'rb') as f:
                  while True:
                        try:
                              table = pickle.load(f)
                              print(table)
                              CatalogManager.indexes[table.tableName] = table
                        except EOFError:
                              break
                        

      @staticmethod
      def initialIndex():
          if not os.path.exists(CatalogManager.indexFilename):
              return
          with open(CatalogManager.indexFilename, 'wb'):
                  pass
          with open(CatalogManager.indexFilename, 'rb') as f:
              while True:
                  try:
                      index = pickle.load(f)
                      CatalogManager.indexes[index.indexName] = index
                  
                  except EOFError:
                      break

      @staticmethod
      def storeCatalog():
            CatalogManager.storeTable()
            CatalogManager.storeIndex()

      @staticmethod
      def storeTable():
            with open(CatalogManager.tableFilename, "wb") as f:
                  
                  for table in CatalogManager.tables.values():
                        pickle.dump(table,f)


      @staticmethod
      def storeIndex():
            with open(CatalogManager.indexFilename, "wb") as f:
                  for index in CatalogManager.indexes.values():
                        pickle.dump(index,f)

      @staticmethod
      def showCatalog():
            CatalogManager.showTable()
            CatalogManager.showIndex()

      @staticmethod
      def showIndex():
            idx = 5
            tab = 5
            attr = 9
            for index in CatalogManager.indexes.values():
                  idx = max(len(index.indexName), idx)
                  tab = max(len(index.tableName), tab)
                  attr = max(len(index.attributeName), attr)
            formatStr = "|%-{}s|%-{}s|%-{}s|\n".format(idx, tab, attr)
            print(formatStr % ("INDEX", "TABLE", "ATTRIBUTE"))
            for index in CatalogManager.indexes.values():
                  print(formatStr % (index.indexName,
                        index.tableName, index.attributeName))

      @staticmethod
      def getMaxAttrLength(table):
            l = max(len(attribute.attributeName) for attribute in table.attributes)
            return max(l,9)

      @staticmethod
      def showTable():
              for table in CatalogManager.tables.values():
                  print("[TABLE] " + table.tableName)
                  formatStr = "|%-{}s|%-5s|%-6s|%-6s|\n".format(CatalogManager.getMaxAttrLength(table))
                  print(formatStr % ("ATTRIBUTE", "TYPE", "LENGTH", "UNIQUE"))
                  for attribute in table.attributes:
                      print(formatStr % (attribute.attributeName, attribute.type, attribute.type.getLength(), attribute.isUnique))
                  print("--------------------------------")
                  
      @staticmethod
      def getTable(tableName):
            return CatalogManager.tables.get(tableName)
            
      @staticmethod
      def getIndex(indexName):
            return CatalogManager.indexes.get(indexName)
            
      @staticmethod
      def getPrimaryKey(tableName):
            return CatalogManager.getTable(tableName).primaryKey
            
      @staticmethod
      def getRowLength(tableName):
            return CatalogManager.getTable(tableName).rowLength
            
      @staticmethod
      def getAttributeNum(tableName):
            return len(CatalogManager.getTable(tableName).attributes)
            
      @staticmethod
      def getRowNum(tableName):
            return CatalogManager.getTable(tableName).rowNum
            
      @staticmethod
      def isPrimaryKey(tableName, attributeName):
            if tableName in CatalogManager.tables:
                  return CatalogManager.getTable(tableName).primaryKey == attributeName
            else:
                  print("The table ", tableName, " doesn't exist")
                  return False
      
      @staticmethod 
      def isUnique(tableName, attributeName):
            if tableName in CatalogManager.tables:
                  table = CatalogManager.getTable(tableName)
                  for attribute in table.attributes:
                        # tmpAttribute = tmpTable.attributes[i]
                        if attribute.attributeName == attributeName:
                              return attribute.isUnique
                  
                  print("The attribute ", attributeName, "is not exist")
                  return False
            print("The table", tableName, "doesnt exist")
            return False
      
      @staticmethod
      def isAttributeExist(tableName, attributeName):

            table = CatalogManager.getTable(tableName)
            for a in table.attributes:
                  if a.attributeName == attributeName:
                        return True
            return False
      
      @staticmethod
      def isIndexKey(tableName,attributeName):
            if not tableName in CatalogManager.tables:
                  print("The table", tableName, "doesnt exist")
                  return False
            
            table = CatalogManager.getTable(tableName)
            
            if not CatalogManager.isAttributeExist(tableName, attributeName):
                  print("The attribute " + attributeName + " doesn't exist")
                  return False

            for index in table.index:
                  if index.attributeName == attributeName:
                        return True

                 
            return False
                              
      
      @staticmethod
      def getIndexName(tableName,attributeName):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            table= CatalogManager.tables[tableName]
            
            if not CatalogManager.isAttributeExist(tableName,attributeName):
                  print("The attribute " + attributeName + " doesn't exist")  
                  return None
            
            for index in table.index:
                  if index.attributeName == attributeName:
                        return index.indexName
            
            # print(333,attributeName )
                
            return None 
      
      @staticmethod 
      def getAttributeName(tableName,i):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            return CatalogManager.tables[tableName].attributes[i].attributeName
      
      @staticmethod
      def getAttributeIndex (tableName,attributeName):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            table= CatalogManager.tables[tableName]
            
            for i,a in enumerate(table.attributes):
                 if a.attributeName == attributeName:
                       return i
            print("The attribute " + attributeName + " doesn't exist")
            return -1
            
      @staticmethod
      def getAttributeType(tableName, attributeName):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            table= CatalogManager.tables[tableName]
            for a in table.attributes:
                 if a.attributeName == attributeName:
                       return a.type
            print("The attribute " + attributeName + " doesn't exist")
            return None
      

      @staticmethod
      def getType(tableName,i):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            return CatalogManager.tables[tableName].attributes[i].type.type
      
      @staticmethod
      def getLength(tableName,i):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            return CatalogManager.tables[tableName].attributes[i].type.getLength()

      @staticmethod
      def addRowNum(tableName):
            CatalogManager.tables[tableName].rowNum+=1
            
      @staticmethod
      def deleteRowNum(tableName, num):
            CatalogManager.tables[tableName].rowNum-=num
            
      @staticmethod
      def createTable(table):
            CatalogManager.tables[table.tableName]=table 
            return True
      
      @staticmethod
      def dropTable(tableName):
            if not tableName in CatalogManager.tables:
                  print("The table " + tableName + " doesn't exist")
                  return None
            
            table=CatalogManager.tables[tableName]
            for i in range(0,len(table.index)):
                  CatalogManager.indexes.pop(table.index[i].indexName)
            CatalogManager.tables.pop(tableName)
            return True 
      
      @staticmethod
      def createIndex(index):
            CatalogManager.tables[index.tableName].index.append(index)
            CatalogManager.indexes[index.indexName]=index
            return True
      
      
      @staticmethod
      def dropIndex(indexName):
            index=CatalogManager.indexes[indexName]
            CatalogManager.tables[index.tableName].index.pop(index)
            CatalogManager.indexes.pop(indexName)
            return True
      
if __name__ == "__main__":
      c=CatalogManager()
      CatalogManager.initialCatalog()