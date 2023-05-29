from BufferManager import BufferManager
from CatalogManager import CatalogManager
from IndexManager import IndexManager
from Index import Index
from Condition import Condition
from Table import Table
from TableRow import TableRow
from RecordManager import RecordManager
from Table import Table
from Attribute import Attribute
from BplusTree import *

class Api:

    def __init__(self):
        pass

    @staticmethod
    def initial():
        BufferManager.initialBuffer()
        CatalogManager.initialCatalog()
        IndexManager.initialIndex()

    @staticmethod
    def store():
        CatalogManager.storeCatalog()
        RecordManager.storeRecord()

    @staticmethod
    def createTable(table: Table) -> bool:
        if RecordManager.createTable(table.tableName) and CatalogManager.createTable(table):
            
            index = Index(table.tableName + ".index.db", table.tableName,
                            CatalogManager.getPrimaryKey(table.tableName))
            IndexManager.createIndex(index)
            CatalogManager.createIndex(index)
            return True
        return False

    @staticmethod
    def dropTable(tabName: str) -> bool:
        for i in range(0, CatalogManager.getAttributeNum(tabName)):
            attrName = CatalogManager.getAttributeName(tabName, i)
            indexName = CatalogManager.getIndexName(tabName, attrName)
            if indexName is not None:
                IndexManager.dropIndex(CatalogManager.getIndex(indexName))
        if CatalogManager.dropTable(tabName) and RecordManager.dropTable(tabName):
            return True

    @staticmethod
    def createIndex(index:Index):
        if IndexManager.createIndex(index) and CatalogManager.createIndex(index):
            return True

    @staticmethod
    def dropIndex(indexName:str):
        index = CatalogManager.getIndex(indexName)
        if IndexManager.dropIndex(index) and CatalogManager.dropIndex(indexName):
            return True

    @staticmethod
    def insertRow(tableName:str, row: TableRow):
        recordAddress = RecordManager.insert(tableName, row)
        attrNum = CatalogManager.getAttributeNum(tableName)
        for i in range(0, attrNum):
            attrName = CatalogManager.getAttributeName(tableName, i)
            indexName = CatalogManager.getIndexName(tableName, attrName)
            if indexName is not None:
                index = CatalogManager.getIndex(indexName)
                key = row.getAttributeValue(i)
                IndexManager.insert(index, key, recordAddress)
                CatalogManager.indexes[indexName] = index
        CatalogManager.tables[tableName].rowNum += 1
        return True

    @staticmethod
    def deleteRow(tabName, conditions:list[Condition]):
        condition = Api.findIndexCondition(tabName, conditions)
        numberOfRecords = 0
        if condition is not None:
            try:
                indexName = CatalogManager.getIndexName(
                    tabName, condition.name)
                index = CatalogManager.getIndex(indexName)
                addresses = IndexManager.select(index, condition)
                if addresses is not None:
                    numberOfRecords = RecordManager.delete(
                        addresses, conditions)
            except TypeError:
                Exception("Table " + tabName + " does not exist!")
            except ValueError as e:
                Exception(str(e))
            except Exception:
                Exception("Failed to delete on table " + tabName)
        else:
            try:
                numberOfRecords = RecordManager.delete(tabName, conditions)
            except TypeError:
                Exception("Table " + tabName + " does not exist!")
            except ValueError as e:
                Exception(str(e))
        CatalogManager.deleteRowNum(tabName, numberOfRecords)
        return numberOfRecords

    @staticmethod
    def select(tableName:str, attriName:list[str], conditions:list[Condition])->list[TableRow]:
        resultSet = []
        condition = Api.findIndexCondition(tableName, conditions)
        # print(condition)
        if condition is not None:
            try:
                indexName = CatalogManager.getIndexName(
                    tableName, condition.name)
                index = CatalogManager.getIndex(indexName)
                # print("index==",index)
                addresses = IndexManager.select(index, condition)
                # print(addresses)
                if addresses is not None:
                    resultSet = RecordManager.selectAddress(addresses, conditions)
            except TypeError:
                Exception("Table " + tableName + " does not exist!")
            except ValueError as e:
                Exception(str(e))
            except Exception:
                Exception("Failed to select from table " + tableName)
        else:
            try:
                resultSet = RecordManager.select(tableName, conditions)
            except TypeError:
                Exception("Table " + tableName + " does not exist!")
            except ValueError as e:
                Exception(str(e))

        if attriName:
            try:
                return RecordManager.project(tableName, resultSet, attriName)
            except TypeError:
                Exception("Table " + tableName + " does not exist!")
            except ValueError as e:
                Exception(str(e))
        else:
            return resultSet

    @staticmethod
    def findIndexCondition(tableName, conditions:list[Condition]):
        condition = None
        for c in conditions:
            if CatalogManager.getIndexName(tableName, c.name) is not None:
                condition = c
                break
        return condition


if __name__ == "__main__":
    Api.initial()
    idd = Attribute('id', 'INT', 4, True)
    name = Attribute('name', 'CHAR', 12)
    cat = Attribute('category', 'CHAR', 20)
    attributes = [idd, name, cat]
    t1 = Table("students", "id", attributes)
    
    
    Api.createTable(t1)
    # print(t1)

    for i in range(0,10):
        Api.insertRow("students", TableRow([i, f'ljx{i}', f'{i}man']))
        # Api.store()

    res=Api.select("students",["id","name","catagory"],[Condition("id",">","1")])
    print([str(r) for r in res])
    res=Api.select("students",["id"],[])
    print([str(r) for r in res])
    print(len(res[0].attributeValue))
    
    