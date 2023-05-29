from BufferManager import BufferManager
from Block import Block
from Address import Address
from BplusTree import *
from collections import defaultdict
from CatalogManager import CatalogManager
from Condition import Condition
from FieldType import FieldType
from Index import Index
from TableRow import TableRow
import struct
import os
import pickle

class IndexManager:

    intTreeMap = {}
    charTreeMap = {}
    floatTreeMap = {}

    @staticmethod
    def select(index, condition):
        tableName = index.tableName
        attributeName = index.attributeName
        type = CatalogManager.getAttributeType(tableName, attributeName).type

        if type == "INT":
            intTree = IndexManager.intTreeMap.get(index.indexName)
            res= IndexManager.satisfiesCondition(intTree, condition)
            return [r[1][0] for r in res]
        elif type == "FLOAT":
            floatTree = IndexManager.floatTreeMap.get(index.indexName)
            res= IndexManager.satisfiesCondition(floatTree, condition)
            return [r[1][0] for r in res]
        elif type == "CHAR":
            charTree = IndexManager.charTreeMap.get(index.indexName)
            res= IndexManager.satisfiesCondition(charTree, condition)
            return [r[1][0] for r in res]

        return None
    
    @staticmethod
    def delete(index,key):
        tableName = index.tableName
        attributeName = index,attributeName
        type = CatalogManager.getAttributeType(tableName,attributeName).type

        if type == "INT":
            intTree = IndexManager.intTreeMap.get(index.indexName)
            intTree.delete(key)
        elif type == "FLOAT":
            floatTree = IndexManager.floatTreeMap.get(index.indexName)
            floatTree.delete(key)
        elif type == "CHAR":
            charTree = IndexManager.charTreeMap.get(index.indexName)
            charTree.delete(key)
                


    
    @staticmethod
    def insert(index,key,value):
        tableName = index.tableName
        attributeName = index.attributeName
        type = CatalogManager.getAttributeType(tableName, attributeName).type
        
        if type == "INT":
            intTree = IndexManager.intTreeMap.get(index.indexName)
            intTree.insert(key,value)
        elif type == "FLOAT":
            floatTree = IndexManager.floatTreeMap.get(index.indexName)
            floatTree.insert(key,value)
        elif type == "CHAR":
            charTree = IndexManager.charTreeMap.get(index.indexName)
            charTree.insert(key,value)

    @staticmethod
    def update(index,key,value):
        tableName = index.tableName
        attributeName = index.attributeName
        type = CatalogManager.getAttributeType(tableName, attributeName).type

        if type == "INT":
            intTree = IndexManager.intTreeMap.get(index.indexName)
            intTree.update(key,value)
        elif type == "FLOAT":
            floatTree = IndexManager.floatTreeMap.get(index.indexName)
            floatTree.update(key,value)
        elif type == "CHAR":
            charTree = IndexManager.charTreeMap.get(index.indexName)
            charTree.update(key,value)
    
    @staticmethod
    def initialIndex():
        fileName = "index.db"
        if not os.path.exists(fileName):
            return
        with open(fileName, "rb") as f:
            while True:
                    try:
                        index = pickle.load(f)
                        CatalogManager.createIndex(index)
                    
                    except EOFError:
                        break
    
    @staticmethod
    def createIndex(idx):
        IndexManager.buildIndex(idx)
        # 把idx的信息写入到硬盘中
        with open(idx.indexName, "wb") as file:
            pickle.dump(idx, file)

        return True  # 文件读写失败返回false

    @staticmethod
    def dropIndex(idx : Index):
        fileName = idx.indexName
        if os.path.exists(fileName):
            os.remove(fileName)
        index = CatalogManager.getAttributeIndex(idx.tableName, idx.attributeName)
        type = CatalogManager.getAttributeType(idx.tableName, index)
        if type == "INT":
            IndexManager.intTreeMap.pop(idx.indexName)
        elif type == "CHAR":
            IndexManager.intTreeMap.pop(idx.indexName)
        elif type == "FLOAT":
            IndexManager.floatTreeMap.pop(idx.indexName)
        
        return True
    

    @staticmethod
    def buildIndex(idx):
        tableName = idx.tableName
        attributeName = idx.attributeName
        tupleNum = CatalogManager.getRowNum(tableName)
        storeLen = IndexManager.getStoreLength(tableName)
        byteOffset = FieldType.INTSIZE
        blockOffset = 0
        processNum = 0
        index = CatalogManager.getAttributeIndex(tableName,attributeName)
        type = CatalogManager.getType(tableName,index)

        block = BufferManager.readBlockFromDiskQuote(tableName, 0)

        intTree = BplusTree(4)
        floatTree = BplusTree(4)
        charTree = BplusTree(4)

        if type == "INT":
            while processNum < tupleNum:
                if byteOffset + storeLen >= Block.BLOCKSIZE:
                    blockOffset+=1
                    byteOffset=0
                    block=BufferManager.readBlockFromDiskQuote(tableName,blockOffset)
                    if block is None:
                        raise RuntimeError("Could not read block")
                
                if block.readInteger(byteOffset) < 0:
                    value = Address(tableName,blockOffset,byteOffset)
                    row = IndexManager.getTuple(tableName,block,byteOffset)
                    key = row.getAttributeValue(index)
                    intTree.insert(key,value)
                    processNum+=1
                byteOffset+=storeLen
            IndexManager.intTreeMap[idx.indexName] = intTree

        elif type == "CHAR":
            while processNum < tupleNum:
                if byteOffset + storeLen >= Block.BLOCKSIZE:
                    blockOffset+=1
                    byteOffset=0
                    block=BufferManager.readBlockFromDiskQuote(tableName,blockOffset)
                    if block is None:
                        raise RuntimeError("Could not read block")
                
                if block.readInteger(byteOffset) < 0:
                    value = Address(tableName,blockOffset,byteOffset)
                    row = IndexManager.getTuple(tableName,block,byteOffset)
                    key = row.getAttributeValue(index)
                    charTree.insert(key,value)
                    processNum+=1
                byteOffset+=storeLen
            IndexManager.charTreeMap[idx.indexName] = charTree
        elif type == "FLOAT":
            while processNum < tupleNum:
                if byteOffset + storeLen >= Block.BLOCKSIZE:
                    blockOffset+=1
                    byteOffset=0
                    block=BufferManager.readBlockFromDiskQuote(tableName,blockOffset)
                    if block is None:
                        raise RuntimeError("Could not read block")
                
                if block.readInteger(byteOffset) < 0:
                    value = Address(tableName,blockOffset,byteOffset)
                    row = IndexManager.getTuple(tableName,block,byteOffset)
                    key = row.getAttributeValue(index)
                    floatTree.insert(key,value)
                    processNum+=1
                byteOffset+=storeLen
            IndexManager.floatTreeMap[idx.indexName] = floatTree


        

    @staticmethod
    def satisfiesCondition(tree : BplusTree, condition:Condition):
        key = condition.value
        condition = condition.operator

        if condition == "=":
            return tree.findEqual(key)
        elif condition == "<>" or condition == "!=":
            return tree.findNotEqual(key)
        elif condition == ">":
            return tree.findGreater(key)
        elif condition == "<":
            return tree.findLess(key)
        elif condition == ">=":
            return tree.findGeq(key)
        elif condition == "<=":
            return tree.findLeq(key)
        else:
            return []
        
    @staticmethod
    def getStoreLength(tableName):
        rowLen= CatalogManager.getRowLength(tableName)
        if rowLen > FieldType.INTSIZE:
            return rowLen + FieldType.INTSIZE
        else:
            return FieldType.INTSIZE + FieldType.CHARSIZE
        
    @staticmethod
    def getTuple(tableName, block, offset):
        attributeNum = CatalogManager.getAttributeNum(tableName)
        attributeValue = None
        result = TableRow()

        offset += 1 #skip first valid flag

        for i in range(attributeNum):
            length = CatalogManager.getLength(tableName, i)
            type = CatalogManager.getType(tableName, i)
            if type == "CHAR":
                attributeValue = block.readString(offset, length)
                # first = attributeValue.find('\0')
                # first = len(attributeValue) if first == -1 else first
                # attributeValue = attributeValue[:first]
            elif type == "INT":
                attributeValue = str(block.readInteger(offset))
            elif type == "FLOAT":
                attributeValue = str(block.readFloat(offset))
            offset += length
            result.addAttributeValue(attributeValue)
        return result


if __name__ == "__main__":
    IndexManager.initialIndex()