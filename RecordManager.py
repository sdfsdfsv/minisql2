import os
from Address import Address
from Block import Block
from Condition import Condition
from BufferManager import BufferManager
from CatalogManager import CatalogManager
from IndexManager import IndexManager
from FieldType import FieldType
from TableRow import TableRow
from typing import List


class RecordManager:

    @staticmethod
    def createTable(tableName: str) -> bool:

        block = BufferManager.readBlockFromDiskQuote(tableName, 0)
        if block is None:
            Exception()
        else:
            block.writeInteger(0, -1)
        return True

    @staticmethod
    def dropTable(tableName: str) -> bool:
        try:
            os.remove(tableName)
            BufferManager.makeInvalid(tableName)
            return True
        except:
            return False

    @staticmethod
    def select(tableName: str, conditions: List[Condition]):
        tupleNum = CatalogManager.getRowNum(tableName)
        storeLen = RecordManager.getStoreLength(tableName)
        processNum = 0
        byteOffset = FieldType.INTSIZE
        blockOffset = 0
        result = []
        block = BufferManager.readBlockFromDiskQuote(tableName, 0)
        # print(tableName, byteOffset)
        # print([block.blockData[i] for i in range(0,1024)])
        if block is None or not RecordManager.checkCondition(tableName, conditions):
            return result
        
        while processNum < tupleNum:
            # print(processNum, storeLen,byteOffset)
            if byteOffset + storeLen >= Block.BLOCKSIZE:
                blockOffset += 1
                byteOffset = 0
                block = BufferManager.readBlockFromDiskQuote(tableName, blockOffset)
                if block is None:
                    return result
                
            # print(block.readInteger(byteOffset),processNum)
            if block.readInteger(byteOffset) < 0:
                newRow = RecordManager.getTuple(tableName, block, byteOffset)
                if all([ condition.satisfy(tableName, newRow) for condition in conditions ]):
                    result.append(newRow)
                processNum += 1
                
            byteOffset += storeLen
            
        return result

    @staticmethod
    def insert(tableName, row):
        tupleNum = CatalogManager.getRowNum(tableName)
        headBlock = BufferManager.readBlockFromDiskQuote(tableName, 0)

        if headBlock is None:
            Exception("Can't get block from buffer")
            # print(114514)
        if not RecordManager.checkRow(tableName, row):
            return None

        headBlock.isLocked = True

        freeOffset = headBlock.readInteger(0)
        if freeOffset < 0:
            tupleOffset = tupleNum
        else:
            tupleOffset = freeOffset

        blockOffset = RecordManager.getBlockOffset(tableName, tupleOffset)
        byteOffset = RecordManager.getByteOffset(tableName, tupleOffset)
        insertBlock = BufferManager.readBlockFromDiskQuote(
            tableName, blockOffset)

        if insertBlock is None:
            headBlock.isLocked = False
            return None

        if freeOffset >= 0:
            freeOffset = insertBlock.readInteger(byteOffset + 1)
            headBlock.writeInteger(0, freeOffset)

        headBlock.isLocked = False
        RecordManager.writeTuple(tableName, row, insertBlock, byteOffset)
        return Address(tableName, blockOffset, byteOffset)

    @staticmethod
    def delete(tableName:str, conditions:list[Condition])->int:
        rowNum = CatalogManager.getRowNum(tableName)
        # print("rowNum: ", rowNum)
        storeLen = RecordManager.getStoreLength(tableName)

        processNum = 0
        byteOffset = FieldType.INTSIZE
        blockOffset = 0
        deleteNum = 0

        headBlock = BufferManager.readBlockFromDiskQuote(tableName, 0)
        laterBlock = headBlock

        if headBlock is None:
            print(Exception("Can't get block from buffer"))
        if not RecordManager.checkCondition(tableName, conditions):
            print("Check condition false" + conditions)
            return 0

        headBlock.isLocked = True

        for currentNum in range(0, rowNum):
            
            if byteOffset + storeLen >= Block.BLOCKSIZE:
                blockOffset += 1
                byteOffset = 0
                laterBlock = BufferManager.readBlockFromDiskQuote(tableName, blockOffset)
                if laterBlock is None:
                    headBlock.isLocked = False
                    print("headBlock lokced")
                    return deleteNum
            # print(currentNum) 
            if laterBlock.readInteger(byteOffset) < 0:
                newRow = RecordManager.getTuple(
                    tableName, laterBlock, byteOffset)
                # print(newRow)
                if all([condition.satisfy(tableName, newRow) for condition in conditions]):
                    laterBlock.writeInteger(byteOffset, 0)
                    laterBlock.writeInteger(byteOffset + 1, headBlock.readInteger(0))
                    headBlock.writeInteger(0, currentNum)
                    print("write new")
                    deleteNum += 1
                    for i in range(0, newRow.getAttributeSize()):
                        attrName = CatalogManager.getAttributeName(tableName, i)
                        print("delete new attribute",attrName, currentNum)
                        if CatalogManager.isIndexKey(tableName, attrName):
                            indexName = CatalogManager.getIndexName( tableName, attrName)
                            index = CatalogManager.getIndex(indexName)
                            # print("delete index",index,newRow.getAttributeValue(i))
                            try:
                                IndexManager.delete(index, newRow.getAttributeValue(i))
                            except Exception:
                                print(Exception)
                           
                processNum += 1
                print("processnum",processNum)
            byteOffset += storeLen

        headBlock.isLocked = False
        return deleteNum

    @staticmethod
    def selectAddress(addresses:list[Address], conditions:list[Condition]):
        if len(addresses) == 0:
            return []
        addresses.sort()
        tableName = addresses[0].fileName
        blockOffset = 0
        blockOffsetPre = -1
        byteOffset = 0

        block = None
        result = []

        if not RecordManager.checkCondition(tableName, conditions):
            return result

        for i in range(len(addresses)):
            blockOffset = addresses[i].blockOffset
            byteOffset = addresses[i].byteOffset
            if i == 0 or blockOffset != blockOffsetPre:
                block = BufferManager.readBlockFromDiskQuote(
                    tableName, blockOffset)
                if block is None:
                    if i == 0:
                        Exception("Can't get block from buffer")
            if block.readInteger(byteOffset) < 0:
                newRow = RecordManager.getTuple(tableName, block, byteOffset)
                if all([condition.satisfy(tableName, newRow) for condition in conditions]):
                    result.append(newRow)
            blockOffsetPre = blockOffset
        return result

    @staticmethod
    def deleteAddress(addresses:list[Address], conditions:list[Condition]):
        if len(addresses) == 0:
            return 0

        addresses.sort()
        tableName = addresses[0].fileName

        block_offset = 0
        block_offset_pre = -1
        byte_offset = 0
        tuple_offset = 0

        head_block = BufferManager.readBlockFromDiskQuote(tableName, 0)
        delete_block = None

        if head_block is None:
            Exception("Can't get head block from buffer")
        if not RecordManager.checkCondition(tableName, conditions):
            return 0

        head_block.isLocked(True)
        delete_num = 0
        print("Deleting")
        for i in range(len(addresses)):
            block_offset = addresses[i].blockOffset
            byte_offset = addresses[i].byteOffset
            tuple_offset = RecordManager.getTupleOffset(
                tableName, block_offset, byte_offset)

            if i == 0 or block_offset != block_offset_pre:
                delete_block = BufferManager.readBlockFromDiskQuote(
                    tableName, block_offset)
                if delete_block is None:
                    head_block.isLocked(False)
                    return delete_num

            if delete_block.readInteger(byte_offset) < 0:
                newRow = RecordManager.getTuple(
                    tableName, delete_block, byte_offset)

                if all([condition.satisfy(tableName, newRow) for condition in conditions]):
                    delete_block.writeInteger(byte_offset, 0)
                    delete_block.writeInteger(
                        byte_offset + 1, head_block.readInteger(0))
                    head_block.writeInteger(0, tuple_offset)
                    delete_num += 1
                    for k in range(newRow.getAttributeSize()):
                        attr_name = CatalogManager.getAttributeName(
                            tableName, k)
                        if CatalogManager.isIndexKey(tableName, attr_name):
                            index_name = CatalogManager.getIndexName(tableName, attr_name)
                            index = CatalogManager.getIndex(index_name)
                            IndexManager.delete(index, newRow.getAttributeValue(k))

            block_offset_pre = block_offset

        head_block.isLocked(False)
        return delete_num

    @staticmethod
    def project(tableName, result, attriName):
        attribute_num = CatalogManager.getAttributeNum(tableName)
        project_result = []

        for row in result:
            new_row = TableRow([])
            for name in attriName:
                index = CatalogManager.getAttributeIndex(tableName, name)
                if index == -1:
                    Exception("Can't not find attribute " + name)
                else:
                    new_row.addAttributeValue(row.getAttributeValue(index))

            project_result.append(new_row)

        return project_result

    @staticmethod
    def storeRecord():
        BufferManager.destructBufferManager()

    @staticmethod
    def getStoreLength(tableName):
        rowLen = CatalogManager.getRowLength(tableName)  # actual length
        if rowLen > FieldType.INTSIZE:  # add a valid byte in head
            return rowLen + FieldType.CHARSIZE
        else:  # empty address pointer + valid byte
            return FieldType.INTSIZE + FieldType.CHARSIZE

    @staticmethod
    def getBlockOffset(tableName, tupleOffset):
        storeLen = RecordManager.getStoreLength(tableName)
        # number of tuples in first block
        tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) // storeLen
        tupleInNext = Block.BLOCKSIZE // storeLen  # number of tuples in later block

        if tupleOffset < tupleInFirst:  # in first block
            return 0
        else:  # in later block
            return (tupleOffset - tupleInFirst) // tupleInNext + 1

    @staticmethod
    def getByteOffset(tableName, tupleOffset):
        storeLen = RecordManager.getStoreLength(tableName)
        # number of tuples in first block
        tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) // storeLen
        tupleInNext = Block.BLOCKSIZE // storeLen  # number of tuples in later block

        blockOffset = RecordManager.getBlockOffset(tableName, tupleOffset)
        if blockOffset == 0:  # in first block
            return tupleOffset * storeLen + FieldType.INTSIZE
        else:  # in later block
            return (tupleOffset - tupleInFirst - (blockOffset - 1) * tupleInNext) * storeLen

    @staticmethod
    def getTupleOffset(tableName, blockOffset, byteOffset):
        storeLen = RecordManager.getStoreLength(tableName)
        # number of tuples in first block
        tupleInFirst = (Block.BLOCKSIZE - FieldType.INTSIZE) // storeLen
        tupleInNext = Block.BLOCKSIZE // storeLen  # number of tuples in later block

        if blockOffset == 0:  # in first block
            return (byteOffset - FieldType.INTSIZE) // storeLen
        else:  # in later block
            return tupleInFirst + (blockOffset - 1) * tupleInNext + byteOffset // storeLen

    @staticmethod
    def getTuple(tableName, block, offset):
        attributeNum = CatalogManager.getAttributeNum(tableName)  # number of attribute
        attributeValue = None
        
        result = TableRow([])
        offset += 1  # skip first valid flag
        
        # print("attributeNum", attributeNum)
        for i in range(attributeNum):  # for each attribute
            length = CatalogManager.getLength(tableName, i)  # get length
            type = CatalogManager.getType(tableName, i)  # get type
            if type == "CHAR":  # char type
                attributeValue = block.readString(offset, length)
                first = attributeValue.find('\0')
                first = first if first != -1 else len(attributeValue)
                attributeValue = attributeValue[:first]  # filter '\0'

            elif type == "INT":  # integer type
                attributeValue = str(block.readInteger(offset))

            elif type == "FLOAT":  # float type
                attributeValue = str(block.readFloat(offset))

            offset += length
            result.addAttributeValue(attributeValue)  # add attribute to row
            
        # print("Attribute",result,len(result.attributeValue))
        return result

    @staticmethod
    def writeTuple(tableName, row, block, offset):
        attributeNum = CatalogManager.getAttributeNum(
            tableName)  # number of attribute

        block.writeInteger(offset, -1)  # set valid byte to 11111111
        offset += 1  # skip first valid flag

        for i in range(attributeNum):  # for each attribute
            length = CatalogManager.getLength(tableName, i)  # get length
            type = CatalogManager.getType(tableName, i)  # get type
            if type == "CHAR":  # char type
                block.writeString(offset, row.getAttributeValue(i))

            elif type == "INT":  # integer type
                block.writeInteger(offset, int(row.getAttributeValue(i)))

            elif type == "FLOAT":  # float type
                block.writeFloat(offset, float(row.getAttributeValue(i)))

            offset += length

    @staticmethod
    def checkRow(tableName, row):
        if CatalogManager.getAttributeNum(tableName) != row.getAttributeSize():
            raise ValueError("Attribute number doesn't match")

        for i in range(row.getAttributeSize()):
            type = CatalogManager.getType(tableName, i)
            length = CatalogManager.getLength(tableName, i)
            if not RecordManager.checkType(type, length, row.getAttributeValue(i)):
                return False

        return True

    @staticmethod
    def checkCondition(tableName, conditions):
        for condition in conditions:
            index = CatalogManager.getAttributeIndex(tableName, condition.name)
            if index == -1:
                raise ValueError("Can't not find attribute " + condition.name)
            type = CatalogManager.getType(tableName, index)
            length = CatalogManager.getLength(tableName, index)
            if not RecordManager.checkType(type, length, condition.value):
                return False
        return True

    @staticmethod
    def checkType(type, length, value):
        if type == "INT":
            try:
                int(value)
            except ValueError:
                raise ValueError(value + " doesn't match int type or overflow")
        elif type == "FLOAT":
            try:
                float(value)
            except ValueError:
                raise ValueError(
                    value + " doesn't match float type or overflow")
        elif type == "CHAR":
            if length < len(value):
                raise ValueError("The char number " + value +
                                 " must be limited in " + str(length) + " bytes")
        else:
            raise ValueError("Undefined type of " + type)
        return True
