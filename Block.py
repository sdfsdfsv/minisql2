import struct

class Block:
      BLOCKSIZE = 4096  # 4KB for 1 block

      def __init__(self):
            self.LRUCount = 0  # block replace strategy
            self.blockOffset = 0  # to check the block
            self.isDirty = False  # true if the block has been modified
            self.isValid = False  # true if the block is valid
            self.isLocked = False  # true if the block is pinned
            self.fileName = ""  # record where the block from
            self.blockData = bytearray([0] * self.BLOCKSIZE)  # allocate 4KB memory for 1 block

      def __str__(self) -> str:
            return f" {self.LRUCount} {self.blockOffset} {self.isDirty} {self.isValid} {self.isLocked} {self.isValid} {self.fileName} {self.blockData}"
      
      def writeData(self, offset:int, data):  # offset from 0 to 4096
            if offset + len(data) > self.BLOCKSIZE:
                  return False
            for i, d in enumerate(data):
                  self.blockData[i + offset] = d
            self.isDirty = True
            self.LRUCount += 1
            return True


      def resetModes(self):
            self.isDirty = self.isLocked = self.isValid = False  # reset all modes
            self.LRUCount = 0  # reset LRU counter


      def readInteger(self, offset):  # read integer from block data -- big-endian method
            if offset + 4 > self.BLOCKSIZE:
                  return 0
            return struct.unpack('>i', self.blockData[offset:offset+4])[0]

      def writeInteger(self, offset:int, val):
            if offset + 4 > self.BLOCKSIZE:
                  return False
            self.blockData[offset:offset+4] = struct.pack('>i', val)
            self.LRUCount += 1
            self.isDirty = True
            return True


      def readFloat(self, offset):
            if offset + 4 > self.BLOCKSIZE:
                  return 0
            dat = self.readInteger(offset)
            return struct.unpack('f', struct.pack('I', dat))[0]

      def writeFloat(self, offset:int, val):
            if offset + 4 > self.BLOCKSIZE:
                  return False
            dat = struct.unpack('I', struct.pack('f', val))[0]
            return self.writeInteger(offset, dat)


      def readString(self, offset: int, length: int) -> str:
            if offset + length > self.BLOCKSIZE:
                  return None
            buf = self.blockData[offset:offset + length]
            self.LRUCount += 1
            return buf.decode()


      def writeString(self, offset: int, string: str) -> bool:
            if offset + len(string.encode()) > self.BLOCKSIZE:
                  return False
            self.blockData[offset:offset + len(string.encode())] = string.encode()
            self.LRUCount += 1
            self.isDirty = True
            return True
            
def test():
      block= Block()
      block.writeString(1,"1234567")
      # print(b.blockData)
      block.writeInteger(100,-1)
      # print(b.blockData)
      block.writeFloat(10,1.1)
      print(block.readFloat(10))
      print(block.readInteger(100))
      print(block.readString(1,7))
      print(block.readString(1,6))
      print(block)
      
      # print(b for b in block.blockData)
      
      
      
if __name__ == "__main__":
      test()