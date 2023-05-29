from Block import Block
import os


class BufferManager:

	MAXBLOCKNUM = 50
	EOF = -1
	buffer = [Block() for _ in range(50)]

	@staticmethod
	def initialBuffer():
		BufferManager.buffer = [Block() for _ in range(BufferManager.MAXBLOCKNUM)]

	@staticmethod
	def testInterface():
		b = Block()
		b.writeInteger(1200, 2245)
		b.writeFloat(76, 2232.14)
		b.writeString(492, "!!!httnb!")
		b.fileName = "buffer_test"
		b.blockOffset = 15
		BufferManager.buffer[1] = b
		BufferManager.writeBlockToDisk(1)

	@staticmethod
	def destructBufferManager():
		for i in range(BufferManager.MAXBLOCKNUM):
			if BufferManager.buffer[i].isValid:
				BufferManager.writeBlockToDisk(i)

	@staticmethod
	def makeInvalid(filename):
		for i in range(BufferManager.MAXBLOCKNUM):
			if (BufferManager.buffer[i].fileName is not None and
							BufferManager.buffer[i].fileName == filename):
				BufferManager.buffer[i].isValid = False


	@staticmethod
	def readBlockFromDiskQuote(fileName: str, ofs: int) -> Block:
		for i in range(BufferManager.MAXBLOCKNUM):
			if BufferManager.buffer[i].isValid and BufferManager.buffer[i].fileName == fileName and BufferManager.buffer[i].blockOffset == ofs:
				break
		if i < BufferManager.MAXBLOCKNUM:
			return BufferManager.buffer[i]
		else:
			bid = BufferManager.getFreeBlockId()
			if bid == BufferManager.EOF or not os.path.exists(fileName):
				return None
			with open(fileName, "rb") as file:
				if not BufferManager.readBlockFromDisk(fileName, ofs, bid, file):
					return None
			return BufferManager.buffer[bid]


	@staticmethod
	def readBlockFromDisk(filename, ofs, bid, file):
		flag = False
		try:
			file.seek(ofs * Block.BLOCKSIZE)
			data = file.read(Block.BLOCKSIZE)
			if not data:
				data = bytearray(Block.BLOCKSIZE)
			flag = True
		except Exception as e:
			print(e)
		if flag:
			BufferManager.buffer[bid].resetModes()
			BufferManager.buffer[bid].blockData = data
			BufferManager.buffer[bid].filename = filename
			BufferManager.buffer[bid].blockOffset = ofs
			BufferManager.buffer[bid].isValid = True
		return flag

	@staticmethod
	def writeBlockToDisk(bid):
		if not BufferManager.buffer[bid].isDirty:
			BufferManager.buffer[bid].isValid = False
			return True
		try:
			out = open(BufferManager.buffer[bid].filename, 'wb')
			out.seek(BufferManager.buffer[bid].blockOffset * Block.BLOCKSIZE)
			out.write(BufferManager.buffer[bid].blockData)
			out.close()
		except Exception as e:
			print(e)
			return False
		BufferManager.buffer[bid].isValid = False
		return True

	@staticmethod
	def getFreeBlockId():
		index = BufferManager.EOF
		mincount = 0x7FFFFFFF
		for i in range(BufferManager.MAXBLOCKNUM):
			if not BufferManager.buffer[i].isLocked and BufferManager.buffer[i].LRUCount < mincount:
				index = i
				mincount = BufferManager.buffer[i].LRUCount
		if index != BufferManager.EOF and BufferManager.buffer[index].isDirty:
			BufferManager.writeBlockToDisk(index)
		return index


if __name__ == "__main__":
    BufferManager.testInterface()
