class Address:
    def __init__(self, fileName=None, blockOffset=0, byteOffset=0):
        self.fileName = fileName
        self.blockOffset= blockOffset
        self.byteOffset = byteOffset

    def __lt__(self, other):
        if self.fileName == other.fileName:
            if self.blockOffset == other.blockOffset:
                return self.byteOffset < other.byteOffset
            else:
                return self.blockOffset < other.blockOffset
        else:
            return self.fileName < other.fileName
