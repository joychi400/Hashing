from Record import Record
from Block import Block
from Directory import Directory

import math

class ExtendibleHashedFile:

    def __init__(self, blockSize, recordSize, fieldSize, fileLoc):
        self.file = fileLoc
        self.blockSize = blockSize
        # record size supplied by user should include the hash field size
        # 1 is added for the deletion marker
        self.recordSize = recordSize + 1
        self.fieldSize = fieldSize
        self.blockPointerSize = 4
        self.bfr = math.floor((blockSize-self.blockPointerSize)/self.recordSize)
        self.directory = Directory(self)

        # truncates the file
        with open(self.file, 'wb') as f:
            f.write(b"""This is a file header
            here is some information
            this is only two blocks in size at maximum""")
        # create overflow file
        open(self.file + '_overflow', 'wb').close()

    def insert(self, value, record):
        formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
        self.directory.insert(value, formattedRecord)

    def search(self, value):
        return self.directory.search(value)

    def update(self, value, data):
         pass

    def readBlock(self, blockNum):
        with open(self.file, 'rb', buffering=self.blockSize) as f:
            f.seek(self.blockSize*(blockNum+2))
            return self.makeBlock(f.read(self.blockSize))

    # TODO: It would be cleaner (and reduce disk accesses) to simply append
    #       records directly to a Block, and then have a single
    #       Block.write method that will write the block back
    #       to disk in a single write operation and only when needed.
    def appendBlock(self, blockNum, record):
        with open(self.file, 'r+b', buffering=self.blockSize) as f:
            f.seek(self.blockSize*(blockNum+2))
            theBlock = self.makeBlock(f.read(self.blockSize))

            # If record is already in the block, don't re-insert it
            if theBlock.containsRecordWithValue(record.getHashValue()):
                return True

            # Otherwise check for a free slot and use it if there is one
            space = theBlock.hasSpace();
            if space>=0:
                # spot was open, move pointer back
                f.seek(self.blockSize*(blockNum+2) + self.recordSize*space)
                # slot data in there boiii
                f.write(record.bytes)

                return True
            else:
                # there has been a collision. handle it.
                return False

    # TODO: Instead of clearing the block with a separate method
    #       it would be nicer to have a "deleted" field on the Record class
    #       so the Record's could be marked for deletion, and written to
    #       disk in a single Block.write call or similar.
    def clearBlock(self, blockNum):
        with open(self.file, 'r+b', buffering=self.blockSize) as f:
            emptyRecord = Record.new(self.recordSize, self.fieldSize, 0, "DELETED")
            for i in range(0, self.bfr):
                f.seek(self.blockSize*(blockNum+2) + self.recordSize*i)
                f.write(emptyRecord.bytes)

    def makeBlock(self, data):
        return Block(self.blockSize, self.blockPointerSize, self.recordSize, self.fieldSize, self.bfr, data)
