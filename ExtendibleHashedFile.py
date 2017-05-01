from Record import Record
from ExtendibleBlock import Block
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
	#self.blockPointerSize = 4
        self.bfr = math.floor((blockSize-self.blockPointerSize)/self.recordSize)
        self.globalDepth = 0
		self.nextAvailableBucket = 2
	
	    self.Directory = {}
		self.Directory["0"] = 2
	
        # truncates the file
        with open(self.file, 'wb') as f:
            f.write(b"""This is a file header
            here is some information
            this is only two blocks in size at maximum""")
        # create overflow file
        open(self.file + '_overflow', 'wb').close()
	
	def h1(self, value):
		return value % 32
		
	def getBinary(self, value)
	    return "{0.b}".format(value)
		
	def getRightmostBits(self, value, count):
	    return value[:count]
		

    def insert(self, value, record):
	    #using the hash function
		self.getBucketPointer(value)
		#format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
		#open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			#navigate to the appropriate bucket
			#plus 2 is to account for the header
			f.seek(self.blockSize*(bucket + 2))
			#check to see if data exits in this bucket
			theBlock = self.makeBlock(f.read(self.blockSize))
		    space = theBlock.hasSpace()
			if space>=0:
				# spot was open, move pointer back
				f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
				#slot data in there 
				f.write(formattedRecord.bytes)
			
			else:
				# If there's collision split bucket
				if theBlock.localDepth == self.globalDepth:
					newDirectory = {}
					for entry in self.Directory.keys():
						str1 = entry + "0"
						str2 = entry + "1"
						newDirectory[str1] = self.Directory[entry]
						newDirectory[str2] = self.Directory[entry]
					self.globalDepth += 1
					
				#if theBlock.localDepth < self.globalDepth:
				origBucketCount = 0
				grabbedBucketCount = 0
				
				allRecords = theBlock.getAllRecords()
				origBucketCount = 0
				grabbedBucketCount = 0
				
				curr = "0" + rightmost
				next = "1" + rightmost
				for record in allRecords:
				    #use the rightmost significant bits to determine which bucket
					whichBucket  = self.getBucketPointer(record.getHashValue())
					if whichBucket == self.curr:
					        origBucketCount +=1
							if origBucketCount > self.bfr:
							       print("theres need to be a split")
					        else:
								f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(origBucketCount - 1)
								f.write(record.bytes)
					if whichBucket == self.next:
						grabbedBucketCount += 1
						if grabbedBucketCount > self.bfr:
							print("there's need to split")	
						else:
							f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(grabbedBucketCount - 1)
							f.write(record.bytes)
				
				self.nextAvailableBucket += 1
				
				

				theBlock.localDepth += 1
				
			
	def getBucketPointer(self, value):
		hashedValue = self.h1(value)
		binary = self.getBinary(hashedValue)
		rightmost = self.getRightmostBits(binary, self.globalDepth)
		return self.Directory[rightmost]

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
        return ExtendibleBlock(self.size, self.recordSize, self.fieldSize, self.bfr, data)
