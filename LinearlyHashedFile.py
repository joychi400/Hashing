from Record import *
from Block import *
import math

class LinearlyHashedFile:
	
	def __init__(self, blockSize, recordSize, fieldSize, fileLoc):
		self.m = 1
		self.n = 0
		self.file = fileLoc
		self.overflow = fileLoc + "_overflow"
		self.blockSize = blockSize
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		self.recordSize = recordSize + 1
		self.fieldSize = fieldSize
		self.blockPointerSize = 4
		self.bfr = math.floor((blockSize-self.blockPointerSize)/self.recordSize)
		# truncates the file
		with open(self.file, 'wb') as f:
			f.write(b"This is a shorter less ridiculous file header")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))
		# create overflow file
		open(self.overflow, 'wb').close()
	
	def h1(self, value):
		return value % self.m
	
	def h2(self, value):
		return value % (2*self.m)
	
	def insert(self, value, record):
		# pass value to first hash function
		bucket = self.h1(value)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(value)
		# format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# check to see if data exists in this bucket
			theBlock = self.makeBlock(f.read(self.blockSize))
			space = theBlock.hasSpace()
			if space>=0:
				# spot was open, move pointer back
				f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
				# slot data in there boiii
				f.write(formattedRecord.bytes)
				
			else:
				# there has been a collision. handle it.
				print("need a split yoooooo.  " +  str(formattedRecord.getHashValue()) + " did it.")
				
				# here is where we need to put the data into an overflow bucket
				with open(self.overflow, 'r+b') as overflow:
					#print("Pointer: " + str(theBlock.getPointer()))
					# need to ignore the 0 pointer because that'll be null pointer
					# but we don't want to waste the first block in overflow
					# so we could subtract one? or maybe use first block for info
					pointer = theBlock.getPointer() - 1
					# only if the pointer points to something, meaning that this block already has an overflow bucket assigned to it
					if pointer >= 0:
						# navigate to the block in overflow file
						overflow.seek(self.blockSize*pointer)
						# read the block
						overflowBlock = self.makeBlock(overflow.read(self.blockSize))
						# find a spot to put the record
						overflowSpace = overflowBlock.hasSpace()
						# if there's a spot
						if overflowSpace>=0:
							# put it in there! gotta move the pointer back cause we read the block
							overflow.seek(self.blockSize*pointer + self.recordSize*overflowSpace)
							overflow.write(formattedRecord.bytes)
						else:
							print("we're having an overflow... in the overflow. OVERFLOWCEPTION")
						
					# else the bucket doesn't already have an overflow bucket assigned to it. so we must make one.
					else:
						# but where do we make it?
						# linear search through the overflow file? is that gross
						i = 0
						bucketFound = False
						while bucketFound == False:
							# navigate to ith bucket in overflow file
							overflow.seek(i*self.blockSize)
							# load bucket into memory
							aBlock = self.makeBlock(overflow.read(self.blockSize))
							if aBlock.isEmpty():
								# found an empty bucket, end looping
								bucketFound = True
							else:
								# bucket wasn't empty, check the next one
								i += 1
						# navigate to empty bucket
						overflow.seek(self.blockSize*i)
						# write the record there
						overflow.write(formattedRecord.bytes)
						# navigate to block in orig file
						f.seek(self.blockSize*(bucket+3) - self.blockPointerSize)
						# update pointer value
						# add one because we're using 1 based indexing
						f.write((i+1).to_bytes(self.blockPointerSize, byteorder='big'))
				
				# navigate to the bucket to be split
				f.seek(self.blockSize*(self.n+2))
				# load bucket to memory
				bucketToBeSplit = self.makeBlock(f.read(self.blockSize))
				# clear out bucket on disk
				f.seek(self.blockSize*(self.n+2))
				f.write(bytearray(self.blockSize))
				BTBSpointer = bucketToBeSplit.getPointer() - 1
				# see if it is pointing to anything
				allRecords = []
				if BTBSpointer >= 0:
					# this method will only handle one overflow bucket per bucket in the original file
					# eventually I should probably use some sort of list of overflow buckets.
					with open(self.overflow, 'r+b') as overflow:
						# navigate to bucket of interest
						overflow.seek(self.blockSize*BTBSpointer)
						# read bucket into memory
						ofBucketToBeSplit = self.makeBlock(overflow.read(self.blockSize))
						# clear the heck out of that bucket
						overflow.seek(self.blockSize*BTBSpointer)
						overflow.write(bytearray(self.blockSize))
						allRecords.extend(ofBucketToBeSplit.getAllRecords())
				allRecords.extend(bucketToBeSplit.getAllRecords())
				# loop through all records
				origBucketCount = 0
				grabbedBucketCount = 0
				for record in allRecords:
					# use second hash function to deterimine which bucket
					whichBucket = self.h2(record.getHashValue())
					if whichBucket == self.n:
						origBucketCount += 1
						if origBucketCount > self.bfr:
							print("there's needs to be a split within a split")
						else:
							f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(origBucketCount - 1))
							f.write(record.bytes)
					else:
						print("we in the new bucket dawg")
						print(record.getData())
						grabbedBucketCount += 1
						if grabbedBucketCount > self.bfr:
							print("there's needs to be a split within a split")
						else:
							f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(grabbedBucketCount - 1))
							f.write(record.bytes)
				# at this point we have rehashed all records and put them in their appropriate buckets
				# now we need to update n and m and hash functions
				
				# increment n
				self.n += 1
				# if n==m we need to reset
				if self.n == self.m:
					# reset n to zero
					self.n = 0
					# set m to twice m
					# this should update the hash functions as well
					# as they are based off of m
					self.m = 2*self.m
					
	def search(self, value):
		# pass value to first hash function
		bucket = self.h1(value)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(value)
		# open the file as binary read and write
		with open(self.file, 'rb', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if theBlock.containsRecordWithValue(value):
				theRecord = theBlock.getRecordWithValue(value)
				print(theRecord.bytes)
			else:
				pointer = theBlock.getPointer() - 1
				with open(self.overflow, 'rb' buffering=self.blockSize) as overflow:
					overflow.seek(self.blockSize*pointer)
					self.makeBlock(overflow.read(self.blockSize))
			# just print it? record should probably have some sort of pretty print function
			print(theRecord.bytes)
		
	def update(self, value, data):
		# pass value to first hash function
		bucket = self.h1(value)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(value)
		# format the record to overwrite with
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, data)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			recLoc = theBlock.getRecordWithValueLoc(value)
			# navigate to the record to be updated
			f.seek(self.blockSize*(bucket+2) + self.recordSize*recLoc)
			# write over the old record with new formatted one
			f.write(formattedRecord.bytes)
			
	
	def makeBlock(self, data):
		return Block(self.blockSize, self.blockPointerSize, self.recordSize, self.fieldSize, self.bfr, data)
