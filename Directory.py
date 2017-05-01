from ExtendibleBlock import *

# A Directory manages the extendible hashing directory
# for an ExntendibleHashedFile.
class Directory:

    # Creates a new Directory for the given ExtendibleHashedFile (hashfile).
    def __init__(self, hashfile):
        self.hashfile = hashfile
        self.depth = 0
        self.directory = [Bucket(hashfile)]

    # Returns the Bucket containing the record with the given key
    def getBucket(self, key):
        index = self.getBucketIndex(key)
        return self.directory[index]

    # Returns the index into the directory that the given key hashes to
    def getBucketIndex(self, key):
        hash = key % 32
        return self.getRightmostBits(hash, self.depth)

    def getRightmostBits(self, value, count):
        return value % (2**count)

    # This finds the largest block number N in the directory
    # and returns (N + 1).
    # This is a quick way to get a new block number
    # without having to access the file.
    def getFreeBlockNumber(self):
        maxBlockNumber = 0
        for bucket in self.directory:
            if bucket.blockNumber > maxBlockNumber:
                maxBlockNumber = bucket.blockNumber
        return maxBlockNumber + 1

    # Locates and returns the record with the specified key, or None if
    # the record doesn't exist
    def search(self, key):
        bucket = self.getBucket(key)

        for rec in bucket.getRecords():
            if rec.getHashValue() == key:
                return rec
        return None

    # Updates the given Record.
    def update(self, record):
        # There is actually more than way to do this.
        #
        # One way involves re-using `insert` (but you'd need to change
        # `ExtendibleHashedFile.appendBlock` accordingly).
        #
        # Another way involves using `search` (but you'd need a new method
        # on `ExtendibleHashedFile`, probably named `updateBlock`).
        #
        # Either way, for right now, this method does nothing.
        pass

    # Inserts a Record with the specified key into the appropriate Bucket
    # in the directory, expanding the directory and/or performing page splits
    # as needed to accomodate the record.
    def insert(self, key, record):
        bucket = self.getBucket(key)

        # Split the bucket if we can't insert the record into the current bucket
        if not bucket.append(record):

            # The below is based on the algorithm here:
            #   https://en.wikipedia.org/wiki/Extendible_hashing#Example_implementation

            if bucket.depth == self.depth:
                # (local depth) == (global depth) --> double the directory size and increment d
                self.depth += 1
                self.directory *= 2

            # Split the bucket by creating a new bucket in the same position as old one (b1)
            # and a another bucket in a free (unused) block (b2).
            #
            # Both buckets have their depths (local depths) increased by 1.
            b1 = Bucket(self.hashfile, bucket.depth + 1, bucket.blockNumber)
            b2 = Bucket(self.hashfile, bucket.depth + 1, self.getnextAvailableBucket())

            # Read the existing records in, and include the record we originally
            # tried to insert in the list of records.
            records = bucket.getRecords() + [record]

            # Clear (delete) the records that were already present in original
            # bucket, since b1 is pointing at bucket and we don't want old
            # recods to be present in the block as we are reidistributing the
            # records across b1 and b2 (we want a "clean slate").
            bucket.clear()

            # Move each record into the correct new bucket based on the newly
            # available bit in the record's hash key (the leftmost bit that
            # is relevant based on the bucket's new depth).
            #
            # If this bit is 0, the record goes into the first bucket(b1).
            # If this bit is 1, the record goes into the second bucket (b2).
            #
            # So if record R1's hash key is 01 and another record R2's hash key
            # is 10, and the current depth of the new bucket is 2, this will
            # ensure R1 goes into the first bucket (b1), and R2 goes into the
            # second bucket (b2).
            for rec in records:
                index = self.getBucketIndex(rec.getHashValue())
                newBit = (index >> bucket.depth) & 1
                if newBit:
                    b2.append(rec)
                    print("Record {} added to split page 2 (block {})".format(rec.getHashValue(), b2.blockNumber))
                else:
                    b1.append(rec)
                    print("Record {} added to split page 1 (block {})".format(rec.getHashValue(), b1.blockNumber))

            # Finally, look through all the entries in the directory,
            # changing any pointers to the original bucket to the correct
            # new bucket, based on the index of the bucket.
            #
            # Like above, buckets with indices that start with "0" will now
            # point to b1, and buckets with indices that start with "1" will
            # now point to b2.
            for i, b in enumerate(self.directory):
                if b == bucket:
                    newBit = (i >> bucket.depth) & 1
                    if newBit:
                        self.directory[i] = b2
                    else:
                        self.directory[i] = b1
