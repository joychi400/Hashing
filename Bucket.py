# A Bucket can contain one or more Record objects. It wraps
# a Block contained within an ExtendibleHashedFile.
class Bucket:
    # Creates a new Bucket for the given ExtendibleHashedFile (hashfile),
    # with an optional local depth (depth) and block number within the
    # file (blockNumber).
    #
    # hashfile and blockNumber are used together to determine
    # which file and where within that file to read and write the
    # contents of the Bucket.
    #
    # This version of Bucket makes unnecessary disk accesses.
    # That could be fixed in any number of ways, but essentially
    # the main optimization/fix involves having Bucket
    # simply store a list of Record objects in memory,
    # and then only write them to disk when necessary.
    #
    # This also means optimizing things like how Records are
    # actually structured, so that (for example), you can a "delete"
    # record in memory (by flagging as deleted in some way),
    # and then deleted records can be written back to disk
    # as part of a single write operation that writes
    # all the records in a bucket to disk at once.
    #
    # In this case, appending would be similar in that a Bucket
    # stores Records in a list, and writes them out together
    # in a single disk write when a Bucket's contents need to
    # be flushed to disk.
    def __init__(self, depth = 0, blockNumber = 0):
        self.hashfile = hashfile
        self.depth = depth
        self.blockNumber = blockNumber

    # Places a record in this Bucket, returning True if the record
    # was successfully inserted into the Bucket, and False if the Bucket
    # is full.
    def append(self, record):
        return self.hashfile.appendBlock(self.blockNumber, record)

    # Returns all the non-deleted records currently in this Bucket.
    def getRecords(self):
        return self.hashfile.readBlock(self.blockNumber).getAllRecords()

    # Clears (deletes) all the records in this bucket.
    def clear(self):
        return self.hashfile.clearBlock(self.blockNumber)
