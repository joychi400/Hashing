from ExtendibleHashedFile import *

file = ExtendibleHashedFile(256, 100, 10, 'test1')

hashValue=input('Enter a hash value: ')

file.insert(int(hashValue), "this is a record")

# for i in range(50, 90):
	# file.insert(i, str(i) + "th record")
	# if file.search(i) == None:
		# print("Record " + str(i) + " was not inserted")

# for i in range(60, 70):
	# file.update(i, str(i) + "th UPDATED record")

# file.update(88, "i'm sure this wont break")


# lst = [12,34,200,255]

# strfile = 'C:/RF/test.txt'
# buffer = bytes(lst)

# print(buffer)

# with open(strfile,'bw') as f:
    # f.write(buffer)

# print('File written, reading it back')

# with open(strfile,'br') as f:
    # buffer = f.read(16)
    # print("Length of buffer is %d" % len(buffer))

    # for i in buffer:
        # print(int(i))
