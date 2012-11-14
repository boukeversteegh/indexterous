#!/usr/bin/python
#-*- coding:utf-8 -*-
import jenkins
import sys
import struct
import json

"""
This indexing tool allows you to add data items to keys.
Keys are collected in buckets, and when retrieving data for the key,
the entire bucket is retrieved, but it can be filtered by your own function.

For example:
  index.addkeydata('mykey', "helloworld")

Because keys will be hashed, collisions will occur.
This means that when you retrieve the key:

  index.getallkeydata('mykey')

Some data from other keys will be included.

To retrieve the correct data, you need to somehow store the key in the data as well. For example by prepending it to your data string.
  index.addkeydata('mykey', "mykey:helloworld")

then you can filter it:
  filter=lambda key, data: data[0:len(key)+1] == key + ':'
  index.getallkeydata('mykey', filter)


You can choose the size of the hashes, so that the buckets will be bigger. Filtering will always be necessary.

"""

class Index:
  def __init__(self, filename, size=0xFFFF, pointersize, keyfilter=None):
    self.filename=filename
    self.size=size # Size of hash (number of buckets). 0xFFFF == 65536 buckets.

    # To filter only the target data from bucket.
    if keyfilter==None:
      # Matches all, no filtering.
      # self.keyfilter= lambda key, data: return True
    else:
      self.keyfilter=keyfilter 
    self.pointersize=pointersize
    self.dbsizesize=pointersize/4
    types={
      "2":"h",
      "4":"I",
      "8":"Q"
    }
    

    self.dbsizetype=types[str(self.dbsizesize)]
    self.pointertype=types[str(self.pointersize)]

    # Create index and database
    open(filename+'.idx', "ab+").close()
    open(filename+'.db', "ab+").close()
    self.idx=open(filename+'.idx', "r+")
    self.db=open(filename+'.db', "r+")
  
  def keyhash(self, input):
    return jenkins.hashlittle(input) & self.size

  def build_index(self):
    self.idx.seek(0)
    self.idx.truncate()
    dbpointer=0 # Points to first item, but is ignored.
    for i in range(0, self.size):
      self.idx.write(struct.pack(self.pointertype*2, i, dbpointer)) 

  def get(self, key):
    hash=self.key_hash(key)
    self.f.seek(hash*2)
     
  def getnext(self, index):
    offset=self.getindexoffset(index)

    self.idx.seek(offset)
    rnext=self.idx.read(self.pointersize)
    if len(rnext) < self.pointersize:
      return index
    else:
      next, = struct.unpack(self.pointertype, rnext)
      return next

  def getindexdata(self, idxoffset):
    idx=self.idx
    db=self.db
    next, dbpointer=self.getindex(idxoffset)

    if next == idxoffset:
      return None
    return self.readdatabase(dbpointer)
  
  def getallindexdata(self, index):
    next, dbpointer = self.getindex(index)
    curindex = index
    data=[]
    while next != curindex:
      data.append(self.readdatabase(dbpointer))
      curindex=next
      next, dbpointer=self.getindex(next)
    return data

  def getlast(self, index):
    current=index
    next = self.getnext(current)
    while next != current:
      current=next
      next = self.getnext(current)
    return next

  def getindex(self, index):
    idx=self.idx    
    idx.seek(self.getindexoffset(index))
    next, dbpointer = struct.unpack(self.pointertype*2, idx.read(self.pointersize*2))
    return (next, dbpointer)

  def readdatabase(self, dbpointer):
    db=self.db    
    db.seek(dbpointer)
    size,=struct.unpack(self.dbsizetype, db.read(self.dbsizesize))
    if size == 0:
      return None
    else:
      data, = struct.unpack(str(size)+'s', db.read(size))
      return data

  def setdata(self, offset, data):
    olddata = self.getdata(offset)
    len(olddata)

  def writeindex(self, index, next, dbpointer):
    self.idx.seek(self.getindexoffset(index))
    self.idx.write(struct.pack(self.pointertype*2, next, dbpointer))
  
  def getindexoffset(self, index):
    return index*self.pointersize*2

  def writedatabase(self, offset, string):
    db=self.db
    size=len(string)
    db.seek(offset)

    roldsize=db.read(self.dbsizesize)
    if len(roldsize) == self.dbsizesize:
      oldsize,=struct.unpack(self.dbsizetype, roldsize)
    else:
      # past end of file      
      oldsize=0

    # Allow overwriting of data only when sizes are exactly the same.
    if oldsize > 0 and oldsize != size:
      raise Exception("Overwriting existing data!")
    else:
      db.seek(offset)
      format=self.dbsizetype+str(len(string))+'s'
      db.write(struct.pack(format, size, string))

  # Append data to an index (bucket)
  def addindexdata(self, index, data):
    last=self.getlast(index)
    idx=self.idx
    db=self.db

    idx.seek(0,2)
    idxend=idx.tell()

    newindex=max(idxend/(self.pointersize*2), self.size)
    
    db.seek(0,2)
    dbend=db.tell()

    self.writeindex(last, newindex, dbend)
    self.writeindex(newindex, newindex, 0)

    self.writedatabase(dbend, data)

  def addkeydata(self, key, data):
    index=self.keyhash(key)
    self.addindexdata(index, data)

  def getkeydata(self, key, keyfilter=None):
    if keyfilter is None:
      keyfilter=self.keyfilter
    index=self.keyhash(key)
    alldata=self.getallindexdata(index)
    if keyfilter is None:
      return alldata
    else:
      return [ data for data in alldata if keyfilter(key, data)]
    

size=0xFF # Size of hash. Divides all keys over 255 buckets.
filepointersize=8 # Number of bytes used to index positions in the database file (.db), and index file (.idx)
indexfile='/tmp/indexterous'

keyfilter=lambda key, data: json.loads(data)[0]==key
keyfilter=lambda key, data: data[0:len(key)+1]==key+':'

idx=Index(indexfile, size, filepointersize, keyfilter)

# Create an empty index file
idx.build_index()

for i in range(0,1000):
  idx.addkeydata(str(i), idx.keyhash(str(i))+':'+str(hash(str(i))))
  pass

for i in range(0,100):
  print idx.getallindexdata(i)
