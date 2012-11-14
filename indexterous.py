#!/usr/bin/python
#-*- coding:utf-8 -*-
import jenkins
import sys
import struct
import json
import cStringIO
import pickle

"""
This indexing tool allows you to add data items to keys.
Keys are collected in buckets, and when retrieving data for the key,
the entire bucket is retrieved, but it can be filtered by your own function.

For example:
  index.addkeydata('mykey', "helloworlself.inmemoryd")

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
  def __init__(self, encoder=None, decoder=None, keyfilter=None, ramdatabase=False, ramindex=False):
    self.idxinmemory = ramindex
    self.dbinmemory  = ramdatabase
    self.encoder=encoder
    self.decoder=decoder
    self.keyfilter=keyfilter
    self.lastcache=dict()
    self.cachelast=True
    pass

  def new(self, filename, size=0xFFFF, pointersize=8):
    self.filename=filename
    self.size=size # Size of hash (number of buckets). 0xFFFF == 65536 buckets.

    self.pointersize=pointersize
    self.dbsizesize=pointersize/4
    types={
      "2":"h",
      "4":"I",
      "8":"Q"
    }

    self.dbsizetype=types[str(self.dbsizesize)]
    self.pointertype=types[str(self.pointersize)]
    self.createhandles()
    self.build_index()
    self.truncate_db()
    self.storemeta()

  def load(self, filename):
    self.filename=filename
    self.loadmeta()
    self.createhandles()

  def loadorcreate(self, filename, size=0xFFFF, pointersize=8):
    try:
      self.load(filename)
    except:
      self.new(filename, size=size, pointersize=pointersize)

  def createhandles(self):
    # Create index and database
    if self.idxinmemory:
      self.idx=cStringIO.StringIO()
      self.idx.write(self.getidxfilehandle().read())
    else:
      self.idx=self.getidxfilehandle()

    if self.dbinmemory:
      self.db=cStringIO.StringIO()
      self.db.write(self.getdbfilehandle().read())
    else:
      self.db=self.getdbfilehandle()

  def loadmeta(self):
    picklefile = self.filename+'.meta.pickle'
    fh = open(picklefile, 'rb')
    meta=pickle.load(fh)
    
    for key, value in meta.items():
      setattr(self,key, value)

  def storemeta(self):
    keys=['pointersize','size','dbsizesize','pointertype','dbsizetype']
    meta={}
    for key in keys:
      meta[key]=getattr(self,key)
    picklefile = self.filename+'.meta.pickle'
    fh=open(picklefile, 'ab+')
    pickle.dump(meta, fh, pickle.HIGHEST_PROTOCOL)
    fh.close()

  def encode(self, key, data):
    if self.encoder is None:
      return data
    else:
      return self.encoder(key,data)

  def decode(self, key, data):
    if self.decoder is None:
      return data
    else:
      return self.decoder(key, data)

  def getdbfilehandle(self):
    open(self.filename+'.db', "ab+").close()
    return open(self.filename+'.db', "r+")
  
  def getidxfilehandle(self):
    open(self.filename+'.idx', "ab+").close()
    return open(self.filename+'.idx', "r+")

  def keyhash(self, input):
    return jenkins.hashlittle(input) & self.size

  def init_index(self):
    self.idx.seek(0,2)
    if self.idx.tell() == 0:
      self.build_index()

  def build_index(self):
    self.idx.seek(0)
    self.idx.truncate()
    dbpointer=0 # Points to first item, but is ignored.
    for i in range(0, self.size):
      self.idx.write(struct.pack(self.pointertype*2, i, dbpointer)) 

  def truncate_db(self):
    self.db.seek(0)
    self.db.truncate()

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
    return self.getdatabase(dbpointer)
  
  def getallindexdata(self, index):
    next, dbpointer = self.getindex(index)
    curindex = index
    data=[]
    while next != curindex:
      data.append(self.getdatabase(dbpointer))
      curindex=next
      next, dbpointer=self.getindex(next)
    return data

  def getlast(self, index):
    if self.cachelast and index in self.lastcache:
      return self.lastcache[index]
    else:
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

  def getdatabase(self, dbpointer):
    db=self.db    
    db.seek(dbpointer)
    rsize=db.read(self.dbsizesize)

    if len(rsize) == 0:
      return None

    size,=struct.unpack(self.dbsizetype, rsize)

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

  def writedatabase(self, offset, data):
    #print offset, repr(data)
    db=self.db
    size=len(data)
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
      format=self.dbsizetype+str(len(data))+'s'
      db.write(struct.pack(format, size, data))
  
  #def writeindexdata(self, index, data):
  #  next, dbpointer = self.getindex(index)
  #  self.writedatabase(dbpointer, data)

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

    self.lastcache[index]=newindex

  def addkeydata(self, key, data):
    index=self.keyhash(key)
    encodeddata = self.encode(key,data);
    self.addindexdata(index, encodeddata)

  def getkeyindex(self, key, keyfilter=None):
    if keyfilter is None:
      keyfilter=self.keyfilter
    index=self.keyhash(key)

    #print "Getting key: ", repr(key)

    next, dbpointer = self.getindex(index)
    curindex = index
    while next != curindex:
      data = self.getdatabase(dbpointer)
      if data is not None:
        #print '--> ', repr(data), len(data)
        if keyfilter(key, data):
          return curindex, next, dbpointer
      curindex=next
      next, dbpointer=self.getindex(next)
    return None, None, None

  def writekeydata(self, key, data):
    index, next, dbpointer=self.getkeyindex(key)
    if dbpointer is None:
      self.addkeydata(key, data)
    else:
      #self.writeindex(index, next, dbpointer)
      encodeddata=self.encode(key, data)
      self.writedatabase(dbpointer, encodeddata)

  def getkeydata(self, key):
    #print repr(key)
    index, next, dbpointer =self.getkeyindex(key)
    if index == next:
      return None
    data = self.getdatabase(dbpointer)
    decoded = self.decode(key, data)
    return decoded

  def increment(self, key, amount=1):
    olddata = self.getkeydata(key)
    if olddata is None:
      self.writekeydata(key, amount)
    else:
      self.writekeydata(key, olddata+amount)
  
  def getallkeydata(self, key, keyfilter=None):
    if keyfilter is None:
      keyfilter=self.keyfilter
    index=self.keyhash(key)
    alldata=self.getallindexdata(index)
    if keyfilter is None:
      return alldata
    else:
      return [ data for data in alldata if keyfilter(key, data)]

  def dumpdb(self):
    i=0
    while True:
      data = self.getdatabase(i)
      if data is None:
        return
      else:
        print data, 
        i=i+len(data)+self.dbsizesize
  def dbsize(self):
    i=0
    size=0
    while True:
      data = self.getdatabase(i)
      if data is None:
        return size
      else:
        i=i+len(data)+self.dbsizesize
        size+=1
    return size

  def flushindex(self):
    if self.idxinmemory:
      idx=self.getidxfilehandle()
      idx.seek(0)
      idx.truncate()
      self.idx.seek(0)
      idx.write(self.idx.read())

  def flushdatabase(self):
    if self.dbinmemory:
      db=self.getdbfilehandle()
      db.seek(0)
      db.truncate()
      self.db.seek(0)
      db.write(self.db.read())

  def flush(self):
    self.flushindex()
    self.flushdatabase()

