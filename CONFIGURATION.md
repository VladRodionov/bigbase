BigBase 1.0.0 Configuration Manual
======

## 1. Introduction

All BigBase - specific configuration options must be defined in hbase-site.xml file. After installation is complete
you must update hbase-site.xml and sync these files on all HBase cluster's nodes.

## 2. BigBase RowCache - specific configuration options

### 2.1 Defines RowCache coprocessor main class
```xml
<property>
<name>hbase.coprocessor.user.region.classes</name>        
        <value>com.inclouds.hbase.rowcache.RowCacheCoprocessor</value>
</property>   
```

### 2.2 Set the maximum RAM memory (in bytes) allocated to RowCache.
```xml
<property>
        <name>offheap.rowcache.maxmemory</name>
        <value>10000000000</value>
</property>
```

### 2.3 Sets approximate maximum number of objects in RowCache.

Approximate maximum number of objects in the cache (per Region server). This is only the estimate -
not the exact limit (there is no limit on number of objects in the cache). This number is just a hint 
for coprocessor which allows to build internal data structures in a most efficient way. The formula to calculate
the maximum number is the following: 
        
          `Max Number of Objects = Max Memory (offheap.rowcache.maxmemory) / Average Object Size`
                            
Make sure to provide good estimate in offheap.rowcache.maxitems to ensure optimal performance.
```xml
<property>
        <name>offheap.rowcache.maxitems</name>
        <value>50000000</value>
</property>
```

### 2.4 Sets the compression codec.

Compression codec (LZ4 , LZ4HC, Snappy, Deflate, None). By default, there is no compression.
```xml
<property>
        <name>offheap.rowcache.compression</name>
        <value>None</value>
</property>
```

### 2.5 Sets the internal direct buffer size.
```xml
<property>
        <name>offheap.rowcache.nativebuffer.size</name>
        <description>Internal direct buffer size in bytes. By default, 256K. This size MUST be greater than the largest row one wants to
                cache.
         </description>
        <value>262144</value>
</property>
```

## 3. BigBase BlockCache - specific configuration parameters

### 3.1 Sets insertion point for LRU-2Q eviction algorithm used in the BlockCache (*).

Insertion point for LRU-2Q eviction algorithm. By default, all new blocks will be inserted into block
cache at insert point of 0.5 (in the middle of a queue). Value 1.0 will move insertion
point to the queue head. (Optional).
```xml
<property>
        <name>offheap.blockcache.young.gen.factor</name>
        <value>0.5</value>
</property>
```

### 3.2 BlockCache maximum size in bytes.  

Block cache size in bytes. The default size is 0 (cache disabled). To enable off heap  BlockCache set 
the size to a value > 0. (Required)
```xml  
<property>
        <name>offheap.blockcache.size</name>
        <value>0</value>
</property>
```

### 3.3   Block cache implementation class (if you have your own). 
(Optional).
```xml
<property>
        <name>offheap.blockcache.impl</name>
        <value></value>
</property>     
```

### 3.4 Sets the size of block references cache 

When external storage (SSD) is enabled, see `offheap.blockcache.storage.enabled`). 

The reference cache stores addresses of data blocks on disk. The overhead is ~ 50 bytes per data block,
so you can size this cache accordingly. The default value is 0.1 * offheap.blockcache.size.
(Optional).
```xml  
<property>
        <name>offheap.blockcache.storage.ref.size</name>
        <value></value>
</property>   
```

### 3.5 Data block compression. 

Block cache compression algorithm (None, LZ4, LZ4HC,  Snappy, Deflate). The default compression is LZ4.
```xml
<property>
        <name>offheap.blockcache.compression</name>
        <value>LZ4</value>
</property>  
```

### 3.6 Sets external storage (L3) enabled/disabled.

External storage (L3 level cache) enabled. Default is false. (Optional)
```xml 
<property>
        <name>offheap.blockcache.storage.enabled</name>
        <value>false</value>
</property>  
```

### 3.7 External storage cache implementation class.

Default is file-based storage. (Optional).
```xml
<property>
        <name>offheap.blockcache.storage.impl</name>
        <value>com.koda.integ.hbase.storage.FileExtStorage</value>
</property>  
```

### 3.8 Sets on heap cache enabled/disabled.

On heap block cache keeps frequently accessed data blocks (`INDEX`, `BLOOM`) to improve overall performance. The default  is 'true' (enabled) (Optional).
```xml               
<property>
        <name>offheap.blockcache.onheap.enabled</name>
        <value>true</value>
</property> 
```

### 3.9 Sets the maximum size of on heap block cache.

On heap block cache size as a ratio of maximum available memory for JVM process. The default  is 0.10 (Optional). Some frequently accessed blocks (`INDEX`, `BLOOM`) are stored on-heap
to improve performance.  
```xml
<property>
        <name>offheap.blockcache.onheap.ratio</name>
        <value>0.10</value>
</property>
```

### 3.10 Is this block cache persistent or not.

Persistence in this context means that cached block are stored on disk upon region server shutdown
and are loaded from disk (see `offheap.blockcache.storage.dir`) upon server's start up. Default is false. (Optional)
```xml   
<property>
        <name>offheap.blockcache.persistent</name>
        <value>false</value>
</property>    
```

### 3.11 Path to local directory where persistent cache data is stored.
See `offheap.blockcache.persistent`. Default: undefined. (Required if `offheap.blockcache.persistent` was enabled)
```xml
<property>
        <name>offheap.blockcache.storage.dir</name>
        <value></value>
</property>    
```

### 3.12 Sets BlockCache snapshots enabled/disabled (**).

Whether online snapshots are enabled for persistent cache.
When enabled, the system periodically make block cache reference data snapshots and store them in the directory 
specified by `offheap.blockcache.storage.dir` configuration parameter.

Default: false. (Optional)        
```xml
<property>
        <name>offheap.blockcache.snapshots.enabled</name>
        <description> 
        </description>
        <value>false</value>
</property> 
```

### 3.13 BlockCache snapshot interval in seconds.

See  `offheap.blockcache.snapshots.enabled`. Default: 600.
(Optional)
```xml
<property>
        <name>offheap.blockcache.snapshots.interval</name>
        <description> 
        	Snapshot interval in seconds, 
        </description>
        <value>600</value>
</property>   
```

### 3.14 Sets the internal direct buffer size.

Internal direct buffer size in bytes. By default, 1M. This must be larger than the largest Key-Value and the block size (which is, by default -64K ).
```xml
<property>
        <name>offheap.blockcache.nativebuffer.size</name>
        <value>1048576</value>
</property>
```

## 4. External File Storage - specific configuration parameters

> **Note:** External File Storage has not been tested in BigBase 1.0. The feature is sheduled for next release (1.1.0)

### 4.1 External storage base directory (local file system). 

Default: unspecified. (Required)

```xml
<property>
        <name>offheap.blockcache.file.storage.baseDir</name>
        <value></value>
</property>  	
```

### 4.2 External storage maximum size in bytes.

Default: 0 (disabled). (Required)	

```xml
<property>
        <name>offheap.blockcache.file.storage.maxSize</name>
        <value>0</value>
</property> 	
```

### 4.3 External storage write buffer size (bytes). 

Default: 8MB. (Optional)

```xml
<property>
        <name>offheap.blockcache.file.storage.bufferSize</name>
        <value>8388608</value>
</property> 	
```

### 4.4 External storage's number of write buffers. 

Default: 10. (Optional)

```xml
<property>
        <name>offheap.blockcache.file.storage.numBuffers</name>
        <value>10</value>
</property> 		
```

### 4.5 External storage's write buffer flush interval (milliseconds).

Default: 30000. (Optional)

```xml
<property>
        <name>offheap.blockcache.file.storage.flushInterval</name>
        <value>30000</value>
</property> 	
```

### 4.6 External storage's data file maximum size (in bytes). 

Default: 2000000000 (~ 2GB) (Optional). Maximum size of a data file is 2GB. (Optional).

```xml
<property>
        <name>offheap.blockcache.file.storage.fileSizeLimit</name>
        <value>2000000000</value>
</property> 	
```

(*) In Rel 1.0.0, LRU-2Q is disabled and LRU is used instead as default.
(\*\*) This feauture is scheduled for 1.1.0 release.
