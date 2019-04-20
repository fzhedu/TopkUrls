# TopkUrls

### Problem: 
Find the top K (100) of the most occurrences of urls from a S (100) GB URL file, note the memory size is limited to 1 GB.

### Solution:
* case 1: the total unique urls can reside into memory.

Calculate top K in memory directly.

* case 2: the total unique urls cannot reside into memory.

Partition the raw files into many smaller files whose size is not large than FILE_SIZE. 
The FILE_SIZE is smaller than 1 GB so that each partition files can calculate top K in memory.

NOTE:

1. It is optimal to partially calculate top K when reading a file in the disk. 
2. The partition files from the raw URL files may execeed FILE_SIZE, so they need to be repartitioned. 

### Implementation
* for case 1:
1. Use a map to count the occurrence of urls from the raw URL file.
2. Use a prioritity queue to get TOP k from the map.

* for case 2:
1. Map and Parition: Use a map to count the occurrence of urls from the raw URL file. If the memory size of the map exceed (FILE_SIZE/2), 
then data in the map are partition and each partitioned data is flashed to a file.
2. Repartition: The partition files in step 1 may execeed FILE_SIZE, so they need to be repartitioned. After repartition, 
the size of some new partition files may also execeed FILE_SIZE, so this step continues until all partition files are small 
enough to reside in limited memory.
3. Reduce all partition: Use a map and a priority queue to calculate partial top K for each partition 
and merge each partial top K to the global Top K.

### Run
1. make
`make`

2. generate data
`./gen.out S U`

3. run topk
`time ./topk.out S U T`

4. check(if the urls are not too large)
`time ./check.out S U T`

NOTE: S, U and T are three positive integers, which mean the Size of URL files, the max number of Unique urls and the target Topk
, respectively. (0 < S < 500, 0 < U < max of uint32, 0 < T < 10000)

### TODO
The current just uses one thead, but I have to attend a conference now. I can optimize the codes in following two aspects.
1. Use multi-threads
2. Optimize accessing I/O
