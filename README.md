massive-ironman
===============

Multi-Threaded duplicate files finder


Features
This is Program to find duplicate files (Files with same content) in a given folder and its sub folders.
Application uses multithreading.
When application finds a duplicate pair, file names of these files are written to results file.
Features a UI to stop, restart and continue, search and writer threads.


Summary of Implementation :
The entire source code has 3 files, 2 for the UI and 1 for main logic. (SearchDuplicate.java).

This project will run in java version 7 (1.7.0_25) or above. 

To implement the Search Thread Pool  -  used the ThreadPoolExecutor class.  This helped in creating and managing thread lifecycle. To implement the Pause and Resume functions, ThreadPoolExecutor was extended and Custom Thread Pool class called PausableThreadPoolExecutor class was created.

To test  the duplicity of files, first created check-sum of the files, if checksum was equal and  the if the  file path was not the same, then files were taken as duplicate files. The check-sum was favored rather than byte by byte comparison because of the speed and nature of the requirement. The Check-sum used the CRC 32 algorithm.

Used the File Hierarchical structure to parallelize checksum testing operation. starting from the folder specified, recursively traversed the sub-folders. if files were found, the checksum was calculated and it was put into a Hashmap Collection. or if a folder was found, a new Task created to recursively check the files and folders.  

The Hashmap from  java.util.Concurrent package was used. This is fast and supports concurrency. On other hand Hasttable implementation is Threadsafe but does not  perform well  in a multi - threaded system.

Hashmap was favoured because it  was needed  to check duplicate files from the entire folder tree, a storage mechanism was needed to keep visited file information  and also a fast and simple solution was needed to check the equality of a file. The concurrent Hashmap provided both fast and concurrent access.

For File I/O operations, the new Java NIO package was used, this provides fast and simple API for file operations.

The Thread Pool managed the allocation of tasks, If more tasks were created than thread pool size, the new tasks were queued. A separate Writer Thread was used to write the duplicate files into output file. A Blocking Queue was used by the tasks to put the duplicate files, while the Writer Thread got the messages from Queue and wrote it to a file. So here the Task Threads (Search Threads) were the Producers and Writer Thread was the Consumer.
