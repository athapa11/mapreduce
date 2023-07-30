# Hadoop MapReduce
This repository contains a Java implementation of a Hadoop MapReduce program as part of my cloud computing optimisation coursework. This is developed through GCP to create n-grams using 10000 e-book titles taken from [Project Gutenberg](https://www.gutenberg.org/). Specifically, the program creates sequences of 4 consecutive words. 

## Implementation
The 4-gram implementation is done by using four string variables. Punctuation is first removed from the
input in the mapper, which is then string-tokenised. When there are tokens to store in the string
variables, the string variables are used to create an engram. Afterwards, words are shifted in a
direction once to create the next engram, until there are no tokens left.

### Global Alphabetical Sorting
The output of 4-grams is sorted alphabetically by their first word globally, across all reducers. This is
done using a custom partitioner I have implemented, which allows the workload of all reducers to be
balanced as much as possible. The partitioner partitions map output by using the ASCII decimal value
of the first character of the first word to identify what map outputs are grouped to go to the same
reducer. I have specifically specified 6 reducers to group together and separately reduce numbers,
letters a to g, letters h to m, letters n to s, letters t to z, and other special characters. In the final
resulting output found in the bucket, the first output would be all the special characters sorted,
followed by the numbers and finally the alphabet.

### Steps taken to optimise execution time
1. Increased the number of worker nodes from 2 to 4 when creating the cluster to maximise
performance.
2. I have prevented the allocation of countless of short-lived objects by creating the objects
before allocating them to every output from the mappers and reducers, this increases
efficiency.
3. Used IntWritable when dealing with non-textual data, as it is inefficient to convert numeric
data to and from UTF8 strings.
4. A Hadoop combiner is used to aggregate the outputs from the mapper by their keys before
being inputted into the reducer, to reduce intermediate data for faster data transfer.
5. Use of Snappy compression to reduce mapper output. The configurations are found in the
code and used in the cluster properties when creating the cluster.
6. Enabling speculative execution to reduce job execution time if tasks take too long to finish,
by setting MapReduce.[map|reduce].speculative to “true” when creating the cluster in
cluster properties (Also found in the code).
7. Combining all the input files before passing them into the mapper using
CombineTextInputFormat class, then splitting the FileInputFormat to a max size of 128MB
each to balance mapper tasks. The configurations are found in the code but are also used in the
cluster properties when creating a cluster.
8. Increased the number of map tasks completed before scheduling reduce tasks, to allow
more resources for mappers. The value for the configuration is found in the code.
9. Increased the memory allocated to the mappers to 2048MB and reducers to 4096MB, to
maximise the use of the memory available.
10. Increased buffer memory to 1024MB which should minimise spills to disk to decrease map
time.
