# Hadoop MapReduce
This repository contains a Java implementation of a Hadoop MapReduce program as part of my cloud computing optimisation coursework. This is developed through GCP to create n-grams using 10,000 e-book titles taken from [Project Gutenberg](https://www.gutenberg.org/). Specifically, the program creates sequences of 4 consecutive words. 

# Implementation
The 4-gram implementation is done by using four string variables. Punctuation is first removed from the
input in the mapper, which is then string-tokenised. When there are tokens to store in the string
variables, the string variables are used to create an engram. Afterwards, words are shifted in a
direction once to create the next engram, until there are no tokens left.

# Alphabetical Sorting
The output of 4-grams is sorted alphabetically by their first word globally, across all reducers. This is
done using a custom partitioner I have implemented, which allows the workload of all reducers to be
balanced as much as possible. The partitioner partitions map output by using the ASCII decimal value
of the first character of the first word to identify what map outputs are grouped to go to the same
reducer. I have specifically specified 6 reducers to group together and separately reduce numbers,
letters a to g, letters h to m, letters n to s, letters t to z, and other special characters. In the final
resulting output found in the bucket, the first output would be all the special characters sorted,
followed by the numbers and finally the alphabet.
