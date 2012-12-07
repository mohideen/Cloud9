Cloud9lib is a project developed at University of Maryland to assist students and researchers in Data-Intensive text processing applications. Cloud9lib has various modules that illustrate different MapReduce algorithms that are explained in the “Data-Intensive Text Processing with MapReduce” book by Professor Jimmy Lin and Chris Dyer. Currently, this library doesn’t have examples for every the important concepts in the book. I would add a module to this library to demonstrate the relational join techniques explained in the book. 

**Relational Join Module**

Relational Joins are operations performed to combine different data structures together and gain meaning information from them. Relational Joins are essential for any data warehousing application. When the data to be processed becomes huge or the data to be processed is not fully structured, the traditional methods are not always efficient. Hadoop framework allows us to design algorithms that will run on both structured and unstructured data even in the petabyte scale.

There are four Relational Join Techniques explained in Professor Lin’s book: 

- Reduce-Side Joins – Caters applications with unorganized data. Less efficient.
- Map-side Joins – Efficient technique but requires sorted input data.
- Memory Backed Joins – Efficient when one of the datasets completely fits into the memory of the nodes in the cluster.
- Memcached Joins – A novel approach for holding one of datasets in-memory on a distributed store when the dataset is too big to fit into the memory of a single node.

These techniques have advantages and disadvantages over each other, and they cater different application needs. This module implements Reduce-Side Join and Map-side Join techniques. Full documentation available at: [http://mohideen.github.com/Cloud9/](http://mohideen.github.com/Cloud9/)

