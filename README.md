# Minibase
Minibase is a relational database management system. Its design is inspired from <a href="http://research.cs.wisc.edu/coral/mini_doc/minibase.html">Minibase</a> (RDBMS developed at University of Wisconsin). Most of the algorithms have been drawn from <a href="https://www.amazon.com/Database-Management-Systems-Raghu-Ramakrishnan/dp/0072465638">Database Management Systems</a> by Ramakrishnan and Gehrke. 

# Components
* Storage
* Query Processor
* Query Optimizer
* Transactions
* Recovery

## Storage
Supports accessing stored data on disk. [BufferPool](src/main/java/minibase/BufferPool.java) is the main class which supports other components in tuples insertion, deletion, page eviction, locks acquisition and release, transaction commit or abort. All the records are stored in [HeapFile](src/main/java/minibase/HeapFile.java).


## Query Processor
Parser converts the query into a logical plan representation and then calls query optimizer to generate an optimal plan. Supported operations are
* Filters and Joins
* Aggregates (COUNT, SUM, AVG, MIN, MAX)
* SELECT, INSERT, DELETE


## Query Optimizer
Query optimizer implements selectivity estimation framework and cost-based optimizer. 
* TableStats estimates selectivities of filters and cost of scans using historgrams
* JoinOptimizer estimates the cost, selectivities and optimal ordering of series of joins


## Transactions
Transactions system uses strict two phase locking. It tracks the locks held by each transaction and grant locks to transactions as they are needed.
* <b>Buffer Management</b> It uses NO STEAL / FORCE buffer management policy. 
* <b>Locking</b> Shared or exclusive lock is used based on transaction's read or write request.
* <b>Deadlock</b> Deadlock is detected using dependency graph. The code is in LockManager.

NO STEAL means that it should not evict updated pages are locked by an uncommitted transaction. FORCE means that on transaction commit, you should force updated pages to disk. 


## Recovery
Recovery is implemented in [LogFile](src/main/java/minibase/LogFile.java). Undo and redo is done at page level (because of page level locking). I have implemented page-level locking because it simplifies handling transactions. If a transaction modified a page, it must have had an exclusive lock on it. Therefore, no other transaction was concurrently modifying it so I can UNDO changes to it by just overwriting the whole page. 


## Future Work
* Statistics estimation in query optimization should consider cost of aggregates. Currently it only supports cost of sequence of joins
* Extend the join cardinality estimation to use a more sophisticated algorithm rather than using simple heuristics
* Extend the cost model to account for caching. The challenge here is multiple joins may be running simultaneously so it is hard to predict how much memory each will have access to using current BufferPool
