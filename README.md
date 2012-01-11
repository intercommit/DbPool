DbPool
======

A bare-bone (JDBC) database pool, used in our 24x7 production systems.

Like SocketDistrib (https://github.com/intercommit/SocketDistrib)
it is small (10 classes) and fast (using Java concurrent).
DbPool was designed to release only valid connections from the pool.
This incurs some overhead (e.g. checking that a connection is valid
before releasing it) but makes up for it in stability.
During database fail-over testing for example, DbPool showed results on par
with the database pool in JBoss.

DbPool is bare-bone in that it manages a database connection pool, 
it does not do any kind of caching (query and/or (prepared) statement).
DbPool classes are designed to be extended when new/other functionality is needed.

DbPool includes a watcher (thread) that can automatically close idle connections 
and can also keep track of how long a connection is in use by one thread 
(and can interrupt that thread if needed).

A number of helper-classes are included that make it easy to fire a query.
The DbConn-class for example bundles the pool with a query and a result-set
so that a minimum of statements is needed to fire a query and close resources. 
Named Queries are supported via the NamedParameterStatement-class). 

For a usage example, have a look at the test-classes (e.g. TestDbPools.java).
For a more comprehensive example look at the test-class "RunMySQL.java",
edit it, and run it using "testmysql".

Support for HSQL and MySQL is included, support for other databases can easily 
be created by implementing the "DbConnFactory" interface.
