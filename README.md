DbPool
======

A (experimental) bare-bone (JDBC) database pool.
It uses Java concurrent to manage database connection leases and
includes a "lease watcher" that keeps track of how long a
connection is in use by one thread (and can interrupt that thread if needed).

Support for Named Queries is included. 
See the test-classes (e.g. TestDbPools.java) for a usage example.

This DbPool package was created to test what happens when an IP-address
of a database server moves from one machine to another (which is something
that MySQL can do in case of fail-over) while transactions are open. 
The DbPool package provided adequate insight 
and also reproduced the results from database pools in JBoss
(indicating this DbPool implementation is of sufficient quality).

A lot of documentation is still missing, but if you can follow the 
test cases in the test-classes, you should get the gist of it.
For a more comprehensive example look at the test-class "RunMySQL.java",
edit it, and run it using "testmysql".

Support for HSQL and MySQL is included, support for other databases can easily 
be created by implementing the "DbConnFactory" interface.
