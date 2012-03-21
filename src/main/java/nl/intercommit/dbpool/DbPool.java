/*  Copyright 2011 InterCommIT b.v.
*
*  This file is part of the "DbPool" project hosted on https://github.com/intercommit/DbPool
*
*  DbPool is free software: you can redistribute it and/or modify
*  it under the terms of the GNU Lesser General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  any later version.
*
*  DbPool is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU Lesser General Public License for more details.
*
*  You should have received a copy of the GNU Lesser General Public License
*  along with DbPool.  If not, see <http://www.gnu.org/licenses/>.
*
*/
package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages database connections in a pool.
 * <br> Before using ({@link #acquire()} and {@link #release(Connection)}), 
 * a database connection factory must be set ({@link #setFactory(DbConnFactory)})
 * and {@link #open(boolean)} must be called.
 * <br> This class contains various public fields that can be used to tune the behavior of the pool.
 * By default, the pool does the following:
 * <br> - close and remove connections not used for 1 minute (see {@link DbPoolWatcher#maxIdleTimeMs})
 * <br> - validate connections before leasing them out (see {@link DbConnFactory#validate(Connection)}).
 * If a connection is not valid, it is removed silently and another connection from the pool is fetched
 * (which is also validated etc.). 
 * <br> - warn if connections are not returned to the pool within 2 minutes 
 * (see {@link DbPoolWatcher#maxLeaseTimeMs})
 * <br> - evict connections from the pool when they do not return within 6 minutes
 * (see {@link DbPoolWatcher#evictThreshold}) 
 * <br><br>
 * Connections can be marked as dirty (see {@link #setDirty(Connection)}) 
 * to prevent connections from being re-used by this pool.
 * DbPool checks the dirty-property on check-out (acquire) and check-in (release).
 * DbPool marks connections as dirty when {@link DbPoolWatcher#maxLeaseTimeMs} has expired. 
 * DbPool also uses this property internally to {@link #flush()} the connection pool.
 * <br><br>
 * Connections are created synchronous (one at a time).
 * The database server will appreciate this but bursts of connection-requests will experience delays.
 * If these delays are unwelcome, set the {@link #minSize} at a higher value.
 * <br><br>
 * Any request for a connection will always result in trying to get a connection from the pool for 1 millisecond.
 * This is to prevent creation of connections that are only used once during bursts of connection-requests.
 * The downside is that creation of connections between {@link #minSize} and {@link #maxSize} 
 * will always experience a delay of 1 millisecond.   
 * 
 * @author frederikw
 *
 */
public class DbPool {

	protected Logger log = LoggerFactory.getLogger(getClass());
	
	/** Minimum amount of connections in the pool. Default 1. */
	public int minSize = 1;
	/** Maximum amount of connections in the pool. Default 10. */
	public int maxSize = 10;
	/** The maximum time it may take to get a connection from the pool. */
	public long maxAcquireTimeMs = 50000L;
	/** 
	 * Number of connections created.
	 * <br>This should be equal to {@link DbPool#connectionsInvalid} + {@link DbPoolWatcher#evictedCount}
	 *  + {@link DbPoolWatcher#idledCount}
	 */
	public AtomicLong connectionsCreated = new AtomicLong();
	/** Number of connections removed from pool because they were invalid. */
	public AtomicLong connectionsInvalid = new AtomicLong();
	
	/** All connections in the pool. */
	protected Map<Connection, PooledConnection> connections = new ConcurrentHashMap<Connection, PooledConnection>();
	/** A LIFO queue containing connections ready to be leased. */
	protected LinkedBlockingDeque<PooledConnection> idleConnections = new LinkedBlockingDeque<PooledConnection>();
	/** Manages permits for leasing connections. Permits are released evenly among requestors. */ 
	protected Semaphore connLeaser = new Semaphore(0, true);
	/** Amount of connections in the pool, use instead of connections.size() which is slow. */
	protected final AtomicInteger connectionCount = new AtomicInteger(); 
	/** The connection factory used to create new connections. */
	protected DbConnFactory connFactory;
	/** The pool watcher keeping a watch on idle connections and leased connections that do not return to the pool. */
	protected DbPoolWatcher poolWatcher = new DbPoolWatcher(this);
	/** Indicates if this pool was closed (in which it cannot be opened again). */
	protected volatile boolean closed;
	
	/** @return The factory used to create, close and validate connections. */
	public DbConnFactory getFactory() { return connFactory; }
	/** @param cf The factory used to create, close and validate connections. */
	public void setFactory(final DbConnFactory cf) { connFactory = cf; }
	
	/** 
	 * Used to start the {@link #poolWatcher} (only when {@link DbPoolWatcher#maxLeaseTimeMs}/{@link DbPoolWatcher#maxIdleTimeMs} > 0)
	 */
	public void execute(final Runnable r, final boolean daemon) { 
		
		final Thread t = new Thread(r);
		t.setDaemon(daemon);
		t.start();
	}

	/**
	 * Opens the database pool, initializes the minimum amount of connections and 
	 * starts the connection time-out watcher if {@link DbPoolWatcher#maxLeaseTimeMs}/{@link DbPoolWatcher#maxIdleTimeMs} > 0. 
	 * @param failOnConnectionError If true, a SQLException is thrown when the minimum amount 
	 * of connections to the database could not be created (else an error is logged but the pool is opened).
	 * @throws SQLException When the pool was previously closed 
	 * or no connection factory was set. Otherwise, when failOnConnectionError is false, this error is not thrown.  
	 */
	public void open(final boolean failOnConnectionError) throws SQLException {
		
		if (closed) throw new SQLException("Cannot re-use a closed database connection pool.");
		if (connFactory == null) throw new SQLException("A database connection factory is required.");
		int i = 0;
		try { 
			while (i < minSize) { 
				release(acquire());
				i++;
			}
		} catch (SQLException sqle) {
			if (failOnConnectionError) {
				log.error("Failed to open database pool with connection factory " + connFactory + ". SQL error: " + sqle);
				PooledConnection[] pcs = connections.values().toArray(new PooledConnection[0]);
				for (PooledConnection pc : pcs) removePooledConnection(pc);
				throw sqle;
			}
			log.error("Could not initialize minimum amount of connections for database pool (acquired " + i + " of " + minSize +")." +
					" Used connection factory: " + connFactory, sqle);
		}
		if (poolWatcher != null && (poolWatcher.maxLeaseTimeMs > 0L || poolWatcher.maxIdleTimeMs > 0L)) {
			execute(poolWatcher, true);
		}
	}
	
	/** Sets a {@link DbPoolWatcher}. The watcher is started when the pool is opened (see {@link #open(boolean)}). */
	public void setWatcher(DbPoolWatcher timeOutWatcher) { this.poolWatcher= timeOutWatcher ; }
	/** The time-out watcher, if any (only available after pool is opened and maxLeaseTimeMs/maxIdleTimeMs > 0). */
	public DbPoolWatcher getWatcher() { return poolWatcher; }

	/** Amount of connections available for usage (i.e. ready to be acquired). */
	public int getCountIdleConnections() { return connLeaser.availablePermits(); }
	/** Amount of connections in the pool. */
	public int getCountOpenConnections() { return connectionCount.get(); }
	/** Amount of connections being used (i.e. waiting for release). */
	public int getCountUsedConnections() { return connectionCount.get() - connLeaser.availablePermits(); }
	
	/** Gets a connection from the pool within {@link #maxAcquireTimeMs}. Sets {@link DbPoolWatcher#maxLeaseTimeMs} for the pooled connection. */
	public Connection acquire() throws SQLException { 
		return acquire(maxAcquireTimeMs, (poolWatcher == null ? 0L : poolWatcher.maxLeaseTimeMs)); 
	}
	/** Gets a connection from the pool within acquireTimeOutMs. Sets {@link DbPoolWatcher#maxLeaseTimeMs} for the pooled connection. */
	public Connection acquire(final long acquireTimeOutMs) throws SQLException{ 
		return acquire(acquireTimeOutMs, (poolWatcher == null ? 0L : poolWatcher.maxLeaseTimeMs)); 
	}
	
	/** Gets a connection from the pool within acquireTimeOutMs. Sets leaseTimeOutMs for the pooled connection. */
	public Connection acquire(final long acquireTimeOutMs, final long leaseTimeOutMs) throws SQLException { 
		
		if (closed) throw new SQLException("Database pool is closed.");
		PooledConnection pc = null;
		if (connectionCount.get() < minSize) {
			pc = getNewConnection(leaseTimeOutMs);
			if (pc != null) return pc.dbConn;
		}
		final long startTime = System.currentTimeMillis();
		boolean retry;
		do {
			retry = false;
			pc = getPooledConnection(1L);
			if (pc == null && connectionCount.get() < maxSize) {
				pc = getNewConnection(leaseTimeOutMs);
				if (pc != null) return pc.dbConn;
			}
			if (pc == null) {
				pc = getPooledConnection(acquireTimeOutMs - System.currentTimeMillis() + startTime);
			}
			if (pc != null) {
				if (!pc.isDirty()) {
					try { connFactory.validate(pc.dbConn); }
					catch (SQLException sqle) {
						log.info("Database connection from pool is invalid: " + sqle);
						pc.dirty();
						connectionsInvalid.incrementAndGet();
					}
				}
				if (pc.isDirty()) {
					removePooledConnection(pc);
					pc = null;
					retry = true;
				}
			}
		} while (pc == null && (retry || System.currentTimeMillis() - startTime < acquireTimeOutMs));
		if (pc == null) throw new SQLException("Failed to acquire database connection from pool within " + acquireTimeOutMs + " milliseconds.");
		pc.setLeased(true, leaseTimeOutMs);
		return pc.dbConn; 
	}
	
	protected PooledConnection getNewConnection(final long leaseTimeOutMs) throws SQLException {
		
		PooledConnection pc = null;
		synchronized(connectionCount) {
			if (connectionCount.get() < maxSize) {
				pc = new PooledConnection(connFactory.getConnection(), leaseTimeOutMs);
				connections.put(pc.dbConn, pc);
				connectionCount.incrementAndGet();
				connectionsCreated.incrementAndGet();
				if (log.isDebugEnabled()) log.debug("Created database connection " + pc.dbConn + " for " + connFactory + ", total connections: " + connectionCount.get());
			}
		}
		return pc;
	}
	
	protected PooledConnection getPooledConnection(final long waitTimeMs) throws SQLException {
		
		if (waitTimeMs < 1L) return null;
		PooledConnection pc = null;
		try { 
			if (connLeaser.tryAcquire(waitTimeMs, TimeUnit.MILLISECONDS)) {
				pc = idleConnections.poll();
			}
		} catch (InterruptedException ie) {
			throw new SQLException("Interrupted while trying to acquire a database connection.", ie);
		}
		return pc;
	}
	
	/** Removes a connection from the pool that is in leased state. */
	protected void removePooledConnection(final PooledConnection pc) {
		
		if (!pc.isDirty()) pc.dirty();
		connections.remove(pc.dbConn);
		close(pc.dbConn, true);
	}
	
	/** Uses the factory to close the given database connection. */
	protected void close(final Connection conn, final boolean wasPooled) {

		connFactory.close(conn);
		if (wasPooled) connectionCount.decrementAndGet();
		if (log.isDebugEnabled()) log.debug("Closed database connection " + conn + " for " + connFactory + ", remaining connections: " + connectionCount.get());
	}
	
	/** 
	 * Releases the connection back into the pool so that another thread may use it.
	 * <br> - If the connection was not leased, only a warning is logged:
	 * <br>"Database connection is already released".
	 * <br> - If the connection is marked as dirty, the connection 
	 * is removed from the pool and closed (no warning is logged).
	 * <br> - If a connection was evicted (see {@link DbPoolWatcher#evictThreshold}) 
	 * or is not part of this pool, a warning is logged: 
	 * <br>"Cannot release a database connection that is not in the pool".
	 * In this case, the connection will only be closed. 
	 */
	public void release(final Connection dbConn) {
		
		if (dbConn == null) return;
		PooledConnection pc = connections.get(dbConn);
		if (pc == null) {
			log.warn("Cannot release a database connection that is not in the pool: " + dbConn);
			close(dbConn, false);
			return;
		}
		if (!pc.isLeased()) {
			log.warn("Database connection is already released: " + pc.dbConn);
			return;
		}
		pc.setLeased(false, 0L);
		if (pc.isDirty()) {
			removePooledConnection(pc);
		} else {
			idleConnections.addFirst(pc);
			connLeaser.release();
		}
	}
	
	/**
	 * Marks a connection as dirty which will remove the connection
	 * from the pool and close it.
	 * @return True if the connection was marked as dirty,
	 * false if the connection is not part of this pool.
	 */
	public boolean setDirty(final Connection dbConn) {
		
		PooledConnection pc = connections.get(dbConn);
		if (pc == null) return false;
		pc.dirty();
		return true;
	}
	/** 
	 * Marks all connections as dirty so that they will be closed
	 * and new connections created.
	 */
	public void flush() {
		Iterator<PooledConnection> pcs = connections.values().iterator();
		while (pcs.hasNext()) pcs.next().dirty();
	}
	
	/**
	 * Marks this pool as closed, no more connections will be provided.
	 * Call {@link #close()} to close all open connections.
	 */
	public void closed() { closed = true; }
	
	/**
	 * Closes this pool and immediately closes all connections (blocks until all connections are closed).
	 */
	public synchronized void close() {
		
		if (!closed) closed();
		if (poolWatcher != null) poolWatcher.stop();
		Iterator<PooledConnection> pcs = connections.values().iterator();
		int closedConnections = 0;
		while (pcs.hasNext()) {
			close(pcs.next().dbConn, true);
			closedConnections++;
		}
		connections.clear();
		log.info("Closed " + closedConnections + " database connection(s) for pool " + connFactory + ", total connections created: " + connectionsCreated.get());
	}
	
	@Override
	public String toString() {
		return (connFactory == null) ? super.toString() : getClass().getSimpleName()+":"+connFactory.getUser()+"@"+connFactory.getUrl();
	}
	
	/** 
	 * Retrieves general information and statistics from this pool.
	 * Can only be used after a connection factory ({@link #setFactory(DbConnFactory)})
	 * has been set.  
	 * */
	public String getStatusInfo() {
		
		final String lf = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder("Status of database pool");
		sb.append(" ").append(connFactory.toString()).append(lf);
		
		sb.append(lf).append("Type: ").append(connFactory.getClass().getSimpleName());
		sb.append(lf).append("URL : ").append(connFactory.getUrl());
		sb.append(lf).append("User: ").append(connFactory.getUser()).append(lf);
		
		sb.append(lf).append("Used connections: ").append(getCountUsedConnections());
		sb.append(lf).append("Open connections: ").append(getCountOpenConnections())
		.append(" (minimum: ").append(minSize).append(", maximum: ").append(maxSize).append(")").append(lf);
		
		sb.append(lf).append("Created connections       : ").append(connectionsCreated.get());
		sb.append(lf).append("Closed invalid connections: ").append(connectionsInvalid.get());
		if (poolWatcher != null) {
			if (poolWatcher.maxIdleTimeMs == 0L) {
				sb.append(lf).append("Not watching idle connections.");
			} else {
				sb.append(lf).append("Closed idle connections   : ").append(poolWatcher.idledCount)
				.append(" (maximum idle time: ").append(poolWatcher.maxIdleTimeMs).append(")");
			}
			if (poolWatcher.maxLeaseTimeMs == 0L) {
				sb.append(lf).append("Not watching for expired leases.");
			} else {
				sb.append(lf).append("Number of expired leases  : ").append(poolWatcher.expiredCount)
				.append(" (maximum lease time: ").append(poolWatcher.maxLeaseTimeMs).append(", interrupt expired connections: ")
				.append(poolWatcher.interrupt).append(")");
				if (poolWatcher.evictThreshold == 0) {
					sb.append(lf).append("Not evicting connections.");
				} else {
					sb.append(lf).append("Evicted connections       : ").append(poolWatcher.evictedCount)
					.append(" (close evicted connections: ").append(poolWatcher.closeEvicted)
					.append(", only when user has terminated: ").append(poolWatcher.closeEvictedOnlyWhenUserTerminated).append(")");
					sb.append(lf).append("Number of times a lease on a connection can expire before it is evicted: ")
					.append(poolWatcher.evictThreshold);
				}
			}
			sb.append(lf).append("Time-out watch interval   : ").append(poolWatcher.timeOutWatchIntervalMs);
		}
		sb.append(lf);
		sb.append(lf).append("Maximum connection acquire time: ").append(maxAcquireTimeMs);
		sb.append(lf).append("Time values are in milliseconds.").append(lf);
		return sb.toString();
	}
}
