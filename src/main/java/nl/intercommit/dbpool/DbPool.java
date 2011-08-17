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
*  along with Weaves.  If not, see <http://www.gnu.org/licenses/>.
*
*/
package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

public class DbPool {

	protected Logger log = Logger.getLogger(getClass());
	public DbConnFactory dbConnFactory;
	
	public int minSize = 1;
	public int maxSize = 10;
	public long maxLeaseTimeMs;
	public long leaseTimeWatchIntervalMs = 1000L;
	public long maxAcquireTimeMs = 50000L;
	public AtomicLong connectionsCreated = new AtomicLong();
	
	protected Map<Connection, PooledConnection> connections = new ConcurrentHashMap<Connection, PooledConnection>();
	protected Queue<PooledConnection> idleConnections = new ConcurrentLinkedQueue<PooledConnection>();
	protected Semaphore connLeaser = new Semaphore(0, true);
	protected final AtomicInteger connectionCount = new AtomicInteger(); 
	protected DbConnFactory connFactory;
	protected DbPoolLeaseWatcher timeOutWatcher;
	protected volatile boolean closed;
	
	public DbConnFactory getFactory() { return connFactory; }
	public void setFactory(DbConnFactory cf) { connFactory = cf; }
	
	/** 
	 * Used to start the timeOutWatcher (only when maxLeaseTimeMs is bigger as 0)
	 */
	public void execute(Runnable r, boolean daemon) { 
		
		Thread t = new Thread(r);
		t.setDaemon(daemon);
		t.start();
	}

	/**
	 * Opens the database pool, initializes the minimum amount of connections and 
	 * starts the connection lease watcher if maxLeaseTimeMs > 0. 
	 * @param failOnConnectionError If true, a SQLException is thrown when the minimum amount 
	 * of connections to the database could not be created (else an error is logged but the pool is opened).
	 * @throws SQLException When the pool could not be opened, was previously closed 
	 * or no connection factory was set.
	 */
	public void open(boolean failOnConnectionError) throws SQLException {
		
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
				PooledConnection[] pcs = connections.values().toArray(new PooledConnection[0]);
				for (PooledConnection pc : pcs) removePooledConnection(pc);
				throw sqle;
			}
			log.error("Could not initialize minimum amount of connections for database pool (acquired " + i + " of " + minSize +").", sqle);
		}
		if (maxLeaseTimeMs > 0L) {
			timeOutWatcher = new DbPoolLeaseWatcher(connections);
			timeOutWatcher.watchInterval = leaseTimeWatchIntervalMs;
			execute(timeOutWatcher, true);
		}
	}
	
	public Connection acquire() throws SQLException { 
		return acquire(maxAcquireTimeMs, maxLeaseTimeMs); 
	}
	public Connection acquire(final long acquireTimeOutMs) throws SQLException{ 
		return acquire(acquireTimeOutMs, maxLeaseTimeMs); 
	}
	
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
	
	protected void removePooledConnection(final PooledConnection pc) {
		
		if (!pc.isDirty()) pc.dirty();
		connections.remove(pc.dbConn);
		close(pc.dbConn);
	}
	
	public void close(final Connection conn) {

		connFactory.close(conn);
		connectionCount.decrementAndGet();
		if (log.isDebugEnabled()) log.debug("Closed database connection " + conn + " for " + connFactory + ", remaining connections: " + connectionCount.get());
	}
	
	public void release(final Connection dbConn) {
		
		if (dbConn == null) return;
		final PooledConnection pc = connections.get(dbConn);
		if (pc == null) {
			log.error("Cannot release a database connection that is not in the pool: " + dbConn);
			close(dbConn);
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
			idleConnections.add(pc);
			connLeaser.release();
		}
	}
	
	/**
	 * Marks a connection as dirty which will remove the connection
	 * from the pool and close it.
	 * @return True if the connection was marked as dirty,
	 * false if the connection is not part of this pool.
	 */
	public boolean setDirty(Connection dbConn) {
		
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
	 * Call close() to close all open connections.
	 */
	public void closed() { closed = true; }
	
	/**
	 * Closes this pool and immediately closes all connections (blocks until all connections are closed).
	 */
	public void close() {
		
		if (!closed) closed();
		if (timeOutWatcher != null) timeOutWatcher.stop();
		Iterator<PooledConnection> pcs = connections.values().iterator();
		while (pcs.hasNext()) {
			close(pcs.next().dbConn);
		}
		log.info("Closed database connection pool for " + connFactory + ", total connections created: " + connectionsCreated.get());
	}
}
