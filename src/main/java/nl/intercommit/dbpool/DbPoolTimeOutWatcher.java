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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/** 
 * A thread running in the background that frequently (see {@link DbPool#timeOutWatchIntervalMs})
 * checks if connections are returned to the pool within the maximum lease time (see {@link DbPool#maxLeaseTimeMs})
 * or have reached maximum idle time (see {@link DbPool#maxIdleTimeMs}).
 * <br>There can be different reasons for a lease time-out:
 * <br> A - the code that acquired a connection, did not release it (programming error)
 * <br> B - the database is very busy or the executed query takes a long time to complete
 * <br> C - the thread that is using a connection is hanging (e.g. waiting on I/O)
 * <br> When in testing/acceptance, set the {@link DbPool#maxLeaseTimeMs} at a low value so you can catch cases A and B.
 * <br> When in production, set the {@link DbPool#maxLeaseTimeMs} at a very high value 
 * (so you do not get endless amount of warnings when the database server is very busy)
 * and consider setting {@link #interrupt} to true. {@link #interrupt} may help unlock hanging threads.   
 * <br>If a lease-time has expired, a warning is logged with a stack-trace of the thread that acquired the connection.
 * The lease-time is then reset so that this warning will appear every max-lease-time period until 
 * the database connection is released back into the pool.
 * <br>Idle time-out checks depend on the LIFO nature of the {@link DbPool#idleConnections}'s queue.
 * @author frederikw
 *
 */
public class DbPoolTimeOutWatcher implements Runnable {

	protected Logger log = Logger.getLogger(getClass());
	
	protected volatile Thread runningThread;
	protected volatile boolean stop;
	
	public int expiredCount;
	public int idledCount;
	public int evictedCount;
	
	protected DbPool dbPool;
	/** 
	 * If true, threads that hold on to a database connection for longer 
	 * then {@link DbPool#maxLeaseTimeMs}, will get interrupted. 
	 * Use this with much care.  
	 */
	public boolean interrupt;

	public DbPoolTimeOutWatcher(final DbPool dbPool) {
		super();
		this.dbPool = dbPool;
	}
	
	/**
	 * Checks connections for idle-timeout and lease-timeout at regular intervals
	 * ({@link DbPool#timeOutWatchIntervalMs}).
	 * <br>Call {@link #stop()} to stop running. 
	 */
	@Override
	public void run() {
		
		runningThread = Thread.currentThread();
		try {
			while (!stop) {
				checkLeaseTimeOut();
				checkIdleTimeOut();
				if (!stop) Thread.sleep(dbPool.timeOutWatchIntervalMs);
			}
		} catch (InterruptedException ie) {
			if (log.isDebugEnabled()) log.debug("Interrupted while watching connection time-outs.");
		} catch (Throwable t) {
			log.error("Database pool time-out watcher no longer operational due to unexpected error.", t);
		} finally {
			if (expiredCount > 0 || idledCount > 0 || evictedCount > 0) { 
				log.info("Database pool time-out watcher closed, idle connections closed: " + idledCount 
						+ ", leases expired: " + expiredCount
						+ ", evicted connections: " + evictedCount);
			} else if (log.isDebugEnabled()) {
				log.debug("Database pool lease watcher closed.");
			}
			runningThread = null;
		}
	}
	
	/** 
	 * Checks for leased pooled connections the max-lease expire time. 
	 * If max-lease time has expired:
	 * <br> - The thread holding the connection can be interrupted (see {@link #interrupt})
	 * <br> - A warning is logged
	 * <br>If max-lease time has expired for the {@link DbPool#evictThreshold}' time,
	 * the connection is removed from the pool (a.k.a. evicted, see also 
	 * {@link #evictConnection(PooledConnection, StackTraceElement[])}).
	 */
	protected void checkLeaseTimeOut() {
		
		final Iterator<PooledConnection> pcs = dbPool.connections.values().iterator();
		while (pcs.hasNext()) {
			final PooledConnection pc = pcs.next();
			if (!pc.isLeased()) continue;
			if (pc.getMaxLeaseTimeMs() < 1L) continue;
			if (pc.getWaitTime() < pc.getMaxLeaseTimeMs()) continue;
			final Thread t = pc.getUser();
			if (t != null && pc.isLeased()) {
				final StackTraceElement[] tstack = t.getStackTrace();
				pc.dirty();
				pc.leaseExpiredCount++;
				if (dbPool.evictThreshold > 0 && pc.leaseExpiredCount >= dbPool.evictThreshold) {
					evictConnection(pc, tstack);
					continue;
				}
				expiredCount++;
				if (interrupt) t.interrupt();
				pc.resetWaitStart();
				final StringBuilder sb = new StringBuilder("Lease time (");
				sb.append(pc.getMaxLeaseTimeMs()).append(") expired for pooled database connection used by thread ");
				sb.append(t.toString());
				if (interrupt) sb.append(". Thread was interrupted.");
				sb.append("\nStack trace from thread:\n");
				addStackTrace(sb, tstack);
				log.warn(sb.toString());
			}
		}
	}
	
	/** Adds a description of the stack-trace to the stringbuilder. */
	protected void addStackTrace(final StringBuilder sb, final StackTraceElement[] tstack) {

		for (final StackTraceElement st : tstack) {
			sb.append(st.getClassName())
			.append("(").append(st.getMethodName())
			.append(":").append(st.getLineNumber()).append(")\n");
		}
	}
	
	/** 
	 * Evicts a pooled and leased connection from the pool. 
	 * Does not close the connection.
	 */
	protected void evictConnection(final PooledConnection pc, final StackTraceElement[] tstack) {
		
		final String connDesc = pc.dbConn.toString();
		evictedCount++;
		dbPool.connections.remove(pc.dbConn);
		dbPool.connectionCount.decrementAndGet();
		final StringBuilder sb = new StringBuilder("Evicting database connection from pool after lease time expired ");
		sb.append(dbPool.evictThreshold).append(" times.\n");
		sb.append("Connection: ").append(connDesc);
		sb.append("\nStack trace from thread:\n");
		addStackTrace(sb, tstack);
		log.warn(sb.toString());
	}
	
	/** Checks for non-leased pooled connections the idle expire time. */
	protected void checkIdleTimeOut() throws InterruptedException{
		
		if (dbPool.maxIdleTimeMs == 0L || dbPool.connectionCount.get() <= dbPool.minSize) return;
		// Fetch the idle connection that has been waiting the longest time.
		PooledConnection pc = dbPool.idleConnections.peekLast();
		while (pc != null && pc.waitStart + dbPool.maxIdleTimeMs < System.currentTimeMillis()) {
			// Try to remove the idle connection from the pool
			// First decrease amount of available connections.
			final boolean haveLease = dbPool.connLeaser.tryAcquire(1L, TimeUnit.MILLISECONDS);
			if (!haveLease) return; // Sudden busy moment: all connections got leased, so no idle time-outs.
			if (pc.isLeased()) { // Should not happen, but better safe then sorry
				dbPool.connLeaser.release();
				log.warn("Idle connection got leased after acquiring permit to remove idle connection.");
				return;
			}
			// Remove connection from pool
			try {
				PooledConnection pcRemoved = dbPool.idleConnections.removeLast();
				if (pc != pcRemoved) { // Should not happen, but better safe then sorry
					dbPool.idleConnections.addLast(pcRemoved);
					dbPool.connLeaser.release();
					log.warn("Idle connection no longer last in queue after acquiring permit to remove idle connection.");
					return;
				}
			} catch (NoSuchElementException nse) {
				dbPool.connLeaser.release();
				log.warn("Idle connection no longer in queue after acquiring permit to remove idle connection.");
				return;
			}
			dbPool.removePooledConnection(pc);
			idledCount++;
			log.info("Removed an idle connection from database pool " + dbPool.connFactory);
			if (dbPool.connectionCount.get() > dbPool.minSize) {
				pc = dbPool.idleConnections.peekLast();
			} else {
				pc = null;
			}
		}
	}
	
	public void stop() {
		stop = true;
		Thread t = runningThread;
		if (t != null) t.interrupt();
	}
}
