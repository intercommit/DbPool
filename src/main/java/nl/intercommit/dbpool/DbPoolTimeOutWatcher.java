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

/** A thread running in the background that frequently checks if connections
 * are returned to the pool within the maxLeaseTimMs or have reached maximum idle time.
 * <br>If a lease-time has expired, a warning is logged with a stack-trace of the (presumably hanging) thread.
 * The lease-time is then reset so that this warning will appear every max-lease-time period until 
 * the database connection is released back into the pool.
 * <br>Idle time checks depend on the LIFO nature of the DbPools' idleConnections queue.
 * The PooledConnection's waitTime variable is used to determine if a pooled connection
 * has been idle for too long.
 * @author frederikw
 *
 */
public class DbPoolTimeOutWatcher implements Runnable {

	protected Logger log = Logger.getLogger(getClass());
	
	protected volatile Thread runningThread;
	protected volatile boolean stop;
	
	public int expiredCount;
	public int idledCount;
	
	protected DbPool dbPool;
	/** 
	 * If true, threads that hold on to a database connection for longer 
	 * then the maxLeaseTimeMs, will get interrupted. Should only be used
	 * during testing, not during production.  
	 */
	public boolean interrupt;

	public DbPoolTimeOutWatcher(final DbPool dbPool) {
		super();
		this.dbPool = dbPool;
	}
	
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
			if (expiredCount > 0 || idledCount > 0) { 
				log.info("Database pool time-out watcher closed, idle connections closed: " + idledCount + ", leases expired: " + expiredCount);
			} else if (log.isDebugEnabled()) {
				log.debug("Database pool lease watcher closed.");
			}
			runningThread = null;
		}
	}
	
	/** Checks for leased pooled connections the max-lease expire time. */
	protected void checkLeaseTimeOut() {
		
		final Iterator<PooledConnection> pcs = dbPool.connections.values().iterator();
		while (pcs.hasNext()) {
			final PooledConnection pc = pcs.next();
			if (!pc.isLeased()) continue;
			if (pc.getMaxLeaseTimeMs() < 1L) continue;
			if (pc.getWaitTime() < pc.getMaxLeaseTimeMs()) continue;
			final Thread t = pc.getUser();
			if (t != null && pc.isLeased()) {
				pc.dirty();
				final StackTraceElement[] tstack = t.getStackTrace();
				if (interrupt) t.interrupt();
				pc.resetWaitStart();
				expiredCount++;
				final StringBuilder sb = new StringBuilder("Lease time (");
				sb.append(pc.getMaxLeaseTimeMs()).append(") expired for pooled database connection used by thread ");
				sb.append(t.toString());
				if (interrupt) sb.append(". Thread was interrupted.");
				sb.append("\nStack trace from thread:\n");
				for (final StackTraceElement st : tstack) {
					sb.append(st.getClassName())
					.append("(").append(st.getMethodName())
					.append(":").append(st.getLineNumber()).append(")\n");
				}
				log.warn(sb.toString());
			}
		}
	}
	
	/** Checks for a non-leased pooled connections the idle expire time. */
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
				log.warn("Idle connection got leased after acquirring permit to remove idle connection.");
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
