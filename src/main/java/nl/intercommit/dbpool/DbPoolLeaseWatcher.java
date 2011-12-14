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
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/** A thread running in the background that frequently checks if connections
 * are returned to the pool within the maxLeaseTimMs.
 * If a lease-time has expired, a warning is logged with a stack-trace of the (presumably hanging) thread.
 * The lease-time is then reset so that this warning will appear every max-lease-time period until 
 * the database connection is released back into the pool.
 * @author frederikw
 *
 */
public class DbPoolLeaseWatcher implements Runnable {

	protected Logger log = Logger.getLogger(getClass());
	
	protected Map<Connection, PooledConnection> connections;
	protected volatile Thread runningThread;
	protected volatile boolean stop;
	public int expiredCount;
	public long watchInterval = 1000L;
	/** 
	 * If true, threads that hold on to a database connection for longer 
	 * then the maxLeaseTimeMs, will get interrupted. Should only be used
	 * during testing, not during production.  
	 */
	public boolean interrupt;

	public DbPoolLeaseWatcher(Map<Connection, PooledConnection> connections) {
		super();
		this.connections = connections;
	}
	
	@Override
	public void run() {
		
		runningThread = Thread.currentThread();
		try {
			while (!stop) {
				Iterator<PooledConnection> pcs = connections.values().iterator();
				while (pcs.hasNext()) {
					PooledConnection pc = pcs.next();
					if (!pc.isLeased()) continue;
					if (pc.getMaxLeaseTimeMs() < 1L) continue;
					if (pc.getWaitTime() < pc.getMaxLeaseTimeMs()) continue;
					Thread t = pc.getUser();
					if (t != null && pc.isLeased()) {
						pc.dirty();
						StackTraceElement[] tstack = t.getStackTrace();
						if (interrupt) t.interrupt();
						pc.resetWaitStart();
						expiredCount++;
						
						StringBuilder sb = new StringBuilder("Lease time (");
						sb.append(pc.getMaxLeaseTimeMs()).append(") expired for pooled database connection used by thread ");
						sb.append(t.toString());
						if (interrupt) sb.append(". Thread was interrupted.");
						sb.append("\nStack trace from thread:\n");
						for (StackTraceElement st : tstack) {
							sb.append(st.getClassName())
							.append("(").append(st.getMethodName())
							.append(":").append(st.getLineNumber()).append(")\n");
						}
						log.warn(sb.toString());
					}
				}
				if (!stop) Thread.sleep(watchInterval);
			}
		} catch (InterruptedException ie) {
			if (log.isDebugEnabled()) log.debug("Interrupted while watching connection leases.");
		} catch (Throwable t) {
			log.error("Database pool lease watcher no longer operational due to unexpected error.", t);
		} finally {
			if (expiredCount > 0) { 
				log.info("Database pool lease watcher closed, leases expired: " + expiredCount);
			} else if (log.isDebugEnabled()) {
				log.debug("Database pool lease watcher closed.");
			}
			runningThread = null;
		}
	}
	
	public void stop() {
		stop = true;
		Thread t = runningThread;
		if (t != null) t.interrupt();
	}
}
