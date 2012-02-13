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

import org.apache.log4j.Logger;

/** A helper class for {@link DbPool} which keeps track of several pool-properties for a database connection. 
 * Most of these properties are used by the {@link DbPoolTimeOutWatcher}. */
public class PooledConnection {

	protected Logger log = Logger.getLogger(getClass());
	
	/** The connection to the database which is pooled. */
	public final Connection dbConn;
	protected Thread user;
	/** Start-time for this connection to be leased or being idle. */
	protected long waitStart;
	protected boolean dirty;
	protected boolean leased;
	protected long maxLeaseTimeMs;
	/** 
	 * Number of times {@link #maxLeaseTimeMs} expired, used to determine
	 * when to abandon a connection (see {@link DbPool#evictThreshold})
	 */
	protected int leaseExpiredCount;

	/** Creates this pooled connection and sets it's state to leased. */
	public PooledConnection(final Connection dbConn, final long leaseTimeOutMs) {
		super();
		this.dbConn = dbConn;
		setLeased(true, leaseTimeOutMs);
	}
	
	public void setMaxLeaseTimeMs(final long timeOutMs) { maxLeaseTimeMs = timeOutMs; }
	public long getMaxLeaseTimeMs() { return maxLeaseTimeMs; }
	
	public void dirty() {
		if (!dirty) {
			dirty = true;
			if (log.isDebugEnabled()) log.debug("Marked database connection as dirty: " + dbConn);
		}
	}
	
	public boolean isDirty() { return dirty; }
	
	public Thread getUser() { return user; }
	public long getWaitTime() { return (System.currentTimeMillis() - waitStart); }
	public void resetWaitStart() { waitStart = System.currentTimeMillis(); }
	
	public void setLeased(final boolean leased, final long leaseTimeOutMs) {

		this.leased = leased;
		if (leased) {
			setMaxLeaseTimeMs(leaseTimeOutMs);
			user = Thread.currentThread();
			if (log.isTraceEnabled()) log.trace(user + " is leasing " + dbConn);
		} else {
			if (log.isTraceEnabled()) log.trace(user + " released " + dbConn);
			user = null;
		}
		waitStart = System.currentTimeMillis();
	}
	
	public boolean isLeased() { return leased; }
}
