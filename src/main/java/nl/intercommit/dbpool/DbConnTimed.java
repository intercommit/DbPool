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
/**
 * Same as {@link DbConn} but with time measurements for "acquire from pool" and "connection used".
 * These measurements can be printed out using the getStats() method. 
 * @author frederikw
 *
 */
public class DbConnTimed extends DbConn {

	public int connAcquireCount;
	/** The total time waiting for a connection from the pool, set via getConnection(). */
	public long connAcquireWaitTime;
	/** The maximum time waiting for a connection from the pool, set via getConnection(). */
	public long connAcquireMaxWaitTime;
	/** The minimum time waiting for a connection from the pool, set via getConnection(). */
	public long connAcquireMinWaitTime = Long.MAX_VALUE;
	/** The total time waiting for a connection from the pool, set via getConnection(). */
	public long connLeaseTime;
	/** The maximum time waiting for a connection from the pool, set via getConnection(). */
	public long connLeaseMaxTime;
	/** The minimum time waiting for a connection from the pool, set via getConnection(). */
	public long connLeaseMinTime = Long.MAX_VALUE;
	/** Time connection was leased. */
	protected long connLeaseStart;
	
	public DbConnTimed(DbPool pool) {
		super(pool);
	}
	
	/** Acquires a connection from the pool, but only when conn is null. */
	public Connection getConnection() throws SQLException {
		
		if (conn == null) {
			final long tstart = System.currentTimeMillis();
			conn = pool.acquire();
			connLeaseStart = System.currentTimeMillis();
			final long waitTime = (connLeaseStart - tstart);
			if (waitTime < connAcquireMinWaitTime) connAcquireMinWaitTime = waitTime;
			if (waitTime > connAcquireMaxWaitTime) connAcquireMaxWaitTime = waitTime;
			connAcquireWaitTime += waitTime;
			connAcquireCount++;
		}
		return conn;
	}
	
	public void close() {
		
		final long leaseTime = (System.currentTimeMillis() - connLeaseStart);
		if (leaseTime < connLeaseMinTime) connLeaseMinTime = leaseTime;
		if (leaseTime > connLeaseMaxTime) connLeaseMaxTime = leaseTime;
		connLeaseTime += leaseTime;
		super.close();
	}

	
	/** Returns a description of this class with the acquire&lease times. */
	public String getStats() {
		return (connAcquireCount > 0 ? "total leased: "+ connAcquireCount 
				+ ", acquire time avg/min/max: " 
				+ (connAcquireWaitTime / connAcquireCount) + " / " + connAcquireMinWaitTime	+ " / " + connAcquireMaxWaitTime 
				+ ", lease time avg/min/max: "
				+ (connLeaseTime / connAcquireCount) + " / " + connLeaseMinTime	+ " / " + connLeaseMaxTime 
				: "no connections acquired");
	}
}
