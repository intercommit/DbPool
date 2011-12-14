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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class HSQLConnFactory implements DbConnFactory {
	
	protected Logger log = Logger.getLogger(getClass());
	protected boolean initialized;

	public String dbDriverClass = "org.hsqldb.jdbc.JDBCDriver";
	public boolean autoCommit;
	public int validateTimeOutSeconds = 3;
	public int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
	public String dbUrl = "jdbc:hsqldb:mem:testdb";
	public Properties hsqlProps;

	public HSQLConnFactory() {
		super();
		hsqlProps = new Properties();
		hsqlProps.setProperty("user", "SA");
		hsqlProps.setProperty("password", "");
	}
	
	public synchronized void initialize() {

		if (initialized) return; 
		try {
			Class.forName(dbDriverClass).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to get HSQL driver class " + dbDriverClass, e);
		}
		initialized = true;
	}

	@Override
	public synchronized Connection getConnection() throws SQLException {

		initialize();
		Connection dbConn = null;
		boolean OK = false;
		try {
			dbConn = DriverManager.getConnection(dbUrl, hsqlProps);
			dbConn.setAutoCommit(autoCommit);
			dbConn.setTransactionIsolation(transactionIsolation);
			OK = true;
		} finally {
			if (!OK) close(dbConn, false);
		}
		return dbConn;
	}
	
	@Override
	public void validate(final Connection dbConn) throws SQLException {
		if (!dbConn.isValid(validateTimeOutSeconds)) {
			throw new SQLException("Database connection invalid or could not be validated within " + validateTimeOutSeconds + " seconds.");
		}
	}

	@Override
	public void close(final Connection dbConn) {
		close(dbConn, !autoCommit);
	}

	@Override
	public void close(final Connection dbConn, final boolean rollback) {

		if (dbConn == null) return;
		try {
			if (rollback) { 
				try { if (!dbConn.getAutoCommit()) dbConn.rollback(); }
				catch (SQLException se) {
					log.warn("Failed to call rollback on a database connection about to be closed: " + se);
				}
			}
			dbConn.close();
		} catch (SQLException sqle) {
			log.warn("Failed to properly close a database connection: " + sqle);
		}
	}

	@Override
	public String toString() {
		return dbUrl;
	}
}
