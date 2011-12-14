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

public class MySQLConnFactory implements DbConnFactory {
	
	protected Logger log = Logger.getLogger(getClass());
	protected boolean initialized;

	public String dbDriverClass = "com.mysql.jdbc.Driver";
	public boolean autoCommit;
	public int validateTimeOutSeconds = 3;
	public int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
	public Properties mysqlProps;
	public String dbUrl = "jdbc:mysql://localhost:3306/test";

	public MySQLConnFactory() {
		super();
		mysqlProps = new Properties();
		mysqlProps.setProperty("user", "test");
		mysqlProps.setProperty("password", "test");
		// http://dev.mysql.com/doc/refman/5.1/en/connector-j-reference-configuration-properties.html
		// Prevent memory leaks
		mysqlProps.setProperty("dontTrackOpenResources", "true");
		// Prevent authorization errors when using functions/procedures 
		mysqlProps.setProperty("noAccessToProcedureBodies", "true");
		// Prevent waiting forever for a new connection, wait a max. of 30 seconds.
		mysqlProps.setProperty("connectionTimeout", "30000");
		// Prevent waiting forever for an answer to a query, wait a max. of 150 seconds.
		// Note: this is a fallback, use Statement.setQueryTimeout() for better query time-out.
		mysqlProps.setProperty("socketTimeout", "150000");
		// Omit unnecessary commit() and rollback() calls.
		mysqlProps.setProperty("useLocalSessionState", "true");
		// Omit unnecessary "set autocommit n" calls (needed for Hibernate).
		mysqlProps.setProperty("elideSetAutoCommits", "true");
		// Fetch database meta-data from modern place.
		mysqlProps.setProperty("useInformationSchema", "true");
		// Prevent date-errors when fetching dates (needed for Hibernate with MyISAM).
		mysqlProps.setProperty("useFastDateParsing", "false");
		// In case of failover, do not set the connection to read-only.
		mysqlProps.setProperty("failOverReadOnly", "false");
	}
	
	public synchronized void initialize() {

		if (initialized) return; 
		try {
			Class.forName(dbDriverClass).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to get MySQL driver class " + dbDriverClass, e);
		}
		initialized = true;
	}

	@Override
	public synchronized Connection getConnection() throws SQLException {

		initialize();
		Connection dbConn = null;
		boolean OK = false;
		try {
			dbConn = DriverManager.getConnection(dbUrl, mysqlProps);
			dbConn.setAutoCommit(autoCommit);
			dbConn.setTransactionIsolation(transactionIsolation);
			//System.out.println("MySQL connection class: " + dbConn.getClass().getName());
			OK = true;
		} finally {
			if (!OK) close(dbConn, false);
		}
		return dbConn;
	}
	
	@Override
	public void validate(final Connection dbConn) throws SQLException {
		//((com.mysql.jdbc.Connection)dbConn).ping();
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
			((com.mysql.jdbc.Connection)dbConn).setSocketTimeout(1000);
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
