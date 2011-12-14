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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.Test;

public class TestDbPools {
	
	protected Logger log = Logger.getLogger(getClass());
	
	@Test
	public void testMysqlProps() {
		
		// Test that you can add/change settings during construction
		MySQLConnFactory cf = new MySQLConnFactory() {{
			mysqlProps.setProperty("user", "root");
			autoCommit = true;
		}};
		assertEquals("Property that was changed.", "root", cf.mysqlProps.get("user"));
		assertEquals("Property that was not changed.", "150000", cf.mysqlProps.get("socketTimeout"));
		assertEquals("Variable that was changed.", true, cf.autoCommit);
		assertEquals("Variable that was not changed.", Connection.TRANSACTION_READ_COMMITTED, cf.transactionIsolation);
	}
	
	public static String createTable = "create table t (id integer generated always as identity(start with 100) primary key, name varchar(256))";
	public static String deleteTable = "drop table t";
	public static String insertRecord = "insert into t (name) values (@name)";
	public static String selectRecord = "select id from t where name like @name";
	
	/** Deletes any created tables. */
	public static void clearDbInMem(DbPool pool) {
		
		DbConn db = new DbConn(pool);
		try {
			db.setQuery(deleteTable);
			db.ps.execute();
			db.conn.commit();
		} catch (Exception ignored) {
		} finally {
			db.close();
		}
	}

	/** Creates required tables. */
	public static void initDbInMem(DbPool pool) throws SQLException {
		
		clearDbInMem(pool);
		DbConn db = new DbConn(pool);
		try {
			db.setQuery(createTable);
			db.ps.execute();
			db.conn.commit();
		} finally {
			db.close();
		}
	}

	/** Use one connection to create a table and insert a record. */
	@Test
	public void testDbInMem() {
		
		DbPool pool = new DbPool();
		pool.setFactory(new HSQLConnFactory());
		DbConn db = null;
		try {
			pool.open(true);
			clearDbInMem(pool);
			db = new DbConn(pool);
			db.setQuery(createTable);
			assertFalse("Table creation.", db.ps.execute());
			db.ps.close();
			db.setNQuery(insertRecord, Statement.RETURN_GENERATED_KEYS);
			db.nps.setString("name", "Frederik");
			assertEquals("Insert 1 record.", 1, db.nps.executeUpdate());
			db.rs = db.nps.getStatement().getGeneratedKeys();
			assertTrue("Have a result.", db.rs.next());
			assertEquals("Generated id value.", 100, db.rs.getInt("id"));
			db.conn.commit();
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError("DbInMem test failed with: " + se);
		} finally {
			if (db != null) db.close();
			pool.close();
		}
	}

	/** Runs tasks that perform database actions.
	 * There are more tasks then database connections so tasks must wait for 
	 * a database connection to become available.
	 * At the end of the test time-statistics are shown for each task. These numbers
	 * should, on average, be about the same.
	 */
	@Test
	public void testDbTasks() {
		
		int taskCount = 12;
		long sleepTime = 3000;
		DbPool pool = new DbPool();
		pool.maxSize = 3;
		pool.setFactory(new HSQLConnFactory());
		pool.maxLeaseTimeMs = 300L;
		pool.leaseTimeWatchIntervalMs = 10L;
		DbTask.maxSleep = 100L;
		DbTask.numberOfInserts = 3;
		DbTask.numberOfSearches = 3;
		DbTask.querySearchKeySize = 3;
		//DbConn db = null;
		DbTask[] tasks = new DbTask[taskCount];
		try {
			pool.open(true);
			initDbInMem(pool);
			for (int i = 0; i < taskCount; i++) {
				tasks[i] = new DbTask(pool);
				pool.execute(tasks[i], true);
			}
			Thread.sleep(sleepTime);
			//pool.connFactory.close(pool.connections.keySet().iterator().next());
			//pool.flush();
			//Thread.sleep(1000);
			for (int i = 0; i < taskCount; i++) tasks[i].stop();
			boolean tasksRunning = true;
			while (tasksRunning) {
				Thread.sleep(50L);
				tasksRunning = false;
				for (int i = 0; i < taskCount; i++) {
					if (tasks[i].isRunning()) {
						tasksRunning = true;
						break;
					}
				}
			}
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError("DbTask test failed with " + se);
		} finally {
			//if (db != null) db.close();
			pool.close();
		}
	}

	/** Returns a random string with given length containing a-z characters. */
	public static String str(int length) {
		StringBuilder sb = new StringBuilder("");
		for (int i = 0; i < length; i++) {
			int c = ((int)'a') + (int)(Math.random()*26); 
			sb.append(((char)c));
		} 
		return sb.toString();
	}
	
	/** Returns a random number as String with the given length. */
	public static String num(int length) {
		if (length == 0) return "";
		StringBuilder sb = new StringBuilder("1");
		for (int i = 1; i < length; i++) {
			int c = ((int)'0') + (int)(Math.random()*10); 
			sb.append(((char)c));
		} 
		return sb.toString();
	}
	
	public static long number(int length) {
		return Long.valueOf(num(length));
	}

}
