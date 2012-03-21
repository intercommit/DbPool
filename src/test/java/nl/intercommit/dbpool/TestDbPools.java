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

import static nl.intercommit.dbpool.TestUtil.clearDbInMem;
import static nl.intercommit.dbpool.TestUtil.createTable;
import static nl.intercommit.dbpool.TestUtil.initDbInMem;
import static nl.intercommit.dbpool.TestUtil.insertRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDbPools {
	
	protected Logger log = LoggerFactory.getLogger(getClass());
	
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
		DbPoolWatcher poolWatcher = new DbPoolWatcher(pool);
		pool.setWatcher(poolWatcher);
		poolWatcher.maxLeaseTimeMs = 300L;
		poolWatcher.timeOutWatchIntervalMs = 10L;
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
			System.out.println(pool.getStatusInfo());
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

}
