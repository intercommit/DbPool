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

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

public class RunMySQL {

	public static void main(String[] args) {
		new RunMySQL().testDbTasks();
	}

	protected Logger log = Logger.getLogger(getClass());

	public static String createTable = "CREATE TABLE IF NOT EXISTS `t` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT, name varchar(256), PRIMARY KEY (`id`))";

	DbTask[] tasks;
	DbPool pool;
	
	public void testDbTasks() {

		InputStream in = null;
		final Properties dbprops = new Properties();
		try {
			in = ClassLoader.getSystemResourceAsStream("db.properties");
			dbprops.load(in);
		} catch (Throwable ignored) {}
		finally { try { in.close(); } catch (Throwable ignored) {} }
		
		final MySQLConnFactory dbuser = new MySQLConnFactory();
		dbuser.log = Logger.getLogger(dbuser.getClass().getName() + ".TestDB");
		if (dbprops.getProperty("db.url") != null) dbuser.dbUrl = dbprops.getProperty("db.url").trim();
		
		Iterator<Object> propKeys = dbprops.keySet().iterator();
		while (propKeys.hasNext()) {
			String key = propKeys.next().toString();
			if (key.startsWith("db.connection.")) {
				dbuser.mysqlProps.put(key.substring("db.connection.".length()), dbprops.getProperty(key).trim());
			}
		}
		
		pool = new DbPool();
		pool.log = Logger.getLogger(getClass().getName() + ".TestDBPool");
		pool.maxSize = Integer.valueOf(dbprops.getProperty("db.pool.maxConnections", "3"));
		pool.setFactory(dbuser);
		pool.maxLeaseTimeMs = Long.valueOf(dbprops.getProperty("db.pool.maxLeaseTimeMs", "0"));
		pool.leaseTimeWatchIntervalMs = Long.valueOf(dbprops.getProperty("db.pool.leaseTimeWatchIintervalMs", "30000"));
		
		int taskCount = Integer.valueOf(dbprops.getProperty("maxClients", "4"));
		DbTask.queryTimeOutSeconds = Integer.valueOf(dbprops.getProperty("queryTimeOutSeconds", "0"));
		DbTask.numberOfInserts = Integer.valueOf(dbprops.getProperty("numberOfInserts", "1"));
		DbTask.querySearchKeySize = Integer.valueOf(dbprops.getProperty("querySearchKeySize", "3"));
		DbTask.numberOfSearches = Integer.valueOf(dbprops.getProperty("numberOfSearches", "1"));
		DbTask.maxSleep = Long.valueOf(dbprops.getProperty("maxSleepTimeMs", "100"));
		
		tasks = new DbTask[taskCount];
		DbConn db = null;
		try {
			pool.open(true);
			db = new DbConn(pool);
			try { 
				db.setNQuery(createTable);
				db.nps.execute(); 
				db.conn.commit(); 
				log.info("Created test table."); 
			} catch (SQLException se) { 
				log.info("Assuming test table already exists: " + se); 
			}
			db.close();
			for (int i = 0; i < taskCount; i++) {
				tasks[i] = new DbTask(pool);
				pool.execute(tasks[i], false);
			}
			Runtime.getRuntime().addShutdownHook(new ShutDownHook());
		} catch (SQLException se) {
			se.printStackTrace();
			pool.close();
		}
	}
	
	class ShutDownHook extends Thread {
		
		public void run() {
			log.info("Initiating shutdown");
			for (int i = 0; i < tasks.length; i++) tasks[i].stop();
			boolean tasksRunning = true;
			while (tasksRunning) {
				try { Thread.sleep(50L); } catch (InterruptedException ie) {
					log.debug("Still waiting for tasks to close.");
				}
				tasksRunning = false;
				for (int i = 0; i < tasks.length; i++) {
					if (tasks[i].isRunning()) {
						tasksRunning = true;
						break;
					}
				}
			}
			log.info("DBTasks finished, closing pool");
			pool.close();
			log.info("Shutdown complete");
		}
	}
}
