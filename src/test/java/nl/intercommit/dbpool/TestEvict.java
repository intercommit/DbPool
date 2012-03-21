package nl.intercommit.dbpool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

public class TestEvict {

	// TODO: write a test that evicts database connections while connections are being used.
	@Test
	public void testEvict() {
		doEvictTest(false, false);
		doEvictTest(true, false);
		doEvictTest(false, true);
	}
	
	public void doEvictTest(boolean evictedIsClosed, boolean evictedIsClosedWhenThreadHasTerminated) {
		
		DbPool pool = new DbPool();
		DbPoolWatcher poolWatcher = new DbPoolWatcher(pool);
		pool.setWatcher(poolWatcher);
		poolWatcher.maxLeaseTimeMs = 60L;
		poolWatcher.evictThreshold = 2;
		poolWatcher.timeOutWatchIntervalMs = 30L;
		poolWatcher.closeEvicted = evictedIsClosed;
		poolWatcher.closeEvictedOnlyWhenUserTerminated = evictedIsClosedWhenThreadHasTerminated;
		pool.setFactory(new HSQLConnFactory());
		DbConn db = null;
		try {
			pool.open(true);
			db = new DbConn(pool);
			db.setQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS");
			Thread.sleep(200L);
			assertEquals("After connection is evicted from pool, there should be no connections in the pool", 0, pool.getCountOpenConnections());
			if (evictedIsClosed) {
				assertTrue("Evicted connection should be closed.", db.conn.isClosed());
			} else if (evictedIsClosedWhenThreadHasTerminated || !evictedIsClosed) {
				assertFalse("Evicted connection should not be closed.", db.conn.isClosed());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new AssertionError(e);
		} finally {
			// db.close() calls pool.release(connection) which should close the connection.
			if (db != null) {
				Connection c = db.conn;
				db.close();
				try {
					assertTrue("An evicted connection should be closed when released.", c.isClosed());
				} catch (SQLException e) {
					e.printStackTrace();
					throw new AssertionError(e.toString());
				} finally {
					pool.close();
				}
			} else {
				pool.close();
			}
		}
	}
}
