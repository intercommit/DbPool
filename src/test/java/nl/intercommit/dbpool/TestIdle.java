package nl.intercommit.dbpool;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestIdle {

	// TODO: write a test that removes idle database connections while connections are being used.
	@Test
	public void testIdleSimple() {
		
		DbPool pool = new DbPool();
		pool.minSize = 3;
		DbPoolWatcher poolWatcher = new DbPoolWatcher(pool);
		pool.setWatcher(poolWatcher);
		poolWatcher.maxIdleTimeMs = 100L;
		poolWatcher.timeOutWatchIntervalMs = 50L;
		pool.setFactory(new HSQLConnFactory());
		DbConn db = null;
		try {
			pool.open(true);
			pool.minSize = 1;
			Thread.sleep(200L);
			assertEquals("Closed idle connections should be 2", 2, pool.getWatcher().idledCount);
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError(se);
		} finally {
			if (db != null) db.close();
			pool.close();
		}
	}
}
