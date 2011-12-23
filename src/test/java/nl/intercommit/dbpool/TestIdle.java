package nl.intercommit.dbpool;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestIdle {

	// TODO: write a test that removes idle database connections while connections are being used.
	@Test
	public void testIdleSimple() {
		
		DbPool pool = new DbPool();
		pool.minSize = 3;
		pool.maxIdleTimeMs = 100L;
		pool.timeOutWatchIntervalMs = 50L;
		pool.setFactory(new HSQLConnFactory());
		DbConn db = null;
		try {
			pool.open(true);
			pool.minSize = 1;
			Thread.sleep(200L);
			assertEquals("Closed idle connections should be 2", 2, pool.getWatcher().idledCount);
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError("DbInMem test failed with: " + se);
		} finally {
			if (db != null) db.close();
			pool.close();
		}
	}
}
