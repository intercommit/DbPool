package nl.intercommit.dbpool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestInterrupt {

	// TODO: write a test that interrupts threads while connections are being used.
	@Test
	public void testInterruptSimple() {
		
		DbPool pool = new DbPool();
		DbPoolWatcher poolWatcher = new DbPoolWatcher(pool);
		pool.setWatcher(poolWatcher);
		poolWatcher.maxLeaseTimeMs = 60L;
		poolWatcher.evictThreshold = 2;
		poolWatcher.timeOutWatchIntervalMs = 30L;
		pool.setFactory(new HSQLConnFactory());
		DbConn db = null;
		try {
			pool.open(true);
			pool.getWatcher().interrupt = true;
			db = new DbConn(pool);
			db.setQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS");
			Thread.sleep(200L);
			fail("Thread should have been interrupted.");
		} catch (InterruptedException e) {
			try { Thread.sleep(10L); } catch (InterruptedException ie2) {
				ie2.printStackTrace();
			}
			assertEquals("1 connection used in the pool", 1, pool.getCountUsedConnections());
		} catch (Exception e) {
			e.printStackTrace();
			throw new AssertionError(e);
		} finally {
			if (db != null) {
				db.close();
			} else {
				pool.close();
			}
		}
		assertEquals("Connection was marked dirty and not returned to the pool", 0, pool.getCountOpenConnections());
		if (db == null) throw new AssertionError("Could not open database connection.");
		pool.close();
	}
}
