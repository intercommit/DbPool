package nl.intercommit.dbpool;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.junit.Test;

import static nl.intercommit.dbpool.TestUtil.*;

public class TestHibernate {

	protected Logger log = Logger.getLogger(getClass());

	/** Use hibernate with a database pool to insert and update a record. 
	 * Tests the HibernateConnectionProvider. */
	@Test
	public void testHibernateDbInMem() {
		
		DbPool pool = new DbPool();
		HSQLConnFactory fact = new HSQLConnFactory();
		fact.autoCommit = true;
		pool.setFactory(fact);
		// Register the dbpool.
		HibernateConnectionProvider.dbPoolsByUrl.put(fact.dbUrl, pool);
		// Setup Hibernate configuration (usually done via hibernate.cfg.xml)
		Configuration configuration = new Configuration();
		// Set the dbpool connection provider.
		configuration.setProperty(Environment.CONNECTION_PROVIDER, "nl.intercommit.dbpool.HibernateCP");
		configuration.setProperty(Environment.URL, fact.dbUrl);
		configuration.setProperty(Environment.AUTOCOMMIT, Boolean.toString(fact.autoCommit));
		// Disable cache so that all expected queries are executed.
		configuration.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "false");
		configuration.setProperty(Environment.SHOW_SQL, "true");
		configuration.addAnnotatedClass(T.class);
		SessionFactory sessionFactory = null;
		Session session = null;
		try {
			pool.open(true);
			clearDbInMem(pool);
			// To see all Hibernate info, set log-level for the "org" category to INFO instead of WARN.
			sessionFactory = configuration.buildSessionFactory();
			assertEquals("Database connections must not be used", 1, pool.getCountIdleConnections());
			session = sessionFactory.openSession();
			session.createSQLQuery(createTable).executeUpdate();
			T t = new T();
			t.setName("Frederik");
			session.persist(t);
			t.setName("Frederik Wiers");
			session.merge(t);
			// Without calling flush, nothing is done (and I don't know why, calling flush should not be mandatory).
			session.flush();
			session.close();
			long id = t.getId();
			session = sessionFactory.openSession();
			t = (T)session.load(T.class, id);
			assertEquals("Name must match last saved value", "Frederik Wiers", t.getName());
			session.close();
			assertEquals("Database connection must be released", 1, pool.getCountIdleConnections());
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError("Hibernate DbInMem test failed with: " + se);
		} finally {
			Exception error = null;
			if (session != null) try { session.close(); } catch (Exception e) {
				if (!"Session was already closed".equals(e.getMessage())) {
					e.printStackTrace();
					error = e;
				}
			}
			if (sessionFactory != null) try { sessionFactory.close(); } catch (Exception e) {
				e.printStackTrace();
				error = e;
			}
			if (error != null) 
				throw new AssertionError("Hibernate DbInMem test could not properly cleanup: " + error);
			//pool.close();
		}
	}
}
