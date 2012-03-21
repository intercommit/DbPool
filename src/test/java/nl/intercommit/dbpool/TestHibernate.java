package nl.intercommit.dbpool;

import static nl.intercommit.dbpool.TestUtil.clearDbInMem;
import static nl.intercommit.dbpool.TestUtil.createTable;
import static org.junit.Assert.assertEquals;

//import java.sql.Connection;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHibernate {

	protected Logger log = LoggerFactory.getLogger(getClass());

	/** Use hibernate with a database pool to insert and update a record. 
	 * Tests the HibernateConnectionProvider. 
	 * <br>To see all Hibernate info, set log-level for the "org" category to INFO instead of WARN.
	 */
	@Test
	public void testHibernateDbInMem() {
		
		/* 
		 * Setup Hibernate configuration manually (usually done via hibernate.cfg.xml).
		 * Note that Hibernate automatic configuration via hibernate.cfg.xml is still very much possible,
		 * see the "HibernateTestCP" class. The only thing that must be specified in the hibernate.cfg.xml-file
		 * is the Hibernate property "hibernate.connection.provider_class".
		*/
		Configuration configuration = new Configuration();
		// Set the dbpool connection provider.
		configuration.setProperty(Environment.CONNECTION_PROVIDER, "nl.intercommit.dbpool.HibernateTestCP");
		configuration.addAnnotatedClass(T.class);
		SessionFactory sessionFactory = null;
		Session session = null;
		//Connection c = null;
		DbPool pool = null;
		try {
			sessionFactory = configuration.buildSessionFactory();
			
			// Cannot get a JDBC connection directly (deprecated):
			// clearDbInMem(sessionFactory.getCurrentSession().connection());
			// So instead lookup pool and use connection from there.
			
			pool = HibernateConnectionProvider.dbPoolsByUrl.get(HibernateTestCP.testDbUrl);
			
			// also an option for this test-class:
			// pool = HibernateTestCP.instance.dbPool;
			
			clearDbInMem(pool);
			
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
			assertEquals("Amount of connections acquired and released", HibernateTestCP.instance.fetchCount.get(), HibernateTestCP.instance.releaseCount.get());
		} catch (Exception se) {
			se.printStackTrace();
			throw new AssertionError(se);
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
