package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateTestCP extends HibernateConnectionProvider {

	public static String testDbUrl;
	public static HibernateTestCP instance;
	
	protected Logger log = LoggerFactory.getLogger(getClass());
	
	public AtomicLong fetchCount = new AtomicLong();
	public AtomicLong releaseCount = new AtomicLong();
	
	@Override
	public void configure(final Properties props) throws HibernateException {
		
		instance = this;
		log.info("Initializing DbPool for Hibernate Test.");
		dbPool = new DbPool();
		HSQLConnFactory fact = new HSQLConnFactory();
		fact.autoCommit = true;
		dbPool.setFactory(fact);
		testDbUrl = fact.dbUrl;
		props.setProperty(Environment.URL, testDbUrl);
		props.setProperty(Environment.AUTOCOMMIT, Boolean.toString(fact.autoCommit));
		// Disable cache so that all expected queries are executed.
		props.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "false");
		props.setProperty(Environment.SHOW_SQL, "true");
		try {
			dbPool.open(false);
			dbPoolsByUrl.put(testDbUrl, dbPool);
		} catch (SQLException sqle) {
			throw new HibernateException(sqle);
		}
		log.info("DbPool initialized for Hibernate Test.");
	}

	@Override
	public Connection getConnection() throws SQLException {
	
		if (log.isDebugEnabled()) log.debug("Acquiring a database connection for Hibernate ...");
		Connection c = dbPool.acquire();
		fetchCount.incrementAndGet();
		if (log.isDebugEnabled()) log.debug("Acquired a database connection for Hibernate: " + c);
		return c; 
	}

	@Override
	public void closeConnection(Connection c) throws SQLException { 
		
		if (log.isDebugEnabled()) log.debug("Releasing a database connection from Hibernate: " + c);
		dbPool.release(c); 
		releaseCount.incrementAndGet();
	}
	
	@Override
	public void close() throws HibernateException { 
		
		dbPool.close();
		log.info("Numer of connection acquires: " + fetchCount.get());
		log.info("Numer of connection releases: " + releaseCount.get());
	}

}
