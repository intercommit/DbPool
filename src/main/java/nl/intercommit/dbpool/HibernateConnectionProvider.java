package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.connection.ConnectionProvider;

/**
 * Wrapper when using Hibernate, see TestHibernate for usage.
 * Example copied from http://www.javalobby.org/java/forums/t18406.html
 * @author frederikw
 *
 */
public class HibernateConnectionProvider implements ConnectionProvider {

	/** Concurrent map containing the DbPools related to each {@link org.hibernate.cfg.Environment#URL} value. */
	public static Map<String, DbPool> dbPoolsByUrl = new  ConcurrentHashMap<String, DbPool>();
	
	protected DbPool dbPool;
	
	@Override
	public void configure(Properties props) throws HibernateException {
		
		dbPool = dbPoolsByUrl.get(props.get(Environment.URL));
		if(dbPool == null) {
			throw new HibernateException("Could not find a connection pool for URL " + props.get(Environment.URL));
		}
	}

	@Override
	public Connection getConnection() throws SQLException {	return dbPool.acquire(); }

	@Override
	public void closeConnection(final Connection c) throws SQLException { dbPool.release(c); }

	@Override
	public void close() throws HibernateException { dbPool.close(); }

	@Override
	public boolean supportsAggressiveRelease() { return false; }

}
