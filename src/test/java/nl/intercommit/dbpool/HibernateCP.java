package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class HibernateCP extends HibernateConnectionProvider {

	protected Logger log = Logger.getLogger(getClass());

	@Override
	public Connection getConnection() throws SQLException {
	
		if (log.isDebugEnabled()) log.debug("Acquiring a database connection for Hibernate ...");
		Connection c = dbPool.acquire();
		if (log.isDebugEnabled()) log.debug("Acquired a database connection for Hibernate: " + c);
		return c; 
	}

	@Override
	public void closeConnection(Connection c) throws SQLException { 
		
		if (log.isDebugEnabled()) log.debug("Releasing a database connection from Hibernate: " + c);
		dbPool.release(c); 
	}
}
