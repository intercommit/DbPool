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
*  along with DbPool.  If not, see <http://www.gnu.org/licenses/>.
*
*/
package nl.intercommit.dbpool;

import java.sql.Connection;
import java.sql.SQLException;

/** Interface for a database connection factory, required by DbPool. */
public interface DbConnFactory {

	/** Returns a new database connection. */ 
	Connection getConnection() throws SQLException;
	/** Validates a database connection. */
	void validate(Connection dbConn) throws SQLException;
	/** Closes a database connection. */
	void close(Connection dbConn);
	/** Closes a database connection, tries to perform a rollback if rollback is true. */
	void close(Connection dbConn, boolean rollback);
}
