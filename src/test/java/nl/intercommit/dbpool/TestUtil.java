package nl.intercommit.dbpool;

import java.sql.SQLException;

public class TestUtil {

	public static String createTable = "create table t (id integer generated always as identity(start with 100) primary key, name varchar(256))";
	public static String deleteTable = "drop table t";
	public static String insertRecord = "insert into t (name) values (@name)";
	public static String selectRecord = "select id from t where name like @name";
	
	/** Deletes any created tables. */
	public static void clearDbInMem(DbPool pool) {
		
		DbConn db = new DbConn(pool);
		try {
			db.setQuery(deleteTable);
			db.ps.execute();
			db.conn.commit();
		} catch (Exception ignored) {
		} finally {
			db.close();
		}
	}

	/** Creates required tables. */
	public static void initDbInMem(DbPool pool) throws SQLException {
		
		clearDbInMem(pool);
		DbConn db = new DbConn(pool);
		try {
			db.setQuery(createTable);
			db.ps.execute();
			db.conn.commit();
		} finally {
			db.close();
		}
	}
	
	/** Returns a random string with given length containing a-z characters. */
	public static String str(int length) {
		StringBuilder sb = new StringBuilder("");
		for (int i = 0; i < length; i++) {
			int c = ((int)'a') + (int)(Math.random()*26); 
			sb.append(((char)c));
		} 
		return sb.toString();
	}
	
	/** Returns a random number as String with the given length. */
	public static String num(int length) {
		if (length == 0) return "";
		StringBuilder sb = new StringBuilder("1");
		for (int i = 1; i < length; i++) {
			int c = ((int)'0') + (int)(Math.random()*10); 
			sb.append(((char)c));
		} 
		return sb.toString();
	}
	
	public static long number(int length) {
		return Long.valueOf(num(length));
	}
}
