/**
 * 
 */
package org.topicquests.pg.api;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.topicquests.support.api.IResult;

/**
 * @author jackpark
 *
 */
public interface IPostgreSqlProvider {

	Connection getConnection() throws Exception;
	
	void closeResultSet(ResultSet rs, IResult r);
	
	void closeConnection(Connection conn, IResult r);
	
	void closeStatement(Statement s, IResult r);
	
	void closePreparedStatement(PreparedStatement s, IResult r);
	
	/**
	 * <p>This platform presumes that the PostgreSQL server is running and
	 * that a database of a certain <em>dbName</em> has been created by
	 * commandline or other means; that database must permit a user defined
	 * by the dbUser and dbPassword settings in postgress-props.xml</p>
	 * <p>This validation presumes a string array <code>tableSchema</code>, each
	 * entry of which is either a database, or indexes, or other constraints.</p>
	 * @param tableSchema
	 * @return
	 */
	IResult validateDatabase(String [] tableSchema);
	
	/**
	 * For use with a {@link Statement}
	 * @param sql
	 * @return does not not return a {@link ResultSet}
	 */
	IResult executeSQL(String sql);
	
	IResult executeSQL(Connection conn, String sql);
	/**
	 * Execute a list of SQL statements at a time
	 * @param sql
	 * @return
	 */
	IResult executeMultiSQL(List<String> sql);
	
	IResult executeUpdate(String sql);
	
	IResult executeUpdate(Connection conn, String sql);
	
	/**
	 * For use with select queries and {@link Statement}
	 * @param sql
	 * @return returns a {@link ResultSet} for further processing
	 */
	IResult executeSelect(String sql);
	
	IResult executeSelect(Connection conn, String sql);
	/**
	 * Will return a {@link Long} value
	 * @param sql
	 * @return
	 */
	IResult executeCount(String sql);
	
	/**
	 * For use with a (@link PreparedStatement}
	 * @param sql 
	 * @param vals
	 * @return does not return a {@link ResultSet}
	 */
	IResult executeSQL(String sql, String ...vals);
	
	IResult executeSQL(Connection conn, String sql, String... vals);
	
	IResult executeUpdate(String sql, String...vals);
	
	IResult executeUpdate(Connection conn, String sql, String... vals);
	
	/**
	 * For use with {@link PreparedStatement}
	 * @param sql
	 * @param vals
	 * @return returns a {@link ResultSet} for further processing
	 */
	IResult executeSelect(String sql, String...vals);
	
	IResult executeSelect(Connection conn, String sql, String... vals);
	
	void shutDown();
}
