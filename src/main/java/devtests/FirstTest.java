/**
 * 
 */
package devtests;

import java.sql.SQLException;

import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.support.api.IResult;

/**
 * @author jackpark
 *
 */
public class FirstTest {
	private IPostgresConnectionFactory provider;
	public final String
		VERTEX_TABLE	= "vertex",
		EDGE_TABLE		= "edge",
		DB_NAME			= "mynewdb";

	private final String [] tableSchema = {
          "DROP INDEX IF EXISTS vid",
          "DROP INDEX IF EXISTS eid",
          "DROP TABLE IF EXISTS vertex",
          "DROP TABLE IF EXISTS edge",
          
          "CREATE TABLE IF NOT EXISTS vertex ("
          + "id VARCHAR(64) PRIMARY KEY,"
          + "json JSON NOT NULL)", 

          "CREATE TABLE IF NOT EXISTS edge ("
          + "id VARCHAR(64) PRIMARY KEY,"
          + "json JSON NOT NULL)",

          "CREATE UNIQUE INDEX vid ON vertex(id)",
          "CREATE UNIQUE INDEX eid ON edge(id)"
	};
	
	/**
	 * 
	 */
	public FirstTest() {
                provider = new PostgresConnectionFactory(DB_NAME, "FirstTestSchema");
                
                try {
                  provider.shutDown();
                } catch (SQLException e) {
                  System.out.println(e.getMessage());
                }
		System.exit(0);
	}

}
