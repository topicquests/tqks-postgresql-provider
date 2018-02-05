/**
 * 
 */
package devtests;

import org.topicquests.pg.PostgreSqlProvider;
import org.topicquests.support.api.IResult;

/**
 * @author jackpark
 *
 */
public class FirstTest {
	private PostgreSqlProvider provider;
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
		provider = new PostgreSqlProvider(DB_NAME);
		IResult r = provider.validateDatabase(tableSchema);
		System.out.println("AAA "+r.getErrorString());
		provider.shutDown();
		System.exit(0);
	}

}
