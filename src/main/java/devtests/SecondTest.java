/**
 * 
 */
package devtests;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.api.IResult;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 * Uses the tables defined in {@link FirstTest}
 */
public class SecondTest {
	private IPostgresConnectionFactory provider;
        private IPostgresConnection conn = null;
	public final String
		VERTEX_TABLE	= "vertex",
		EDGE_TABLE		= "edge",
		DB_NAME			= "mynewdb",
		V_ID			= Long.toString(System.currentTimeMillis());

	/**
	 * 
	 */
	public SecondTest() {
          try {
                provider = new PostgresConnectionFactory(DB_NAME, "SecondTestSchema");
                conn = provider.getConnection();
          } catch (SQLException e) {
                System.out.println("SecondTest ERROR " + e.getMessage());
          }
                
		// Generate Some SQL
		JSONObject jo = new JSONObject();
		jo.put("Hello", "World");
		// Insert something
		String sql = "INSERT INTO "+VERTEX_TABLE+" values('"+V_ID+"', '"+jo.toJSONString()+"')";
		IResult r = conn.executeSQL(sql);
		System.out.println("AAA "+r.getErrorString());
		// Get it back
		sql = "SELECT json FROM "+VERTEX_TABLE+" where id='"+V_ID+"'";
		r = conn.executeSelect(sql);
		Object o = r.getResultObject();
		System.out.println("BBB "+r.getErrorString()+" | "+o);
		if (o != null) {
			ResultSet rs = (ResultSet)o;
			try {
				if (rs.next())
					System.out.println("CCC "+rs.getString("json"));
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
                try {
                  conn.closeConnection(r);
                  provider.shutDown();
                } catch (SQLException e) {
                  System.out.println(e.getMessage());
                }
		System.exit(0);

	}

}
