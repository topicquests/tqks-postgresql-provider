/**
 * 
 */
package devtests;

import java.sql.ResultSet;

import org.topicquests.pg.PostgreSqlProvider;
import org.topicquests.support.api.IResult;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 * Uses the tables defined in {@link FirstTest}
 */
public class SecondTest {
	private PostgreSqlProvider provider;
	public final String
		VERTEX_TABLE	= "vertex",
		EDGE_TABLE		= "edge",
		DB_NAME			= "mynewdb",
		V_ID			= Long.toString(System.currentTimeMillis());

	/**
	 * 
	 */
	public SecondTest() {
                provider = new PostgreSqlProvider(DB_NAME, "SecondTestSchema");
		// Generate Some SQL
		JSONObject jo = new JSONObject();
		jo.put("Hello", "World");
		// Insert something
		String sql = "INSERT INTO "+VERTEX_TABLE+" values('"+V_ID+"', '"+jo.toJSONString()+"')";
		IResult r = provider.executeSQL(sql);
		System.out.println("AAA "+r.getErrorString());
		// Get it back
		sql = "SELECT json FROM "+VERTEX_TABLE+" where id='"+V_ID+"'";
		r = provider.executeSelect(sql);
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
		
		
		provider.shutDown();
		System.exit(0);

	}

}
