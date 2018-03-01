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
 *
 */
public class ThirdTest {
	private PostgreSqlProvider provider;
	public final String
		VERTEX_TABLE	= "vertex",
		EDGE_TABLE		= "edge",
		DB_NAME			= "mynewdb",
		V_ID			= Long.toString(System.currentTimeMillis());

	/**
	 * 
	 */
	public ThirdTest() {
                provider = new PostgreSqlProvider(DB_NAME, "ThirdTestSchema");
		// Generate Some SQL
		JSONObject jo = new JSONObject();
		jo.put("Hello", "World");
		String [] vals = new String [2];
		vals[0]=V_ID;
		vals[1]=jo.toJSONString();
		// Insert something
		String sql = "INSERT INTO "+VERTEX_TABLE+" values(?, ?)";
		IResult r = provider.executeSQL(sql, vals);
		System.out.println("AAA "+r.getErrorString());
		// Get it back
		sql = "SELECT json FROM "+VERTEX_TABLE+" where id=?";
		r = provider.executeSelect(sql, V_ID);
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
/**
AAA ; ERROR: column "json" is of type json but expression is of type character varying
  Hint: You will need to rewrite or cast the expression.
  Position: 31
BBB  | org.postgresql.jdbc.PgResultSet@224aed64
		
 */
		
		provider.shutDown();
		System.exit(0);
	}

}
