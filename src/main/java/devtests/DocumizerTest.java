/**
 * 
 */
package devtests;

import java.sql.ResultSet;
import java.util.UUID;

import org.topicquests.pg.PostgreSqlProvider;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

/**
 * @author jackpark
 * This test presumes the following:<br/>
 * <ol><li>A postgreSQL db named "test_library" is created
 * and booted with asr_schema.sql</li>
 * <li>A postgreSQL db named 'test_working" is created and initialized
 * with the same schema</li></ol>
 * For this test, those databases were created without owners or roles or passwords
 */
public class DocumizerTest {
	private IPostgresConnectionFactory library;
	private IPostgresConnectionFactory working;
    //private IPostgresConnection li = null;

	private static final String
		LIB_DB_NAME 	= "test_library",
		WORK_DB_NAME	= "test_working",
		SCHEMA_NAME		= "tqos_asr",
		//fields correspond to the sentence database
		SENTENCE_ID_FLD	= "id",
		PARA_ID_FLD		= "paraid",
		DOC_ID_FLD		= "docid",
		SENTENCE_FLD	= "sentence";
	/**
	 * 
	 */
	public DocumizerTest() {
		library = new PostgresConnectionFactory(LIB_DB_NAME, SCHEMA_NAME);
		working = new PostgresConnectionFactory(WORK_DB_NAME, SCHEMA_NAME);
		System.out.println("A "+library);
		System.out.println("B "+working);
		runTest();
	}

	void runTest() {
		JSONObject firstGram = new JSONObject();
		String id = UUID.randomUUID().toString();
		String docId = UUID.randomUUID().toString();
		String paraId = UUID.randomUUID().toString();
		String sentence = "hello world";
		firstGram.put(SENTENCE_ID_FLD, id);
		firstGram.put(PARA_ID_FLD, paraId);
		firstGram.put(DOC_ID_FLD, docId);
		firstGram.put(SENTENCE_FLD, sentence);
		try {
			//put this gram in library
			IResult r = post(id, firstGram, library.getConnection());
			System.out.println("R1 "+r.getErrorString());
			// fetch it back as if from another client
			r = get(id, library.getConnection());
			System.out.println("R2 "+r.getErrorString());
			JSONObject jo = (JSONObject)r.getResultObject();
			System.out.println("R3 "+jo);
//R3 {"sentence":"hello world","parid":"fe3d601b-82de-4c5c-8520-9005ca00ccd3","docid":"609cffab-7d28-4b0a-a022-8c898b061d2c","id":"3c7d5999-a078-44d9-a969-8acc37adc424"}

			if (jo != null) {
				//put it in working
				r = post(id, jo, working.getConnection());
				System.out.println("R4 "+r.getErrorString());
				//now fetch it from there
				r = get(id, working.getConnection());
				System.out.println("R5 "+r.getErrorString());
				jo = (JSONObject)r.getResultObject();
				System.out.println("R6 "+jo);
//R6 {"sentence":"hello world","parid":null,"docid":"609cffab-7d28-4b0a-a022-8c898b061d2c","id":"3c7d5999-a078-44d9-a969-8acc37adc424"}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	IResult post(String id, JSONObject data, IPostgresConnection conn) {
		IResult result = new ResultPojo();
		String sql = "INSERT INTO tqos_asr.sentences VALUES(?, ?, ?, ?)";
	    IResult r = null;
	    try {
			conn.setProxyRole(r);
			r = conn.beginTransaction();
			Object [] vals = new Object[4];
			vals[0] = data.getAsString(SENTENCE_ID_FLD);
			vals[1] = data.getAsString(DOC_ID_FLD);
			vals[2] = data.getAsString(PARA_ID_FLD);
			vals[3] = data.getAsString(SENTENCE_FLD);
			conn.executeSQL(sql, r, vals);
			if (r.hasError())
				result.addErrorString(r.getErrorString());
		} catch (Exception e) {
			result.addErrorString(e.getMessage());
			e.printStackTrace();
		}
	    conn.endTransaction(r);
	    conn.closeConnection(r);		
		if (r.hasError())
			result.addErrorString(r.getErrorString());
		return result;
	}
	
	IResult get(String id, IPostgresConnection conn) {
		IResult result = new ResultPojo();
		String sql = "SELECT row_to_json(sentences) FROM tqos_asr.sentences WHERE id = ?";
	    IResult r = new ResultPojo();
	    try {
			conn.setProxyRole(r);
			conn.executeSelect(sql, r, id);
			ResultSet rs = (ResultSet)r.getResultObject();
			if (rs != null && rs.next()) {
				String json = rs.getString(1);
				System.out.println("GET "+json);
//GET {"id":"3c7d5999-a078-44d9-a969-8acc37adc424","docid":"609cffab-7d28-4b0a-a022-8c898b061d2c","parid":null,"sentence":"hello world"}

				JSONParser p = new JSONParser(JSONParser.MODE_JSON_SIMPLE);

				JSONObject jo = (JSONObject)p.parse(json);
				result.setResultObject(jo);
			}
		} catch (Exception e) {
			result.addErrorString(e.getMessage());
			e.printStackTrace();
		}
	    conn.closeConnection(r);		
		if (r.hasError())
			result.addErrorString(r.getErrorString());		
		return result;
	}
}
