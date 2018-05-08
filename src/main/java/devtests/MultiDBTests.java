//
// To run this test, set up the test databases in Postgres:
//
// $ psql -a -f multidb.sql template1
//
// After the test finishes, drop the test databases in Postgres:
// $ psql -a -f clean_multidb.sql template1
//

package devtests;

import java.sql.*;

import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.pg.PostgresConnection;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.PostgresProviderException;
import org.topicquests.support.api.IResult;
import org.topicquests.support.ResultPojo;

import net.minidev.json.JSONObject;

public class MultiDBTests extends Thread {
  // Default no of threads to 10
  private static int NUM_OF_THREADS = 3;
  private static IPostgresConnectionFactory provider;

  int m_myId;

  static  int c_nextId = 0;
  static  IPostgresConnection s_conn = null;

  synchronized static int getNextId() {
    return c_nextId++;
  }

  public static void main (String args []) {
    try {
      // Create the threads
      Thread[] threadList = new Thread[NUM_OF_THREADS];

      // spawn threads
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i] = new MultiDBTests();
        threadList[i].start();
      }
    
      // Start everyone at the same time
      setGreenLight();

      // wait for all threads to end
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i].join();
      }

      // shut down the database provider
      provider.shutDown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }  

  public MultiDBTests() {
    super();
    // Assign an Id to the thread
    m_myId = getNextId();
  }

  public void run() {
    IPostgresConnection conn = null;
    ResultSet rs = null;

    try {
      IResult result = new ResultPojo();
      
      while (!getGreenLight())
        yield();

      //
      // Setup a connection factory for a particular database and user.
      //
      provider = setupTestUser();
      conn = provider.getConnection();

      // Run operations with a connection from the factory.
      insertRows(conn);
      countRows(conn);
      conn.closeConnection(result);

      //
      // Change the database and user of the current connection factory.
      //
      if (m_myId == 0) {
        provider = setupTestUser(1);
      } else {
        provider = setupTestUser(0);
      }

      // get connection from the new provider.
      conn = provider.getConnection();

      // Run operations with a connection from the updated factory.
      insertRows(conn);
      countRows(conn);
      conn.closeConnection(result);
      
      yield();
      System.out.println("Thread " + m_myId +  " is finished. ");
    } catch (Exception e) {
      System.out.println("Thread " + m_myId + " got Exception: " + e);
      e.printStackTrace();
      return;
    }
  }

  private IPostgresConnectionFactory setupTestUser() {
    return setupTestUser(m_myId);
  }
  
  private IPostgresConnectionFactory setupTestUser(int id) {
    String user = "usr" + id;
    String pwd  = user + "pwd";
    String db   = "testdb" + id;
    System.out.println("creating connection to " + db);
    return new PostgresConnectionFactory(db, "", user, pwd);
  }

  static boolean greenLight = false;
  static synchronized void setGreenLight () { greenLight = true; }
  synchronized boolean getGreenLight () { return greenLight; }

  private void insertRows(IPostgresConnection conn) {
    String tableName = "db" + m_myId + "_table";
    int numRows = 100000;
    Object[] vals = new Object[numRows * 2];

    for (int i = 0; i < numRows; i++) {
      int idx1 = 2*i;
      int idx2 = 2*i+1;
      vals[idx1] = new Integer(i);
      vals[idx2] = "data " + i;
    }

    // Batch Insert
    String sql = "INSERT INTO " + tableName + " values(?, ?)";
    IResult r = null;
    try {
      conn.beginTransaction();
      r = conn.executeBatch(sql, vals);
      conn.endTransaction();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    System.out.println("Thread " + m_myId +  ": inserted " + numRows + " rows.");
  }

  private void countRows(IPostgresConnection conn) {
    String tableName = "db" + m_myId + "_table";
    String sql = "SELECT COUNT(*) FROM " + tableName;
    IResult r = null;
    
    try {
      r = conn.executeSelect(sql);
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    Object o = r.getResultObject();

    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          System.out.println("Thread " + m_myId +  ": row count = " + rs.getInt(1));
        }
      } catch (Exception e) {
        System.err.println(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        System.err.println(r.getErrorString());
      }
    }
  }
}
