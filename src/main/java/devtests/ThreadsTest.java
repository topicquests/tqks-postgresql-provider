//
// To run this test, set up the geodb database in Postgres:
//
// $ psql -U postgres
// postgres=# create role geo with login password 'geopwd';
// CREATE ROLE
// postgres=# create database geodb encoding 'UTF-8' owner 'geo';
// CREATE DATABASE
// postgres=# \i world.sql
//

package devtests;

import java.sql.*;

import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.pg.PostgresConnection;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.support.api.IResult;
import org.topicquests.support.ResultPojo;

import net.minidev.json.JSONObject;

public class ThreadsTest extends Thread {
  // Default no of threads to 10
  private static int NUM_OF_THREADS = 10;
  private static IPostgresConnectionFactory provider;

  private static final String TEST_DB = "geodb";

  int m_myId;

  static  int c_nextId = 1;
  static  IPostgresConnection s_conn = null;
  static  boolean share_connection = false;

  synchronized static int getNextId() {
    return c_nextId++;
  }

  public static void main (String args []) {
    try {
      provider = setupTestUser();
      
      // If NoOfThreads is specified, then read it
      if ((args.length > 2)  || 
          ((args.length > 1) && !(args[1].equals("share")))) {
        System.out.println("Error: Invalid Syntax. ");
        System.out.println("java ThreadsTest [NoOfThreads] [share]");
        System.exit(0);
      }

      if (args.length > 1) {
        share_connection = true;
        System.out.println
            ("All threads will be sharing the same connection");
      }
  
      // get the no of threads if given
      if (args.length > 0)
        NUM_OF_THREADS = Integer.parseInt (args[0]);
  
      // get a shared connection
      if (share_connection)
        s_conn = provider.getConnection();
  
      // Create the threads
      Thread[] threadList = new Thread[NUM_OF_THREADS];

      // spawn threads
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i] = new ThreadsTest();
        threadList[i].start();
      }
    
      // Start everyone at the same time
      setGreenLight();

      // wait for all threads to end
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i].join();
      }

      if (share_connection) {
        IResult r = new ResultPojo();
        s_conn.closeConnection(r);
        s_conn = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }  

  public ThreadsTest() {
    super();
    // Assign an Id to the thread
    m_myId = getNextId();
  }

  public void run() {
    IPostgresConnection conn = null;
    ResultSet  rs   = null;
    Statement  stmt = null;

    try {    
      // Get the connection
      if (share_connection)
        stmt = s_conn.createStatement();
      else {
        conn = provider.getConnection();
        stmt = conn.createStatement();
      }

      while (!getGreenLight())
        yield();
          
      // Execute the Query
      String sqlstmt = "select * from city where id < " + Integer.toString(m_myId * 3);
      rs = stmt.executeQuery(sqlstmt);
          
      // Loop through the results
      while (rs.next()) {
        System.out.println("Thread " + m_myId + 
                           " City Id : " + rs.getInt(1) + 
                           " Name : " + rs.getString(2));
        yield();  // Yield To other threads
      }
          
      // Close all the resources
      rs.close();
      rs = null;
  
      // Close the statement
      stmt.close();
      stmt = null;
  
      // Close the local connection
      if ((!share_connection) && (conn != null)) {
        IResult r = new ResultPojo();
        conn.closeConnection(r);
        conn = null;
      }
      System.out.println("Thread " + m_myId +  " is finished. ");
    } catch (Exception e) {
      System.out.println("Thread " + m_myId + " got Exception: " + e);
      e.printStackTrace();
      return;
    }
  }

  private static IPostgresConnectionFactory setupTestUser() {
    return new PostgresConnectionFactory(TEST_DB, "", "geo", "geopwd");
  }

  static boolean greenLight = false;
  static synchronized void setGreenLight () { greenLight = true; }
  synchronized boolean getGreenLight () { return greenLight; }
}
