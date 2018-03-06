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

public class ThreadsTest2 extends Thread {
  // Default no of threads to 10
  private static int NUM_OF_THREADS = 10;
  private static IPostgresConnectionFactory provider;

  private static final String TEST_DB = "geodb";

  int m_myId;

  static  int c_nextId = 1;

  synchronized static int getNextId() {
    return c_nextId++;
  }

  public static void main (String args []) {
    try {
      provider = setupTestUser();
      
      // If NoOfThreads is specified, then read it
      if (args.length > 1) {
        System.out.println("Error: Invalid Syntax. ");
        System.out.println("java ThreadsTest2 [NoOfThreads]");
        System.exit(0);
      }

      // get the no of threads if given
      if (args.length > 0)
        NUM_OF_THREADS = Integer.parseInt (args[0]);
  
      // Create the threads
      Thread[] threadList = new Thread[NUM_OF_THREADS];

      // spawn threads
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i] = new ThreadsTest2();
        threadList[i].start();
      }
    
      // Start everyone at the same time
      setGreenLight();

      // wait for all threads to end
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        threadList[i].join();
      }

      cleanUp();

      // shut down the database provider
      provider.shutDown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void cleanUp() {
    IPostgresConnection conn = null;
    IResult   result  = null;
    Statement stmt = null;

    try {    
      conn = provider.getConnection();
      stmt = conn.createStatement();

      // Begin the transaction for this thread.
      result = conn.beginTransaction();

      String sqlstmt = "delete from city where id > 9999";
      conn.executeSQL(sqlstmt, result);

      // End the transaction.
      conn.endTransaction(result);
    } catch (Exception e) {
      System.out.println("Exception during cleanup: " + e);
      e.printStackTrace();
    }
  }

  public ThreadsTest2() {
    super();
    // Assign an Id to the thread
    m_myId = getNextId();
  }

  public void run() {
    IPostgresConnection conn = null;
    IResult   result  = null;
    Statement stmt = null;

    try {    
      conn = provider.getConnection();
      stmt = conn.createStatement();

      while (!getGreenLight())
        yield();

      Object [] vals = new Object[5];
      vals[0] = new Integer(m_myId + 10000);
      vals[1] = new String("city");
      vals[2] = new String("USA");
      vals[3] = new String("district");
      vals[4] = new Integer(0);

      String idstr = Integer.toString(m_myId);

      // Begin the transaction for this thread.
      result = conn.beginTransaction();
      checkError(result, "");
      
      // Execute the Query
      String sqlstmt = "insert into city values (?, ?, ?, ?, ?)";
      conn.executeSQL(sqlstmt, result, vals);
      checkError(result, sqlstmt);

      sqlstmt = "update city set name = ? where id = ?";
      vals = new Object[2];
      vals[0] = new String("city" + idstr);
      vals[1] = new Integer(m_myId + 10000);
      
      conn.executeSQL(sqlstmt, result, vals);
      checkError(result, sqlstmt);

      sqlstmt = "update city set population = ? where id = ?";
      vals = new Object[2];
      vals[0] = new Integer(m_myId + 10000);
      vals[1] = new Integer(m_myId + 10000);

      conn.executeSQL(sqlstmt, result, vals);
      checkError(result, sqlstmt);

      // End the transaction.
      conn.endTransaction(result);
      checkError(result, "");
  
      // Close the statement
      stmt.close();
      stmt = null;
  
      // Close the local connection
      if (conn != null) {
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

  private void checkError(IResult result, String sqlstmt) {
    if (result != null) {
      if (result.hasError()) {
        System.out.println("ERROR: " + result.getErrorString());
        System.out.println("SQL: " + sqlstmt);
        System.exit(1);
      }
    }
  }

  private static IPostgresConnectionFactory setupTestUser() {
    return new PostgresConnectionFactory(TEST_DB, "", "geo", "geopwd");
  }

  static boolean greenLight = false;
  static synchronized void setGreenLight () { greenLight = true; }
  synchronized boolean getGreenLight () { return greenLight; }
}
