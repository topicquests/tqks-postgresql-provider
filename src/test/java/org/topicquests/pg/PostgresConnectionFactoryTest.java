package org.topicquests.pg;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.topicquests.pg.PostgresConnection;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.pg.api.IPostgresConnectionFactory;

import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import net.minidev.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Properties;

public class PostgresConnectionFactoryTest {

  private void initAll() {
    System.out.println("in initAll");
    String[] testCreation = {
      "CREATE ROLE testuser WITH LOGIN PASSWORD 'testpwd'",
      "CREATE DATABASE testdb ENCODING 'UTF-8' OWNER 'testuser'"
    };

    setupRoot();

    executeStatements(testCreation);
    closeConnection();

    try {
      provider.shutDown();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

    setupTestUser();
    setupDBTables();
  }

  private void setupDBTables() {
    System.out.println("in setupDBTables");
    assertEquals("testuser", provider.getUser());

    final String [] tableSchema = {
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

    executeStatements(tableSchema);
  }

  @Test
  @DisplayName("SQL tests")
  void TestAll() {
    initAll();
    InsertAndSelect();
    InsertAndSelect2();
    updateRow1();
    updateRow2();
    getRowCount();
    tearDownAll();
  }

  //
  // Test the connection to the topic map proxy database.
  //
  @Test
  @DisplayName("TQ Proxy Connection test")
  void TestTQProxy() {
    setupTQAdminUser();
    InsertAndSelect3();
    DeleteUser1();
    closeConnection();
    try {
      provider.shutDown();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  void InsertAndSelect() {
    System.out.println("in InsertAndSelect");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", provider.getUser());

    // Generate Some SQL
    JSONObject jo = new JSONObject();
    jo.put("Hello", "World");

    // Insert
    String sql = "INSERT INTO " + VERTEX_TABLE +
        " values('" + V_ID + "', '" + jo.toJSONString() + "')";

    IResult r = null;
    r = conn.beginTransaction();
    r = conn.executeSQL(sql, r);
    r = conn.endTransaction(r);
    if (r.hasError()) {
      fail(r.getErrorString());
    }
    
    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id='" + V_ID + "'";
    r = conn.executeSelect(sql);
    if (r.hasError()) {
      fail("Error in SELECT: " + r.getErrorString());
    }
    
    Object o = r.getResultObject();
    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          assertEquals("{\"Hello\":\"World\"}", rs.getString("json"));
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }
  }

  void InsertAndSelect2() {
    System.out.println("in InsertAndSelect2");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", provider.getUser());

    // Generate Some SQL
    JSONObject jo = new JSONObject();
    jo.put("Hello", "World");

    String [] vals = new String [2];
    vals[0] = V_ID;
    vals[1] = jo.toJSONString();
    
    // Insert
    String sql = "INSERT INTO " + VERTEX_TABLE + " values(?, to_json(?::json))";
    IResult r = null;
    try {
      conn.beginTransaction();
      r = conn.executeSQL(sql, vals);
      conn.endTransaction();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id=?";
    try {
      r = conn.executeSelect(sql, V_ID);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    Object o = r.getResultObject();

    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          assertEquals("{\"Hello\":\"World\"}", rs.getString("json"));
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }
  }
  
  void InsertAndSelect3() {
    System.out.println("in InsertAndSelect3");
    final String
        USERS_TABLE	= "tq_authentication.users";

    assertEquals("tq_admin", provider.getUser());

    setUserRole();

    // Insert into users table
    String sql = "INSERT INTO " + USERS_TABLE +
        " values('locator', 'test@email.org', 'testpwd', 'handle', " +
        "'Joe', 'User', 'en', true)";
    IResult r = null;

    try {
      conn.beginTransaction();
      r = conn.executeSQL(sql);
      conn.endTransaction();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select
    sql = "SELECT * FROM " + USERS_TABLE + " WHERE locator = ?";
    try {
      r = conn.executeSelect(sql, "locator");
    } catch (Exception e) {
      fail(e.getMessage());
    }
    Object o = r.getResultObject();

    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          assertEquals("handle", rs.getString("handle"));
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }
  }
  
  void DeleteUser1() {
    System.out.println("in DeleteUser1");
    final String
        USERS_TABLE	= "tq_authentication.users";

    assertEquals("tq_admin", provider.getUser());

    setUserRole();

    // Insert into users table
    String sql = "DELETE FROM " + USERS_TABLE + " WHERE handle = 'handle'";

    IResult r = null;
    try {
      conn.beginTransaction();
      r = conn.executeSQL(sql);
      conn.endTransaction();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
  
  void updateRow1() {
    System.out.println("in updateRow1");
    final String VERTEX_TABLE = "vertex";
    String V_ID = "";

    // Select row with the max id.
    String sql = "SELECT max(id) FROM " + VERTEX_TABLE;
    IResult r = null;
    
    r = conn.beginTransaction();
    r = conn.executeSQL(sql, r);
    r = conn.endTransaction(r);
    if (r.hasError()) {
      fail(r.getErrorString());
    }
    
    Object o = r.getResultObject();
    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          V_ID = rs.getString(1);
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }
      
      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }

    // Update the json value in the row containing the max id.
    JSONObject jo = new JSONObject();
    jo.put("Goodbye", "World");

    String [] vals = new String [2];
    vals[0] = jo.toJSONString();
    vals[1] = V_ID;
    
    sql = "UPDATE " + VERTEX_TABLE + " SET json = to_json(?::json) WHERE id = ?";
    r = null;
    r = conn.beginTransaction();
    conn.executeSQL(sql, r, vals);
    conn.endTransaction(r);
    if (r.hasError()) {
      fail(r.getErrorString());
    }

    // Select updated row
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id = ?";
    r = null;
    try {
      r = conn.executeSelect(sql, V_ID);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    o = r.getResultObject();

    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          assertEquals("{\"Goodbye\":\"World\"}", rs.getString("json"));
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }
      
      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }
  }
  
  void updateRow2() {
    System.out.println("in updateRow2");
    final String VERTEX_TABLE = "vertex";
    String V_ID = "";
       
    // Select row with the max id.
    String sql = "SELECT max(id) FROM " + VERTEX_TABLE;
    IResult r = conn.executeSelect(sql);
    
    Object o = r.getResultObject();
    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          V_ID = rs.getString(1);
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }

    // Update the json value in the row containing the max id.
    JSONObject jo = new JSONObject();
    jo.put("Goodbye", "World");

    sql = "UPDATE " + VERTEX_TABLE + " SET json = '" + jo.toJSONString() +
        "' WHERE id = '" + V_ID + "'";
    r = conn.beginTransaction();
    conn.executeSQL(sql, r);
    conn.endTransaction(r);
    if (r.hasError()) {
      fail(r.getErrorString());
    }

    // Select updated row
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id = ?";
    r = null;
    try {
      r = conn.executeSelect(sql, V_ID);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    o = r.getResultObject();

    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          assertEquals("{\"Goodbye\":\"World\"}", rs.getString("json"));
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }

      conn.closeResultSet(rs, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
    }
  }
  
  void getRowCount() {
    System.out.println("in getRowCount");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", provider.getUser());

    // Select
    String sql = "SELECT * FROM " + VERTEX_TABLE;
    IResult r = null;
    try {
      r = conn.executeCount(sql);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    Object o = r.getResultObject();

    if (o != null) {
      Long count = (Long)o;
      assertEquals(2, count.longValue());
    } else {
      fail("count not found");
    }
  }
  
  private void tearDownAll() {
    System.out.println("in tearDownAll");
    // Drop the testuser provider
    closeConnection();
    try {
      provider.shutDown();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

    // Create a new provider to drop the test databases.
    setupTemplate();
    
    String[] testDropDBs = {
      "DROP DATABASE testdb"
    };

    executeStatements(testDropDBs);
    closeConnection();
    try {
      provider.shutDown();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

    String[] testDropUser = {
      "DROP ROLE testuser"
    };
    setupRoot();
    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    executeStatements(testDropUser);
    closeConnection();
    try {
      provider.shutDown();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  //
  // Helper attributes and methods.
  //
  private static IPostgresConnectionFactory provider;
  private static Properties props;
  private static IPostgresConnection conn;

  private static final String ROOT_DB = "postgres";
  private static final String TEST_DB = "testdb";
  private static final String TEMPLATE_DB = "template1";
  private static final String TQ_ADMIN_DB = "tq_database";

  static IResult executeStatements(String[] stmts) {
    IResult r = new ResultPojo();
    
    if (conn != null) {
      conn.executeMultiSQL(stmts, r);
    }

    return r;
  }

  private void closeConnection() {
    if (conn != null) {
      IResult r = new ResultPojo();

      conn.closeConnection(r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
      conn = null;
    }
  }
  
  private void setupRoot() {
    provider = new PostgresConnectionFactory(ROOT_DB, "",
                                             "postgres", "postgres");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setupTestUser() {
    provider = new PostgresConnectionFactory(TEST_DB, "",
                                             "testuser", "testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setupTQAdminUser() {
    provider = new PostgresConnectionFactory(TQ_ADMIN_DB, "",
                                             "tq_admin", "tq-admin-pwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setupTemplate() {
    provider = new PostgresConnectionFactory(TEMPLATE_DB, "",
                                             "testuser", "testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setUserRole() {
    String[] setRoleStmt = {
      "SET ROLE tq_admin"
    };

    executeStatements(setRoleStmt);
  }

  private void setProxyRole() {
    String[] setProxyStmt = {
      "SET ROLE tq_proxy"
    };

    executeStatements(setProxyStmt);
  }
}
