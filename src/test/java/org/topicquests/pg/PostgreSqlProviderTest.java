import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.topicquests.pg.PostgreSqlProvider;

import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import net.minidev.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;

public class PostgreSqlProviderTest {

  private void initAll() {
    System.out.println("in initAll");
    String[] testCreation = {
      "CREATE ROLE testuser WITH LOGIN PASSWORD 'testpwd'",
      "CREATE DATABASE testdb ENCODING 'UTF-8' OWNER 'testuser'"
    };

    setupRoot();

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
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

    Connection con = null;
    try {
      con = provider.getConnection();
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Generate Some SQL
    JSONObject jo = new JSONObject();
    jo.put("Hello", "World");

    // Insert
    String sql = "INSERT INTO " + VERTEX_TABLE +
        " values('" + V_ID + "', '" + jo.toJSONString() + "')";
    provider.beginTransaction(con);
    IResult r = provider.executeSQL(con, sql);
    provider.endTransaction(con);
    
    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id='" + V_ID + "'";
    r = provider.executeSelect(con, sql);
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
        provider.closeConnection(con, r);
        e.printStackTrace();
        fail(e.getMessage());
      }

      provider.closeResultSet(rs, r);
      if (r.hasError()) {
        provider.closeConnection(con, r);
        fail(r.getErrorString());
      }
    }

    provider.closeConnection(con, r);
  }

  void InsertAndSelect2() {
    System.out.println("in InsertAndSelect2");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", provider.getUser());

    Connection con = null;
    try {
      con = provider.getConnection();
    } catch (SQLException e) {
      fail(e.getMessage());
    }

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
      provider.beginTransaction(con);
      r = provider.executeSQL(con, sql, vals);
      provider.endTransaction(con);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id=?";
    try {
      r = provider.executeSelect(con, sql, V_ID);
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
        provider.closeConnection(con, r);
        fail(e.getMessage());
      }

      provider.closeResultSet(rs, r);
      if (r.hasError()) {
        provider.closeConnection(con, r);
        fail(r.getErrorString());
      }
    }

    provider.closeConnection(con, r);
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
      provider.beginTransaction();
      r = provider.executeSQL(sql);
      provider.endTransaction();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select
    sql = "SELECT * FROM " + USERS_TABLE + " WHERE locator = ?";
    try {
      r = provider.executeSelect(sql, "locator");
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

      provider.closeResultSet(rs, r);
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
      provider.beginTransaction();
      r = provider.executeSQL(sql);
      provider.endTransaction();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
  
  void updateRow1() {
    System.out.println("in updateRow1");
    final String VERTEX_TABLE = "vertex";
    String V_ID = "";

    Connection con = null;
    try {
      con = provider.getConnection();
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Select row with the max id.
    String sql = "SELECT max(id) FROM " + VERTEX_TABLE;
    provider.beginTransaction(con);
    IResult r = provider.executeSelect(con, sql);
    provider.endTransaction(con);
    
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
      
      provider.closeResultSet(rs, r);
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
    try {
      provider.beginTransaction(con);
      r = provider.executeUpdate(con, sql, vals);
      provider.endTransaction(con);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select updated row
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id = ?";
    r = null;
    try {
      r = provider.executeSelect(con, sql, V_ID);
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
        provider.closeConnection(con, r);
        fail(e.getMessage());
      }
      
      provider.closeResultSet(rs, r);
      if (r.hasError()) {
        provider.closeConnection(con, r);
        fail(r.getErrorString());
      }
    }

    provider.closeConnection(con, r);
  }
  
  void updateRow2() {
    System.out.println("in updateRow2");
    final String VERTEX_TABLE = "vertex";
    String V_ID = "";
       
    Connection con = null;
    try {
      con = provider.getConnection();
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Select row with the max id.
    String sql = "SELECT max(id) FROM " + VERTEX_TABLE;
    IResult r = provider.executeSelect(con, sql);
    
    Object o = r.getResultObject();
    if (o != null) {
      ResultSet rs = (ResultSet)o;

      try {
        if (rs.next()) {
          V_ID = rs.getString(1);
        }
      } catch (Exception e) {
        provider.closeConnection(con, r);
        fail(e.getMessage());
      }

      provider.closeResultSet(rs, r);
      if (r.hasError()) {
        provider.closeConnection(con, r);
        fail(r.getErrorString());
      }
    }

    // Update the json value in the row containing the max id.
    JSONObject jo = new JSONObject();
    jo.put("Goodbye", "World");

    String [] vals = new String [2];
    vals[0] = jo.toJSONString();
    vals[1] = V_ID;
    
    sql = "UPDATE " + VERTEX_TABLE + " SET json = '" + jo.toJSONString() +
        "' WHERE id = '" + V_ID + "'";
    r = null;
    try {
      provider.beginTransaction(con);
      r = provider.executeUpdate(con, sql);
      provider.endTransaction(con);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select updated row
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id = ?";
    r = null;
    try {
      r = provider.executeSelect(con, sql, V_ID);
    } catch (Exception e) {
      provider.closeConnection(con, r);
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
        provider.closeConnection(con, r);
        fail(e.getMessage());
      }

      provider.closeResultSet(rs, r);
      if (r.hasError()) {
        provider.closeConnection(con, r);
        fail(r.getErrorString());
      }
    }

    provider.closeConnection(con, r);
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
      r = provider.executeCount(sql);
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
    
    Statement s = null;
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
  private static PostgreSqlProvider provider;
  private static Properties props;
  private static Connection conn;

  private static final String ROOT_DB = "postgres";
  private static final String TEST_DB = "testdb";
  private static final String TEMPLATE_DB = "template1";
  private static final String TQ_ADMIN_DB = "tq_database";

  static void executeStatements(String[] stmts) {
    Statement s = null;
    
    try {
      s = conn.createStatement();

      for (int i = 0; i < stmts.length; i++) {
        s.execute(stmts[i]);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      if (s != null) {
        IResult r = new ResultPojo();
        
        provider.closeStatement(s, r);
        if (r.hasError()) {
          fail(r.getErrorString());
        }
      }
    }
  }

  private void closeConnection() {
    if (conn != null) {
      IResult r = new ResultPojo();

      provider.closeConnection(conn, r);
      if (r.hasError()) {
        fail(r.getErrorString());
      }
      conn = null;
    }
  }
  
  private void setupRoot() {
    provider = new PostgreSqlProvider(ROOT_DB, "RootSchema");
    provider.setUser("postgres");
    provider.setPassword("postgres");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setupTestUser() {
    provider = new PostgreSqlProvider(TEST_DB, "UserSchema");
    provider.setUser("testuser");
    provider.setPassword("testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void setupTQAdminUser() {
    provider = new PostgreSqlProvider(TQ_ADMIN_DB, "AdminSchema");
    provider.setUser("tq_admin");
    provider.setPassword("tq-admin-pwd");

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

  private void setupTemplate() {
    provider = new PostgreSqlProvider(TEMPLATE_DB, "TemplateSchema");
    provider.setUser("testuser");
    provider.setPassword("testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
