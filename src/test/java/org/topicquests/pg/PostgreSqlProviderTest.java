import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.topicquests.pg.PostgreSqlProvider;
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
  @BeforeAll
  @DisplayName("Initialize Test Database")
  static void initAll() {
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

    provider.shutDown();

    setupTestUser();
    setupDBTables();
  }

  private static void setupDBTables() {
    System.out.println("in setupDBTables");
    assertEquals("testuser", props.getProperty("user"));

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
  @DisplayName("Insert and Select")
  void InsertAndSelect() {
    System.out.println("in InsertAndSelect");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", props.getProperty("user"));

    // Generate Some SQL
    JSONObject jo = new JSONObject();
    jo.put("Hello", "World");

    // Insert
    String sql = "INSERT INTO " + VERTEX_TABLE +
        " values('" + V_ID + "', '" + jo.toJSONString() + "')";
    IResult r = provider.executeSQL(sql);
    
    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id='" + V_ID + "'";
    r = provider.executeSelect(sql);
    
    Object o = r.getResultObject();
    if (o != null) {
      ResultSet rs = (ResultSet)o;
      try {
        if (rs.next()) {
          assertEquals("{\"Hello\":\"World\"}", rs.getString("json"));
        }
        rs.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  @DisplayName("Insert and Select #2")
  void InsertAndSelect2() {
    System.out.println("in InsertAndSelect2");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", props.getProperty("user"));

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
      r = provider.executeSQL(sql, vals);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    // Select
    sql = "SELECT json FROM " + VERTEX_TABLE + " where id=?";
    try {
      r = provider.executeSelect(sql, V_ID);
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
        rs.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  @Test
  @DisplayName("Test Row Count")
  void getRowCount() {
    System.out.println("in getRowCount");
    final String
        VERTEX_TABLE	= "vertex",
        EDGE_TABLE      = "edge",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("testuser", props.getProperty("user"));

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
  
  @AfterAll
  @DisplayName("Tear Down Test Database")
  static void tearDownAll() {
    System.out.println("in tearDownAll");
    // Drop the testuser provider
    provider.shutDown();

    // Create a new provider to drop the test databases.
    setupTemplate();
    
    Statement s = null;
    String[] testDropDBs = {
      "DROP DATABASE testdb"
    };

    executeStatements(testDropDBs);
    provider.shutDown();

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
    provider.shutDown();
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
        try {
          s.close();
        }
        catch (Exception e) {
          fail(e.getMessage());
        }
      }
    }
  }

  private static void setupRoot() {
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }

    provider = new PostgreSqlProvider(ROOT_DB);
    props = provider.getProps();
    props.setProperty("user", "postgres");
    props.setProperty("password", "postgres");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void setupTestUser() {
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }

    provider = new PostgreSqlProvider(TEST_DB);
    props = provider.getProps();
    props.setProperty("user", "testuser");
    props.setProperty("password", "testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void setupTemplate() {
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }

    provider = new PostgreSqlProvider(TEMPLATE_DB);
    props = provider.getProps();
    props.setProperty("user", "testuser");
    props.setProperty("password", "testpwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
