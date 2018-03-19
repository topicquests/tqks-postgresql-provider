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

public class UniquePairTest {

  private void initAll() {
    System.out.println("in initAll");
    setupTQAdminUser();
    setupDBTables();
  }

  private void setupDBTables() {
    System.out.println("in setupDBTables");
    assertEquals("tq_admin", provider.getUser());

    final String [] tableSchema = {
      "DROP TABLE IF EXISTS vertex",
          
      "CREATE TABLE IF NOT EXISTS vertex ("
      + "id VARCHAR(64), "
      + "value INTEGER)"
    };

    executeStatements(tableSchema);
  }

  @Test
  @DisplayName("SQL tests")
  void TestAll() {
    initAll();
    InsertAndSelect();
    tearDownAll();
  }

  void InsertAndSelect() {
    System.out.println("in InsertAndSelect");
    final String
        VERTEX_TABLE	= "vertex",
        V_ID            = Long.toString(System.currentTimeMillis());

    assertEquals("tq_admin", provider.getUser());

    // Insert
    String pstmt = "insert into vertex (id, value) " +
        "select ?, ? " +
        "where not exists (select 1 from vertex where id=? and value=?)";

    Object [] vals = new Object [4];
    vals[0] = V_ID;
    vals[1] = new Integer(40);
    vals[2] = V_ID;
    vals[3] = new Integer(40);

    IResult r = null;
    r = conn.beginTransaction();
    r = conn.executeSQL(pstmt, r, vals);
    r = conn.endTransaction(r);
    if (r.hasError()) {
      fail("First insert error: " + r.getErrorString());
    }

    IResult r2 = null;
    r2 = conn.beginTransaction();
    r2 = conn.executeSQL(pstmt, r2, vals);
    r2 = conn.endTransaction(r2);
    if (r2.hasError()) {
      fail("Second insert error: " + r2.getErrorString());
    }

    getRowCount();
  }

  void getRowCount() {
    System.out.println("in getRowCount");
    final String VERTEX_TABLE = "vertex";

    assertEquals("tq_admin", provider.getUser());

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
      assertEquals(1, count.longValue());
    } else {
      fail("count not found");
    }
  }
  
  private void tearDownAll() {
    System.out.println("in tearDownAll");

    String[] testDropDBs = {
      "DROP DATABASE testdb"
    };

    executeStatements(testDropDBs);
    closeConnection();
    shutDownProvider();
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
  
  private void setupTQAdminUser() {
    System.out.println("Setting up DB connection as tq_admin user...");
    provider = new PostgresConnectionFactory(TQ_ADMIN_DB, "",
                                             "tq_admin", "tq-admin-pwd");

    try {
      conn = provider.getConnection();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    System.out.println("DONE - Setting up DB connection as tq_admin user...");
  }

  private void shutDownProvider() {
    try {
      provider.shutDown();
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

}
