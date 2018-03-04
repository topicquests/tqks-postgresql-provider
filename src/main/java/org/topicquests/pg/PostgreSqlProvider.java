/*
 * Copyright 2018, TopicQuests
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.topicquests.pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.topicquests.pg.api.IPostgreSqlProvider;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.RootEnvironment;
import org.topicquests.support.api.IResult;

// import org.postgresql.ds.PGPoolingDataSource;
import org.apache.commons.dbcp2.*;

public class PostgreSqlProvider extends RootEnvironment 
    implements IPostgreSqlProvider {
  private final String urx;
  private final Properties props;

  /**
   * Pools Connections for each local thread
   * Must be closed when the thread terminates
   */
  // private ThreadLocal<Connection> localMapConnection = new ThreadLocal<Connection>();
  private Connection conn = null;
  // private PGPoolingDataSource source = null;
  private BasicDataSource connectionPool = null;

  public PostgreSqlProvider(String dbName, String dbSchema) {
    super("postgress-props.xml", "logger.properties");
    
    String dbUrl = getStringProperty("DatabaseURL");
    String dbPort = getStringProperty("DatabasePort");
    String dbUser = getStringProperty("DbUser");
    String dbPwd = getStringProperty("DbPwd");
    int    clientCacheSize = Integer.parseInt(getStringProperty("ClientCacheSize"));
    
    urx = "jdbc:postgresql://" + dbUrl + /*":"+_dbPort+*/ "/" + dbName;
    
    props = new Properties();
    props.setProperty("user", dbUser);

    // must allow for no password
    if (dbPwd != null && !dbPwd.equals(""))
      props.setProperty("password",dbPwd);

    connectionPool = new BasicDataSource();
    connectionPool.setUsername(dbUser);
    connectionPool.setPassword(dbPwd);
    connectionPool.setDriverClassName("org.postgresql.Driver");
    connectionPool.setUrl(urx);
    connectionPool.setInitialSize(1);
    connectionPool.setMaxTotal(10);
  }

  public String getUser() {
    return connectionPool.getUsername();
  }

  public void setUser(String user) {
    connectionPool.setUsername(user);
  }

  public void setPassword(String password) {
    connectionPool.setPassword(password);
  }

  /////////////////
  // connection handling
  ////////////////

  public Connection getConnection() throws SQLException {
    conn = connectionPool.getConnection();
    return conn;
  }

  private Connection getMapConnection() throws SQLException {
    return getConnection();
  }

  private void closeLocalConnection() throws SQLException {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      conn = null;
    }
  }

  public void beginTransaction() throws SQLException {
    try {
      if (conn != null) {
        conn.setAutoCommit(false);
      }
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      throw e;
    }
  }

  public void beginTransaction(Connection con) throws SQLException {
    try {
      if (con != null) {
        con.setAutoCommit(false);
      }
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      throw e;
    }
  }

  public void endTransaction() throws SQLException {
    try {
      if (conn != null) {
        conn.commit();
      }
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      throw e;
    }
  }

  public void endTransaction(Connection con) throws SQLException {
    try {
      if (con != null) {
        con.commit();
      }
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      throw e;
    }
  }

  public Properties getProps() {
    return props;
  }

  private IResult errorResult(SQLException e) {
    IResult r = new ResultPojo();
    r.addErrorString(e.getMessage());
    return r;
  }
	
  /**
   * Must be called
   */
  @Override
  public void shutDown() throws SQLException {
    try {
      connectionPool.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      throw e;
    } finally {
      connectionPool = null;
    }
  }

  /////////////////////
  // Simple Statement queries
  /////////////////////
  @Override
  public IResult executeSQL(String sql) {
    Connection conn = null;
    
    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeSQL(conn, sql);
    closeConnection(conn, rset);
    return rset;
  }

  @Override
  public IResult executeMultiSQL(String[] stmts) {
    List<String> sqlList = Arrays.asList(stmts);
    return executeMultiSQL(sqlList);
  }
  
  @Override
  public IResult executeMultiSQL(List<String> sql) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult result = new ResultPojo();
    IResult r = null;

    Iterator<String> itr = sql.iterator();
    while (itr.hasNext()) {
      r = executeSQL(conn, itr.next());
      if (r.hasError())
        result.addErrorString(r.getErrorString());
    }

    closeConnection(conn, result);
    return result;
  }
	
  @Override
  public IResult executeSQL(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;

    try {
      s = conn.createStatement();
      s.execute(sql);
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;
  }
	
  @Override
  public IResult executeCount(String sql) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }
    
    IResult rset = executeCount(conn, sql);
    closeConnection(conn, rset);
    return rset;
  }

  @Override
  public IResult executeCount(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;
    ResultSet rs = null;

    try {
      s = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                               ResultSet.CONCUR_READ_ONLY);
      rs = s.executeQuery(sql);
      rs.last();
      System.out.println("count: " + rs.getRow());
      result.setResultObject(new Long(rs.getRow()));
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;		
  }
	
  @Override
  public IResult executeUpdate(String sql) {
    Connection conn = null;
    
    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeUpdate(conn, sql);
    closeConnection(conn, rset);
    return rset;
  }
	
  @Override
  public IResult executeUpdate(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;

    try {
      s = conn.createStatement();
      s.executeUpdate(sql);
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;
  }
	
  @Override
  public IResult executeSelect(String sql) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeSelect(conn, sql);
    closeConnection(conn, rset);
    return rset;
  }
	
  @Override
  public IResult executeSelect(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;

    try {
      s = conn.createStatement();
      ResultSet rs = s.executeQuery(sql);
      result.setResultObject(rs);
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    }
    return result;
  }
  
  /////////////////
  // PreferredStatement queries
  /////////////////
	
  @Override
  public IResult executeSQL(String sql, String... vals) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeSQL(conn, sql, vals);
    closeConnection(conn, rset);
    return rset;
  }
	
  @Override
  public IResult executeSQL(Connection conn, String sql, String... vals) {
    IResult result = new ResultPojo();
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i=0;i<len;i++) {
        s.setString(i+1, vals[i]);
      }
      s.execute();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closePreparedStatement(s, result);
      }
    }

    return result;		
  }

  @Override
  public IResult executeUpdate(String sql, String... vals) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeUpdate(conn, sql, vals);
    closeConnection(conn, rset);
    return rset;
  }
	
  @Override
  public IResult executeUpdate(Connection conn, String sql, String... vals) {
    IResult result = new ResultPojo();
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i = 0; i < len; i++) {
        s.setString(i+1, vals[i]);
      }
      s.executeUpdate();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closePreparedStatement(s, result);
      }
    }

    return result;
  }

  @Override
  public IResult executeSelect(String sql, String... vals) {
    Connection conn = null;

    try {
      conn = getMapConnection();
    } catch (SQLException e) {
      return errorResult(e);
    }

    IResult rset = executeSelect(conn, sql, vals);
    closeConnection(conn, rset);
    return rset;
  }
	
  @Override
  public IResult executeSelect(Connection conn, String sql, String... vals) {
    IResult result = new ResultPojo();
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i=0;i<len;i++) {
        s.setString(i+1, vals[i]);
      }
      ResultSet rs = s.executeQuery();
      result.setResultObject(rs);
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    }
    
    return result;
  }
	
  //Relies on CREATE IF NOT EXISTS
  @Override
  public IResult validateDatabase(String [] tableSchema) {
    IResult result = new ResultPojo();
    Connection con = null;
    ResultSet rs = null;
    Statement s = null;
    // System.out.println("VALIDATE-1 ");
 
    try {
      con = getConnection();
      String[] sql = tableSchema;
      int len = sql.length;
      // System.out.println("VALIDATE-3a "+len);
      s = con.createStatement();

      for (int i = 0; i < len; i++) {
        logDebug(sql[i]);
        // System.out.println("EXPORTING "+sql[i]);
        s.execute(sql[i]);
      }
    } catch (SQLException x) {
      logError(x.getMessage(), x);
      x.printStackTrace();
      throw new RuntimeException(x);
    } finally {
      // System.out.println("VALIDATE-4");
      if (s != null) {
        try {
          s.close();
        } catch (SQLException a) {
          logError(a.getMessage(), a);
          a.printStackTrace();
          throw new RuntimeException(a);
        } 
      }
      // System.out.println("VALIDATE-5");
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException a) {
          logError(a.getMessage(), a);				
          throw new RuntimeException(a);
        } 
      }
      // System.out.println("VALIDATE-6");
      if (con != null) {
        try {
          con.close();
        } catch (SQLException a) {
          logError(a.getMessage(), a);				
          throw new RuntimeException(a);
        } 
      }
    }

    return result;
  }

  @Override
  public void closeResultSet(ResultSet rs, IResult r) {
    try {
      if (rs != null)
        rs.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeConnection(Connection conn, IResult r) {
    try {
      if (conn != null)
        conn.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeStatement(Statement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closePreparedStatement(PreparedStatement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }	
  }

}
