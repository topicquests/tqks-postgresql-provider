/**
 * 
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

/**
 * @author jackpark
 *
 */
public class PostgreSqlProvider extends RootEnvironment 
    implements IPostgreSqlProvider {
  private final String urx;
  private final Properties props;

  /**
   * Pools Connections for each local thread
   * Must be closed when the thread terminates
   */
  private ThreadLocal<Connection> localMapConnection = new ThreadLocal<Connection>();

  public PostgreSqlProvider(String dbName) {
    super("postgress-props.xml", "logger.properties");
    String dbUrl = getStringProperty("DatabaseURL");
    String dbPort = getStringProperty("DatabasePort");
    String dbUser = getStringProperty("DbUser");
    String dbPwd = getStringProperty("DbPwd");
    urx = "jdbc:postgresql://"+dbUrl+/*":"+_dbPort+*/"/"+dbName;
    props = new Properties();
    props.setProperty("user",dbUser);

    // must allow for no password
    if (dbPwd != null &&  !dbPwd.equals(""))
      props.setProperty("password",dbPwd);

  }

  public Connection getConnection() throws Exception {
    // System.out.println("GETCON");
    return DriverManager.getConnection(urx, props);
  }

  public Properties getProps() {
    return props;
  }
	
  /**
   * Must be called
   */
  @Override
  public void shutDown() {
    closeLocalConnection();
  }

  /////////////////////
  // Simple Statement queries
  /////////////////////
  @Override
  public IResult executeSQL(String sql) {
    Connection conn = null;
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeSQL(conn, sql);
  }
	
  @Override
  public IResult executeMultiSQL(List<String> sql) {
    IResult result = new ResultPojo();
    Connection conn = null;
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    Iterator<String> itr = sql.iterator();
    while (itr.hasNext()) {
      r = executeSQL(conn, itr.next());
      if (r.hasError())
        result.addErrorString(r.getErrorString());
    }
    return result;
  }
	
  @Override
  public IResult executeSQL(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;
    try {
      s = conn.createStatement();
      s.execute(sql);
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (Exception x) {
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
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeCount(conn, sql);
  }

  private IResult executeCount(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;
    ResultSet rs = null;
    try {
      s = conn.createStatement();
      rs = s.executeQuery(sql);
      if (rs.next()) {
        long x = rs.getLong("count");
        result.setResultObject(new Long(x));
      }
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (Exception x) {
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
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeUpdate(conn, sql);
  }
	
  @Override
  public IResult executeUpdate(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;
    try {
      s = conn.createStatement();
      s.executeUpdate(sql);
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (Exception x) {
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
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeSelect(conn, sql);
  }
	
  @Override
  public IResult executeSelect(Connection conn, String sql) {
    IResult result = new ResultPojo();
    Statement s = null;
    try {
      s = conn.createStatement();
      ResultSet rs = s.executeQuery(sql);
      result.setResultObject(rs);
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      /** cannot close s because it closes rs
          if (s != null) {
			 
          try {
          s.close();
          } catch (Exception x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
          }
          }
      */
    }
    return result;
  }
  /////////////////
  // PreferredStatement queries
  /////////////////
	
  @Override
  public IResult executeSQL(String sql, String... vals) {
    Connection conn = null;
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeSQL(conn, sql, vals);
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
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (Exception x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
        }
      }
    }
    return result;		
  }

  @Override
  public IResult executeUpdate(String sql, String... vals) {
    Connection conn = null;
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeUpdate(conn, sql, vals);
  }
	
  @Override
  public IResult executeUpdate(Connection conn, String sql, String... vals) {
    IResult result = new ResultPojo();
    PreparedStatement s = null;
    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i=0;i<len;i++) {
        s.setString(i+1, vals[i]);
      }
      s.executeUpdate();
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (Exception x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
        }
      }
    }
    return result;
  }

  @Override
  public IResult executeSelect(String sql, String... vals) {
    Connection conn = null;
    IResult r = getMapConnection();
    if (r.hasError())
      return r;
    conn = (Connection) r.getResultObject();
    return executeSelect(conn, sql, vals);
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
    } catch (Exception e) {
      logError(e.getMessage(), e);
      result.addErrorString(e.getMessage());
    } finally {
      /** You cannot close s because it closes rs
          if (s != null) {
          try {
          s.close();
          } catch (Exception x) {
          logError(x.getMessage(), x);
          result.addErrorString(x.getMessage());					
          }
          }
      */
    }
    return result;
  }
	
  /////////////////
  // connection handling
  ////////////////

  private IResult getMapConnection() {
    synchronized (localMapConnection) {
      IResult result = new ResultPojo();
      try {
        Connection con = this.localMapConnection.get();
        //because we don't "setInitialValue", this returns null if nothing for this thread
        if (con == null) {
          con = getConnection();
          // System.out.println("GETMAPCONNECTION " + con);
          localMapConnection.set(con);
        }
        result.setResultObject(con);
      } catch (Exception e) {
        result.addErrorString(e.getMessage());
        logError(e.getMessage(), e);
      }
      return result;
    }
  }

  IResult closeLocalConnection() {
    IResult result = new ResultPojo();
    boolean isError = false;
    try {
      synchronized (localMapConnection) {
        Connection con = this.localMapConnection.get();
        if (con != null)
          con.close();
        localMapConnection.remove();
        //  localMapConnection.set(null);
      }
    } catch (SQLException e) {
      isError = true;
      result.addErrorString(e.getMessage());
    }
    if (!isError)
      result.setResultObject("OK");
    return result;
  }

  //Relies on CREATE IF NOT EXISTS
  @Override
  public IResult validateDatabase(String [] tableSchema) {
    IResult result = new ResultPojo();
    Connection con = null;
    ResultSet rs = null;
    Statement s = null;
    System.out.println("VALIDATE-1 ");
 
      try {
    	con = getConnection();
        String[] sql = tableSchema;
        int len = sql.length;
        System.out.println("VALIDATE-3a "+len);
        s = con.createStatement();
        for (int i = 0; i < len; i++) {
          logDebug(sql[i]);
          System.out.println("EXPORTING "+sql[i]);
          s.execute(sql[i]);
        }
      } catch (Exception x) {
        logError(x.getMessage(), x);
        x.printStackTrace();
        throw new RuntimeException(x);
      } finally {
      System.out.println("VALIDATE-4");
      if (s != null) {
        try {
          s.close();
        } catch (Exception a) {
          logError(a.getMessage(), a);
          a.printStackTrace();
          throw new RuntimeException(a);
        } 
      }
      System.out.println("VALIDATE-5");
      if (rs != null) {
        try {
          rs.close();
        } catch (Exception a) {
          logError(a.getMessage(), a);				
          throw new RuntimeException(a);
        } 
      }
      System.out.println("VALIDATE-6");
      if (con != null) {
        try {
          con.close();
        } catch (Exception a) {
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
    } catch (Exception e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeConnection(Connection conn, IResult r) {
    try {
      if (conn != null)
        conn.close();
    } catch (Exception e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeStatement(Statement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (Exception e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closePreparedStatement(PreparedStatement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (Exception e) {
      logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }	
  }

}
