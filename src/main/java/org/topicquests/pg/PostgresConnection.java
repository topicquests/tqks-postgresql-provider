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

import java.sql.*;
import java.util.*;

import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.RootEnvironment;
import org.topicquests.support.api.IResult;

import org.apache.commons.dbcp2.*;

public class PostgresConnection implements IPostgresConnection {
  private Connection conn = null;

  public PostgresConnection(Connection con) {
    conn = con;
  }

  @Override
  public IResult beginTransaction() {
    IResult result = new ResultPojo();
    return beginTransaction(result);
  }
  
  public IResult beginTransaction(IResult result) {
    try {
      if (conn != null)
        conn.setAutoCommit(false);
    } catch(SQLException e) {
      result.addErrorString(e.getMessage());
    }

    return result;
  }

  @Override
  public IResult endTransaction() {
    IResult result = new ResultPojo();
    return endTransaction(result);
  }

  @Override
  public IResult endTransaction(IResult result) {
    try {
      if (conn != null) {
        conn.commit();
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    }

    return result;
  }

  @Override
  public IResult setProxyRole() {
    IResult result = new ResultPojo();
    return setProxyRole(result);
  }

  @Override
  public IResult setProxyRole(IResult result) {
    String role_sql = "SET ROLE tq_proxy";
    return setRole(role_sql, result);
  }

  @Override
  public IResult setUsersRole() {
    IResult result = new ResultPojo();
    return setUsersRole(result);
  }

  @Override
  public IResult setUsersRole(IResult result) {
    String role_sql = "SET ROLE tq_users";
    return setRole(role_sql, result);
  }

  @Override
  public IResult setProxyRORole() {
    IResult result = new ResultPojo();
    return setProxyRORole(result);
  }

  @Override
  public IResult setProxyRORole(IResult result) {
    String role_sql = "SET ROLE tq_proxy_ro";
    return setRole(role_sql, result);
  }

  @Override
  public IResult setUsersRORole() {
    IResult result = new ResultPojo();
    return setUsersRORole(result);
  }

  @Override
  public IResult setUsersRORole(IResult result) {
    String role_sql = "SET ROLE tq_users_ro";
    return setRole(role_sql, result);
  }

  private IResult setRole(String role_sql, IResult result) {
    if (conn != null) {
      result = this.executeSQL(role_sql);
      if (result.hasError()) {
        return result;
      }
    }

    return result;
  }

  @Override
  public IResult setSavepoint() {
    IResult result = new ResultPojo();
    return setSavepoint(result);
  }

  @Override
  public IResult setSavepoint(IResult result) {
    try {
      if (conn != null) {
        Savepoint svpt = conn.setSavepoint();
        result.setResultObject(svpt);
      }
    } catch(SQLException e) {
      result.addErrorString(e.getMessage());
    }

    return result;
  }

  @Override
  public IResult setSavepoint(String name) {
    IResult result = new ResultPojo();
    return setSavepoint(name, result);
  }

  @Override
  public IResult setSavepoint(String name, IResult result) {
    try {
      if (conn != null) {
        Savepoint svpt = conn.setSavepoint(name);
        result.setResultObject(svpt);
      }
    } catch(SQLException e) {
      result.addErrorString(e.getMessage());
    }

    return result;
  }

  @Override
  public IResult rollback() {
    IResult result = new ResultPojo();
    return rollback(result);
  }

  @Override
  public IResult rollback(IResult result) {
    try {
      if (conn != null) {
        Object obj = result.getResultObject();

        if (obj == null) {  // no savepoint in result object
          conn.rollback();
        } else {
          Savepoint svpt = (Savepoint)obj;
          conn.rollback(svpt);
        }
      }
    } catch(SQLException e) {
      result.addErrorString(e.getMessage());
    }

    return result;
  }

  private IResult errorResult(SQLException e) {
    IResult r = new ResultPojo();
    r.addErrorString(e.getMessage());
    return r;
  }
	
  @Override
  public IResult executeSQL(String sql) {
    IResult result = new ResultPojo();
    return executeSQL(sql, result);
  }

  @Override
  public IResult executeSQL(String sql, IResult result) {
    Statement s = null;

    if (result.hasError())
      return result;

    try {
      s = conn.createStatement();
      s.execute(sql);
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;
  }

  @Override
  public IResult executeMultiSQL(String[] stmts) {
    List<String> sqlList = Arrays.asList(stmts);
    return executeMultiSQL(sqlList);
  }
  
  @Override
  public IResult executeMultiSQL(String[] stmts, IResult result) {
    List<String> sqlList = Arrays.asList(stmts);
    return executeMultiSQL(sqlList, result);
  }
  
  @Override
  public IResult executeMultiSQL(List<String> sql) {
    IResult result = new ResultPojo();
    return executeMultiSQL(sql, result);
  }
  
  @Override
  public IResult executeMultiSQL(List<String> sql, IResult result) {
    Iterator<String> itr = sql.iterator();
    while (itr.hasNext()) {
      result = executeSQL(itr.next(), result);
    }

    return result;
  }
	
  @Override
  public IResult executeCount(String sql) {
    IResult result = new ResultPojo();
    return executeCount(sql, result);
  }
  
  @Override
  public IResult executeCount(String sql, IResult result) {
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
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;		
  }
	
  @Override
  public IResult executeUpdate(String sql) {
    IResult result = new ResultPojo();
    return executeUpdate(sql, result);
  }
  
  @Override
  public IResult executeUpdate(String sql, IResult result) {
    Statement s = null;

    try {
      s = conn.createStatement();
      int rowcount = s.executeUpdate(sql);
      result.setResultObject(new Integer(rowcount));
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
          result.addErrorString(x.getMessage());					
        }
      }
    }

    return result;
  }
	
  @Override
  public IResult executeSelect(String sql) {
    IResult result = new ResultPojo();
    return executeSelect(sql, result);
  }
  
  @Override
  public IResult executeSelect(String sql, IResult result) {
    Statement s = null;

    try {
      s = conn.createStatement();
      ResultSet rs = s.executeQuery(sql);
      result.setResultObject(rs);
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    }
    return result;
  }
  
  @Override
  public IResult executeSQL(String sql, Object... vals) {
    IResult result = new ResultPojo();
    return executeSQL(sql, result, vals);
  }
  
  @Override
  public IResult executeSQL(String sql, IResult result, Object... vals) {
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i = 0; i < len; i++) {
        s.setObject(i+1, vals[i]);
      }
      s.execute();
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closeStatement(s, result);
      }
    }

    return result;		
  }

  @Override
  public IResult executeUpdate(String sql, Object... vals) {
    IResult result = new ResultPojo();
    return executeUpdate(sql, result, vals);
  }
  
  @Override
  public IResult executeUpdate(String sql, IResult result, Object... vals) {
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i = 0; i < len; i++) {
        s.setObject(i+1, vals[i]);
      }
      s.executeUpdate();
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closeStatement(s, result);
      }
    }

    return result;
  }

  @Override
  public IResult executeBatch(String sql, Object... vals) {
    IResult result = new ResultPojo();
    return executeBatch(sql, result, vals);
  }
  
  @Override
  public IResult executeBatch(String sql, IResult result, Object... vals) {
    PreparedStatement s = null;
    System.out.println("--- in executeBatch");

    try {
      s = conn.prepareStatement(sql);
      int paramCount = s.getParameterMetaData().getParameterCount();
      int len = vals.length;
      System.out.println("executeBatch - len: " + len + ", paramCount: " + paramCount);
      
      for (int i = 0; i < len;) {
        for (int j = 1; j <= paramCount; j++, i++) {
          System.out.println("executeBatch - setObject: (" + j + ", " + vals[i] + ")");
          s.setObject(j, vals[i]);
        }
        s.addBatch();
      }
      int[] inserted = s.executeBatch();
      result.setResultObject(new Integer(inserted.length));
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closeStatement(s, result);
      }
    }

    return result;
  }

  @Override
  public IResult executeSelect(String sql, Object... vals) {
    IResult result = new ResultPojo();
    return executeSelect(sql, result, vals);
  }
  
  @Override
  public IResult executeSelect(String sql, IResult result, Object... vals) {
    PreparedStatement s = null;

    try {
      s = conn.prepareStatement(sql);
      int len = vals.length;
      for (int i=0;i<len;i++) {
        s.setObject(i+1, vals[i]);
      }
      ResultSet rs = s.executeQuery();
      result.setResultObject(rs);
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    }
    
    return result;
  }
	
  @Override
  public IResult validateDatabase(String [] tableSchema) {
    return this.executeMultiSQL(tableSchema);
  }

  @Override
  public IResult createStatement() {
    IResult result = new ResultPojo();
    return createStatement(result);
  }

  @Override
  public IResult createStatement(IResult result) {
    try {
      if (conn != null) {
        Statement stmt = conn.createStatement();
        result.setResultObject(stmt);
      }
    } catch (SQLException e) {
      result.addErrorString(e.getMessage());
    }
    
    return result;
  }
  
  @Override
  public void closeResultSet(ResultSet rs, IResult r) {
    try {
      if (rs != null)
        rs.close();
    } catch (SQLException e) {
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeConnection(IResult r) {
    try {
      if (conn != null)
        conn.close();
    } catch (SQLException e) {
      r.addErrorString(e.getMessage());
    } finally {
      conn = null;
    }
  }

  @Override
  public void closeStatement(Statement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (SQLException e) {
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeStatement(PreparedStatement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (SQLException e) {
      r.addErrorString(e.getMessage());
    }	
  }

}
