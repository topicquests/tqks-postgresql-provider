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

import java.io.InputStream;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.RootEnvironment;
import org.topicquests.support.api.IResult;

import org.apache.commons.dbcp2.*;

public class PostgresConnection implements IPostgresConnection {
	private PostgresConnectionFactory environment;
	private Connection conn = null;

  public PostgresConnection(Connection con, PostgresConnectionFactory env) {
    environment = env;
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
    	environment.logError(e.getMessage(), e);
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
    resetRole(result);
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
    resetRole(result);
    return setRole(role_sql, result);
  }

  @Override
  public IResult setConvRole() {
    IResult result = new ResultPojo();
    return setConvRole(result);
  }

  @Override
  public IResult setConvRole(IResult result) {
    String role_sql = "SET ROLE tq_conv";
    resetRole(result);
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
    resetRole(result);
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
    resetRole(result);
    return setRole(role_sql, result);
  }

  @Override
  public IResult setConvRORole() {
    IResult result = new ResultPojo();
    return setConvRORole(result);
  }

  @Override
  public IResult setConvRORole(IResult result) {
    String role_sql = "SET ROLE tq_conv_ro";
    resetRole(result);
    return setRole(role_sql, result);
  }

  @Override
  public IResult resetRole() {
    IResult result = new ResultPojo();
    return resetRole(result);
  }

  @Override
  public IResult resetRole(IResult result) {
    String role_sql = "RESET ROLE";
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
    	environment.logError(e.getMessage(), e);
     result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
        	environment.logError(x.getMessage(), x);
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
      result.setResultObject(new Long(rs.getRow()));
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
     result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
        	environment.logError(x.getMessage(), x);
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
    	environment.logError(e.getMessage(), e);
     result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (SQLException x) {
        	environment.logError(x.getMessage(), x);
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
    	environment.logError(e.getMessage(), e);
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
      setParamValues(s, vals);
      s.execute();
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
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
      setParamValues(s, vals);
      int rowcount = s.executeUpdate();
      result.setResultObject(new Integer(rowcount));

    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
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

    try {
      s = conn.prepareStatement(sql);
      int paramCount = s.getParameterMetaData().getParameterCount();
      int len = vals.length;
      
      for (int i = 0; i < len;) {
        for (int j = 1; j <= paramCount; j++, i++) {
          s.setObject(j, vals[i]);
        }
        s.addBatch();
      }
      
      int[] inserted = s.executeBatch();
      result.setResultObject(new Integer(inserted.length));
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
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
      setParamValues(s, vals);
      ResultSet rs = s.executeQuery();
      result.setResultObject(rs);
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
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
    	environment.logError(e.getMessage(), e);
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
    	environment.logError(e.getMessage(), e);
      r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeConnection(IResult r) {
    try {
      if (conn != null)
        conn.close();
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
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
    	environment.logError(e.getMessage(), e);
     r.addErrorString(e.getMessage());
    }
  }

  @Override
  public void closeStatement(PreparedStatement s, IResult r) {
    try {
      if (s != null)
        s.close();
    } catch (SQLException e) {
    	environment.logError(e.getMessage(), e);
     r.addErrorString(e.getMessage());
    }	
  }

  private void setParamValues(PreparedStatement s, Object... vals) throws SQLException {
    int len = vals.length;

    for (int i = 0; i < len; i++) {
      if (vals[i] == null) {
        s.setNull(i+1, java.sql.Types.OTHER);
      } else {
        if (vals[i] instanceof Byte) {
          s.setInt(i+1, ((Byte) vals[i]).intValue());
        } else if (vals[i] instanceof String) {
          s.setString(i+1, (String) vals[i]);
        } else if (vals[i] instanceof BigDecimal) {
          s.setBigDecimal(i+1, (BigDecimal) vals[i]);
        } else if (vals[i] instanceof Short) {
          s.setShort(i+1, ((Short) vals[i]).shortValue());
        } else if (vals[i] instanceof Integer) {
          s.setInt(i+1, ((Integer) vals[i]).intValue());
        } else if (vals[i] instanceof Long) {
          s.setLong(i+1, ((Long) vals[i]).longValue());
        } else if (vals[i] instanceof Float) {
          s.setFloat(i+1, ((Float) vals[i]).floatValue());
        } else if (vals[i] instanceof Double) {
          s.setDouble(i+1, ((Double) vals[i]).doubleValue());
        } else if (vals[i] instanceof byte[]) {
          s.setBytes(i+1, (byte[]) vals[i]);
        } else if (vals[i] instanceof java.sql.Date) {
          s.setDate(i+1, (java.sql.Date) vals[i]);
        } else if (vals[i] instanceof Time) {
          s.setTime(i+1, (Time) vals[i]);
        } else if (vals[i] instanceof Timestamp) {
          s.setTimestamp(i+1, (Timestamp) vals[i]);
        } else if (vals[i] instanceof Boolean) {
          s.setBoolean(i+1, ((Boolean) vals[i]).booleanValue());
        } else if (vals[i] instanceof InputStream) {
          s.setBinaryStream(i+1, (InputStream) vals[i], -1);
        } else if (vals[i] instanceof java.sql.Blob) {
          s.setBlob(i+1, (java.sql.Blob) vals[i]);
        } else if (vals[i] instanceof java.sql.Clob) {
          s.setClob(i+1, (java.sql.Clob) vals[i]);
        } else if (vals[i] instanceof java.util.Date) {
          s.setTimestamp(i+1, new Timestamp(((java.util.Date) vals[i]).getTime()));
        } else if (vals[i] instanceof BigInteger) {
          s.setString(i+1, vals[i].toString());
        } else {
          s.setObject(i+1, vals[i]);
        }
      }
    }
  }
}
