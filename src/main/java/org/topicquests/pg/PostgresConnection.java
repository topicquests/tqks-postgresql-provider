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

  public void beginTransaction() throws SQLException {
    if (conn != null)
      conn.setAutoCommit(false);
  }

  public void endTransaction() throws SQLException {
    if (conn != null) {
      conn.commit();
      conn.setAutoCommit(true);
    }
  }

  private IResult errorResult(SQLException e) {
    IResult r = new ResultPojo();
    r.addErrorString(e.getMessage());
    return r;
  }
	
  @Override
  public IResult executeSQL(String sql) {
    IResult result = new ResultPojo();
    Statement s = null;

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
  public IResult executeMultiSQL(List<String> sql) {
    IResult result = new ResultPojo();
    IResult r = null;

    Iterator<String> itr = sql.iterator();
    while (itr.hasNext()) {
      r = executeSQL(itr.next());
      if (r.hasError())
        result.addErrorString(r.getErrorString());
    }

    return result;
  }
	
  @Override
  public IResult executeCount(String sql) {
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
    Statement s = null;

    try {
      s = conn.createStatement();
      s.executeUpdate(sql);
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
  public IResult executeSQL(String sql, String... vals) {
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
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closeStatement(s, result);
      }
    }

    return result;		
  }

  @Override
  public IResult executeUpdate(String sql, String... vals) {
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
      result.addErrorString(e.getMessage());
    } finally {
      if (s != null) {
        this.closeStatement(s, result);
      }
    }

    return result;
  }

  @Override
  public IResult executeSelect(String sql, String... vals) {
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
      result.addErrorString(e.getMessage());
    }
    
    return result;
  }
	
  @Override
  public IResult validateDatabase(String [] tableSchema) {
    return this.executeMultiSQL(tableSchema);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return conn.createStatement();
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
