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
package org.topicquests.pg.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.topicquests.support.api.IResult;

public interface IPostgresConnection {

  /**
   * Begin a transaction for the connection.
   * @return IResult
   */
  public IResult beginTransaction();
  public IResult beginTransaction(IResult result);

  /**
   * End a transaction for the connection.
   * @return IResult
   */
  public IResult endTransaction();
  public IResult endTransaction(IResult result);

  /**
   * Set the proxy role.
   * @return IResult
   */
  public IResult setProxyRole();
  public IResult setProxyRole(IResult result);

  /**
   * Set the users role.
   * @return IResult
   */
  public IResult setUsersRole();
  public IResult setUsersRole(IResult result);

  /**
   * Set the proxy read-only role.
   * @return IResult
   */
  public IResult setProxyRORole();
  public IResult setProxyRORole(IResult result);

  /**
   * Set the users read-only role.
   * @return IResult
   */
  public IResult setUsersRORole();
  public IResult setUsersRORole(IResult result);

  /**
   * Set a savepoint.
   * @return IResult
   */
  public IResult setSavepoint();
  public IResult setSavepoint(IResult result);

  /**
   * Set a savepoint.
   * @param name The name of the savepoint.
   * @return IResult
   */
  public IResult setSavepoint(String name);
  public IResult setSavepoint(String name, IResult result);

  /**
   * Rollback a transaction. If a savepoint is set in a result object,
   * the transaction will be rolled back to the savepoint.
   * @param name The name of the savepoint.
   * @param result Result object that may contain a savepoint object.
   * @return IResult
   */
  public IResult rollback();
  public IResult rollback(IResult result);

  /**
   * Execute the SQL string in the database.
   * @param sql The SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql);
  public IResult executeSQL(String sql, IResult result);

  /**
   * Execute the array of SQL strings in the database.
   * @param stmts The SQL strings to be executed.
   * @return An IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(String[] stmts);
  public IResult executeMultiSQL(String[] stmts, IResult result);
  
  /**
   * Execute the list of SQL strings in the database.
   * @param stmts The SQL strings to be executed.
   * @return An IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(List<String> sql);
  public IResult executeMultiSQL(List<String> sql, IResult result);
	
  /**
   * Execute the list of SQL strings in the database and return the number of rows.
   * @param stmts The SQL string to be executed.
   * @return An IResult object containing the row count and any error messages.
   */
  public IResult executeCount(String sql);
  public IResult executeCount(String sql, IResult result);

  /**
   * Execute the UPDATE SQL string in the database.
   * @param stmts The UPDATE SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql);
  public IResult executeUpdate(String sql, IResult result);
	
  /**
   * Execute the SELECT SQL string in the database.
   * @param stmts The SELECT SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql);
  public IResult executeSelect(String sql, IResult result);
  
  /**
   * Execute the prepared statement SQL string in the database.
   * @param stmts The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql, Object... vals);
  public IResult executeSQL(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement UPDATE SQL string in the database.
   * @param stmts The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql, Object... vals);
  public IResult executeUpdate(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement for batch inserts/updates.
   * @param stmts The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the number of rows updated and any error messages.
   */
  public IResult executeBatch(String sql, Object... vals);
  public IResult executeBatch(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement SELECT SQL string in the database.
   * @param stmts The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql, Object... vals);
  public IResult executeSelect(String sql, IResult Result, Object... vals);

  /**
   * Perform a validation of the database.
   * @param tableSchema An array of SQL statements to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult validateDatabase(String [] tableSchema);

  /**
   * Create a statement object and return a result object.
   * @param result Result containing a statement object.
   * @return An IResult object containing the statement object.
   */
  public IResult createStatement();
  public IResult createStatement(IResult result);

  public void closeResultSet(ResultSet rs, IResult r);

  public void closeConnection(IResult r);

  public void closeStatement(Statement s, IResult r);

  public void closeStatement(PreparedStatement s, IResult r);
}
