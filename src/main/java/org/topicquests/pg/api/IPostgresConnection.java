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

import java.sql.*;
import java.util.List;

import org.topicquests.support.api.IResult;

public interface IPostgresConnection {

  /**
   * Begin a transaction for the connection.
   * @return IResult
   */
  public IResult beginTransaction();

  /**
   * Begin a transaction for the connection.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult beginTransaction(IResult result);

  /**
   * End a transaction for the connection.
   * @return IResult
   */
  public IResult endTransaction();

  /**
   * End a transaction for the connection.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult endTransaction(IResult result);

  /**
   * Set the proxy role.
   * @return IResult
   */
  public IResult setProxyRole();

  /**
   * Set the proxy role.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setProxyRole(IResult result);

  /**
   * Set the users role.
   * @return IResult
   */
  public IResult setUsersRole();

  /**
   * Set the users role.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setUsersRole(IResult result);

  /**
   * Set the role to access the conversation tree.
   * @return IResult
   */
  public IResult setConvRole();

  /**
   * Set the role to access the conversation tree.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setConvRole(IResult result);

  /**
   * Set the proxy read-only role.
   * @return IResult
   */
  public IResult setProxyRORole();

  /**
   * Set the proxy read-only role.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setProxyRORole(IResult result);

  /**
   * Set the users read-only role.
   * @return IResult
   */
  public IResult setUsersRORole();

  /**
   * Set the users read-only role.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setUsersRORole(IResult result);

  /**
   * Set the role to access the conversation tree in read-only mode.
   * @return IResult
   */
  public IResult setConvRORole();

  /**
   * Set the role to access the conversation tree in read-only mode.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult setConvRORole(IResult result);

  /**
   * Reset the role to the original current_user.
   * @return IResult
   */
  public IResult resetRole();

  /**
   * Reset the role to the original current_user.
   * @param result - an IResult object
   * @return an updated IResult object
   */
  public IResult resetRole(IResult result);

  /**
   * Set a savepoint.
   * @return IResult containing a java.sql.Savepoint object
   */
  public IResult setSavepoint();

  /**
   * Set a savepoint.
   * @param result - an IResult object
   * @return an updated IResult object containing a java.sql.Savepoint object
   */
  public IResult setSavepoint(IResult result);

  /**
   * Set a savepoint.
   * @param name The name of the savepoint.
   * @return an IResult object containing a java.sql.Savepoint object
   */
  public IResult setSavepoint(String name);

  /**
   * Set a savepoint.
   * @param name The name of the savepoint.
   * @param result an IResult object
   * @return an updated IResult object containing a java.sql.Savepoint object
   */
  public IResult setSavepoint(String name, IResult result);

  /**
   * Rollback a transaction. If a savepoint is set in a result object,
   * the transaction will be rolled back to the savepoint.
   * @return IResult
   */
  public IResult rollback();

  /**
   * Rollback a transaction. If a savepoint is set in a result object,
   * the transaction will be rolled back to the savepoint.
   * @param result Result object that may contain a java.sql.Savepoint object.
   * @return IResult
   */
  public IResult rollback(IResult result);

  /**
   * Execute the SQL string in the database.
   * @param sql The SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql);

  /**
   * Execute the SQL string in the database.
   * @param sql The SQL string to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql, IResult result);

  /**
   * Execute the array of SQL strings in the database.
   * @param stmts The SQL strings to be executed.
   * @return An IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(String[] stmts);

  /**
   * Execute the array of SQL strings in the database.
   * @param stmts The SQL strings to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(String[] stmts, IResult result);
  
  /**
   * Execute the list of SQL strings in the database.
   * @param sql The SQL strings to be executed.
   * @return An IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(List<String> sql);

  /**
   * Execute the list of SQL strings in the database.
   * @param sql The SQL strings to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the ResultSets and any error messages.
   */
  public IResult executeMultiSQL(List<String> sql, IResult result);
	
  /**
   * Execute the list of SQL strings in the database and return the number of rows.
   * @param sql The SQL string to be executed.
   * @return An IResult object containing the row count and any error messages.
   */
  public IResult executeCount(String sql);

  /**
   * Execute the list of SQL strings in the database and return the number of rows.
   * @param sql The SQL string to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the row count and any error messages.
   */
  public IResult executeCount(String sql, IResult result);

  /**
   * Execute the UPDATE SQL string in the database.
   * @param sql The UPDATE SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql);

  /**
   * Execute the UPDATE SQL string in the database.
   * @param sql The UPDATE SQL string to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql, IResult result);
	
  /**
   * Execute the SELECT SQL string in the database.
   * @param sql The SELECT SQL string to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql);

  /**
   * Execute the SELECT SQL string in the database.
   * @param sql The SELECT SQL string to be executed.
   * @param result an IResult object
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql, IResult result);
  
  /**
   * Execute the prepared statement SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql, Object... vals);

  /**
   * Execute the prepared statement SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param result an IResult object
   * @param vals The values to be injected into the prepared statement.
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSQL(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement UPDATE SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql, Object... vals);

  /**
   * Execute the prepared statement UPDATE SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param result an IResult object
   * @param vals The values to be injected into the prepared statement.
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeUpdate(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement for batch inserts/updates.
   * @param sql The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the number of rows updated and any error messages.
   */
  public IResult executeBatch(String sql, Object... vals);

  /**
   * Execute the prepared statement for batch inserts/updates.
   * @param sql The prepared statement to be executed.
   * @param result an IResult object
   * @param vals The values to be injected into the prepared statement.
   * @return An updated IResult object containing the number of rows updated and any error messages.
   */
  public IResult executeBatch(String sql, IResult result, Object... vals);

  /**
   * Execute the prepared statement SELECT SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql, Object... vals);

  /**
   * Execute the prepared statement SELECT SQL string in the database.
   * @param sql The prepared statement to be executed.
   * @param result an IResult object
   * @param vals The values to be injected into the prepared statement.
   * @return An updated IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql, IResult result, Object... vals);

  /**
   * Perform a validation of the database.
   * @param tableSchema An array of SQL statements to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult validateDatabase(String [] tableSchema);

  /**
   * Create a statement object and return a result object.
   * @return An IResult object containing the statement object.
   */
  public IResult createStatement();

  /**
   * Create a statement object and return a result object.
   * @param result an IResult object
   * @return An IResult object containing the statement object.
   */
  public IResult createStatement(IResult result);

  /**
   * Close a result set.
   * @param rs a java.sql.ResultSet object to close
   * @param result an IResult object
   */
  public void closeResultSet(ResultSet rs, IResult result);

  /**
   * Close this SQL connection.
   * @param result an IResult object to hold any errors
   */
  public void closeConnection(IResult result);

  /**
   * Close a SQL statement.
   * @param stmt the java.sql.Statement object to close
   * @param result an IResult object to hold any errors
   */
  public void closeStatement(Statement stmt, IResult result);

  /**
   * Close a prepared SQL statement.
   * @param stmt the java.sql.PreparedStatement object to close
   * @param result an IResult object to hold any errors
   */
  public void closeStatement(PreparedStatement stmt, IResult result);
}
