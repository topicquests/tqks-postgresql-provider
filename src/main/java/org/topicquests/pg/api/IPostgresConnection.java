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
  public IResult executeUpdate(String sql, String... vals);
  public IResult executeUpdate(String sql, IResult result, String... vals);

  /**
   * Execute the prepared statement SELECT SQL string in the database.
   * @param stmts The prepared statement to be executed.
   * @param vals The values to be injected into the prepared statement.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult executeSelect(String sql, String... vals);
  public IResult executeSelect(String sql, IResult Result, String... vals);

  /**
   * Perform a validation of the database.
   * @param tableSchema An array of SQL statements to be executed.
   * @return An IResult object containing the ResultSet and any error messages.
   */
  public IResult validateDatabase(String [] tableSchema);

  public Statement createStatement() throws SQLException;

  public void closeResultSet(ResultSet rs, IResult r);

  public void closeConnection(IResult r);

  public void closeStatement(Statement s, IResult r);

  public void closeStatement(PreparedStatement s, IResult r);
}
