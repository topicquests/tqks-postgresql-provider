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
import java.sql.SQLException;

import java.util.Properties;

public interface IPostgresConnectionFactory {

  /**
   * Get the user name set in the connection pool.
   * @return The user name from the connection pool.
   */
  public String getUser();

  /**
   * Set the user name in the connection pool.
   * @param The user name to use in the connection pool.
   */
  public void setUser(String user);

  /**
   * Set the password for the user in the connection pool.
   * @param The password for the user in the connection pool.
   */
  public void setPassword(String password);

  /**
   * Get the properties used to set up the connection pool.
   * @return The properties from the connection pool.
   */
  public Properties getProps();

  /**
   * Get a connection from the connection pool.
   * @return A database connection from the connection pool.
   */
  public IPostgresConnection getConnection() throws SQLException;

  /**
   * Shut down the connection pool.
   * @throws A SQLException if the pool does not shut down properly.
   */
  public void shutDown() throws SQLException;
}
