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
import java.sql.SQLException;
import java.util.*;

import org.topicquests.pg.api.IPostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.pg.PostgresConnection;
import org.topicquests.support.RootEnvironment;

import org.apache.commons.dbcp2.*;

public class PostgresConnectionFactory extends RootEnvironment 
    implements IPostgresConnectionFactory {
  private String urx;
  private BasicDataSource connectionPool = null;

  /**
   * Create a connection factory to produce database connections to
   * a PostgreSQL database.
   * @param dbName The name of the database that will be used for all generated connections from this factory.
   */
  public PostgresConnectionFactory(String dbName) {
    this(dbName, null, null, null);
  }

  /**
   * Create a connection factory to produce database connections to a PostgreSQL database.
   * @param dbName The name of the database that will be used for all generated connections from this factory.
   * @param dbSchema The name of the schema contained in the specified database.
   */
  public PostgresConnectionFactory(String dbName, String dbSchema) {
    this(dbName, dbSchema, null, null);
  }

  /**
   * Create a connection factory to produce database connections to a PostgreSQL database.
   * @param dbName The name of the database that will be used for all generated connections from this factory.
   * @param user The name of the database user.
   * @param password The password for the database user.
   */
  public PostgresConnectionFactory(String dbName, String user, String password) {
    this(dbName, null, user, password);
  }

  /**
   * Create a connection factory to produce database connections to a PostgreSQL database.
   * @param dbName The name of the database that will be used for all generated connections from this factory.
   * @param dbSchema The name of the schema contained in the specified database.
   * @param user The name of the database user.
   * @param password The password for the database user.
   */
  public PostgresConnectionFactory(String dbName, String dbSchema,
                                   String user, String password) {
    super("postgress-props.xml");
    logDebug("PostgresConnectionFactory starting");
    String dbUrl = getStringProperty("DatabaseURL");
    int    dbPort = Integer.parseInt(getStringProperty("DatabasePort"));
    int    clientCacheSize = Integer.parseInt(getStringProperty("ClientCacheSize"));

    if (user == null) {
      user = getStringProperty("DbUser");
    }
    if (password == null) {
      password = getStringProperty("DbPwd");
    }
    
    connectionPool = new BasicDataSource();
    this.setUser(user);
    this.setPassword(password);
    this.setUrl(dbUrl, dbPort, dbName, dbSchema);
    connectionPool.setMaxOpenPreparedStatements(20);
    connectionPool.setDriverClassName("org.postgresql.Driver");
    connectionPool.setInitialSize(1);
    connectionPool.setMaxTotal(10);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getUser() {
    return connectionPool.getUsername();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUser(String user) {
    connectionPool.setUsername(user);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setPassword(String password) {
    connectionPool.setPassword(password);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getUrl() {
    return connectionPool.getUrl();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUrl(String url) {
    connectionPool.setUrl(url);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUrl(String hostName, int port, String dbName) {
    this.setUrl(hostName, port, dbName, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUrl(String hostName, int port, String dbName, String schema) {
    urx = "jdbc:postgresql://" + hostName + ":" + port + "/" + dbName;
    if ((schema != null) && (schema != "")) {
      urx += "?currentSchema=" + schema;
    }

    this.setUrl(urx);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public IPostgresConnection getConnection() throws SQLException {
    Connection con = connectionPool.getConnection();
    return new PostgresConnection(con, this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutDown() /*throws SQLException*/ {
    try {
      connectionPool.close();
    } catch (SQLException e) {
      logError(e.getMessage(), e);
      e.printStackTrace();
      //throw e;
    } finally {
      connectionPool = null;
    }
  }
}
