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
  private final Properties props;

  private BasicDataSource connectionPool = null;

  public PostgresConnectionFactory(String dbName, String dbSchema) {
    this(dbName, dbSchema, null, null);
  }

  public PostgresConnectionFactory(String dbName, String dbSchema,
                                   String user, String password) {
    super("postgress-props.xml", "logger.properties");
    
    String dbUrl = getStringProperty("DatabaseURL");
    String dbPort = getStringProperty("DatabasePort");
    int    clientCacheSize = Integer.parseInt(getStringProperty("ClientCacheSize"));

    if (user == null) {
      user = getStringProperty("DbUser");
    }
    if (password == null) {
      password = getStringProperty("DbPwd");
    }
    
    urx = "jdbc:postgresql://" + dbUrl + /*":"+_dbPort+*/ "/" + dbName;
    if (dbSchema != "") {
      urx += "?currentSchema=" + dbSchema;
    }
    
    props = new Properties();
    props.setProperty("user", user);

    // must allow for no password
    if (password != null && !password.equals(""))
      props.setProperty("password", password);

    connectionPool = new BasicDataSource();
    connectionPool.setMaxOpenPreparedStatements(20);
    connectionPool.setUsername(user);
    connectionPool.setPassword(password);
    connectionPool.setDriverClassName("org.postgresql.Driver");
    connectionPool.setUrl(urx);
    connectionPool.setInitialSize(1);
    connectionPool.setMaxTotal(10);
  }

  @Override
  public String getUser() {
    return connectionPool.getUsername();
  }

  @Override
  public void setUser(String user) {
    connectionPool.setUsername(user);
  }

  @Override
  public void setPassword(String password) {
    connectionPool.setPassword(password);
  }

  @Override
  public Properties getProps() {
    return props;
  }

  @Override
  public IPostgresConnection getConnection() throws SQLException {
    Connection con = connectionPool.getConnection();
    return new PostgresConnection(con);
  }

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
}
