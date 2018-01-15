/*
 * Copyright (C) 2017 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc;

import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Config for relational adapters.
 */
@JsonTypeName(JdbcStorageConfig.NAME)
public class JdbcStorageConfig extends StoragePluginConfig {

  public static final String NAME = "jdbc";

  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private final int fetchSize;
  private final String database;
  private final boolean showOnlyConnDatabase;

  @JsonCreator
  public JdbcStorageConfig(
      @JsonProperty("driver") String driver,
      @JsonProperty("url") String url,
      @JsonProperty("username") String username,
      @JsonProperty("password") String password,
      @JsonProperty(value = "fetchSize", defaultValue = "0") int fetchSize,
      @JsonProperty("database") String database,
      @JsonProperty(value = "showOnlyConnDatabase", defaultValue = "false") boolean showOnlyConnDatabase) {
    super();
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.database = database;
    this.showOnlyConnDatabase = showOnlyConnDatabase;
  }

  public String getDriver() {
    return driver;
  }

  public String getUrl() {
    return url;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public String getDatabase() {
    return database;
  }

  public boolean showOnlyConnDatabase() {
    return showOnlyConnDatabase;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((driver == null) ? 0 : driver.hashCode());
    result = prime * result + ((password == null) ? 0 : password.hashCode());
    result = prime * result + ((url == null) ? 0 : url.hashCode());
    result = prime * result + ((username == null) ? 0 : username.hashCode());
    result = prime * result + fetchSize;
    result = prime * result + ((database == null) ? 0 : database.hashCode());
    result = prime * result + ((showOnlyConnDatabase) ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    JdbcStorageConfig other = (JdbcStorageConfig) obj;
    if (driver == null) {
      if (other.driver != null) {
        return false;
      }
    } else if (!driver.equals(other.driver)) {
      return false;
    }
    if (password == null) {
      if (other.password != null) {
        return false;
      }
    } else if (!password.equals(other.password)) {
      return false;
    }
    if (url == null) {
      if (other.url != null) {
        return false;
      }
    } else if (!url.equals(other.url)) {
      return false;
    }
    if (username == null) {
      if (other.username != null) {
        return false;
      }
    } else if (!username.equals(other.username)) {
      return false;
    }
    if (fetchSize !=  other.fetchSize) {
      return false;
    }
    if (database == null) {
      if (other.database != null) {
        return false;
      }
    } else if (!database.equals(other.database)) {
      return false;
    }
    if (showOnlyConnDatabase != other.showOnlyConnDatabase) {
      return false;
    }
    return true;
  }


}
