/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.plugins.mongo;

import java.util.Objects;

import com.dremio.common.store.StoragePluginConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Plugin Configuration for MongoDB
 */
@JsonTypeName(MongoStoragePluginConfig.NAME)
public class MongoStoragePluginConfig extends StoragePluginConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoStoragePluginConfig.class);

  public static final String NAME = "mongo";

  /**
   * Connection options not supported directly by mongo but
   * supported by Dremio
   *
   */
  public static final class ExtraConnectionOptions {
    /**
     * Set this connection property to true to allow for invalid hostname in SSL certificates
     *
     * This is not something recommended as it exposes user to man-in-the-middle attacks
     */
    public static final String SSL_INVALID_HOSTNAME_ALLOWED = "sslInvalidHostnameAllowed";

    /**
     * Set this connection property to true to allow connection to server with invalid certificates
     *
     * This is not something recommended as it exposes user to man-in-the-middle attacks
     */
    public static final String SSL_INVALID_CERTIFICATE_ALLOWED = "sslInvalidCertificateAllowed";
  }

  private final int authTimeoutMillis;
  private final String connection;
  private final boolean secondaryReadsOnly;
  private final int subpartitionSize;

  @JsonCreator
  public MongoStoragePluginConfig(@JsonProperty("authenticationTimeoutMillis") int authTimeoutMillis, // default 2s
                                  @JsonProperty("connection") String connection,
                                  @JsonProperty("secondaryReadsOnly") boolean secondaryReadsOnly,
                                  @JsonProperty("subpartitionSize") int subpartitionSize) {
    this.authTimeoutMillis = authTimeoutMillis;
    this.connection = connection;
    this.secondaryReadsOnly = secondaryReadsOnly;
    this.subpartitionSize = subpartitionSize;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoStoragePluginConfig that = (MongoStoragePluginConfig) o;
    return authTimeoutMillis == that.authTimeoutMillis &&
        secondaryReadsOnly == that.secondaryReadsOnly &&
        subpartitionSize == that.subpartitionSize &&
        Objects.equals(connection, that.connection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authTimeoutMillis, connection, secondaryReadsOnly, subpartitionSize);
  }

  public int getSubpartitionSize() {
    return subpartitionSize;
  }

  public boolean isSecondaryReadsOnly() {
    return secondaryReadsOnly;
  }


  public String getConnection() {
    return connection;
  }

  public int getAuthTimeoutMillis() {
    return authTimeoutMillis;
  }

}
