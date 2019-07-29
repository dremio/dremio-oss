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
package com.dremio.jdbc.impl;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import com.dremio.common.util.DremioVersionInfo;

/**
 * Avatica JDBC driver.
 */
public class DriverImpl extends UnregisteredDriver {
  private static final String CONNECTION_STRING_PREFIX = "jdbc:dremio:";
  private static final String METADATA_PROPERTIES_RESOURCE_PATH =
      "dremio-jdbc.properties";


  public DriverImpl() {
    super();
  }


  @Override
  protected String getConnectStringPrefix() {
    return CONNECTION_STRING_PREFIX;
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      return "com.dremio.jdbc.impl.DremioJdbc3Factory";
    case JDBC_40:
      return "com.dremio.jdbc.impl.DremioJdbc40Factory";
    case JDBC_41:
    default:
      return "com.dremio.jdbc.impl.DremioJdbc41Factory";
    }
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(
        this.getClass(),
        METADATA_PROPERTIES_RESOURCE_PATH,
        // Driver name and version:
        "Dremio JDBC Driver",
        DremioVersionInfo.getVersion(),
        // Database product name and version:
        "Dremio",
        "<Properties resource " + METADATA_PROPERTIES_RESOURCE_PATH + " not loaded>");
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new DremioMetaImpl((DremioConnectionImpl) connection);
  }

  @Override
  protected Handler createHandler() {
    return new DremioHandler();
  }

}
