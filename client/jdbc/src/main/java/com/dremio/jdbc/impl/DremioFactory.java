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

package com.dremio.jdbc.impl;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;


/**
 * Partial implementation of {@link net.hydromatic.avatica.AvaticaFactory}
 * (factory for main JDBC objects) for Dremio's JDBC driver.
 * <p>
 *   Handles JDBC version number.
 * </p>
 */
abstract class DremioFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /** Creates a JDBC factory with given major/minor version number. */
  protected DremioFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public int getJdbcMajorVersion() {
    return major;
  }

  @Override
  public int getJdbcMinorVersion() {
    return minor;
  }


  /**
   * Creates a Dremio connection for Avatica (in terms of Avatica types).
   * <p>
   *   This implementation delegates to
   *   {@link #newDremioConnection(DriverImpl, DremioFactory, String, Properties)}.
   * </p>
   */
  @Override
  public final AvaticaConnection newConnection(UnregisteredDriver driver,
                                               AvaticaFactory factory,
                                               String url,
                                               Properties info) throws SQLException {
    return newConnection((DriverImpl) driver, (DremioFactory) factory, url, info);
  }

  /**
   * Creates a Dremio connection (in terms of Dremio-specific types).
   */
  abstract DremioConnectionImpl newConnection(DriverImpl driver,
                                                  DremioFactory factory,
                                                  String url,
                                                  Properties info) throws SQLException;
}
