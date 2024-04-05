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
package com.dremio.jdbc.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.ExecTest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Ignore;
import org.junit.Test;

// NOTE:  Currently, must not inherit from anything that loads the Dremio driver
// class (and must not be run in JVM where the Dremio driver class has already
// been loaded).

/** Test of TracingProxyDriver's loading of driver class. */
public class TracingProxyDriverClassLoadingTest extends ExecTest {

  @Ignore("except when run in own JVM (so Dremio Driver not already loaded)")
  @Test
  public void testClassLoading() throws SQLException, ClassNotFoundException {

    // Note:  Throwing exceptions for test setup problems so they're JUnit
    // errors (red in Eclipse's JUnit view), not just JUnit test failures
    // (blue in Eclipse).

    // 1.  Confirm that Dremio driver is not loaded/registered.
    assertThatThrownBy(() -> DriverManager.getDriver("jdbc:dremio:zk=localhost:123456"))
        .isInstanceOf(SQLException.class)
        .hasMessage("No suitable driver");

    assertThatThrownBy(() -> DriverManager.getConnection("jdbc:dremio:zk=localhost:123456", null))
        .isInstanceOf(SQLException.class)
        .hasMessage("No suitable driver found for jdbc:dremio:zk=localhost:123456");

    // 2.  Confirm that TracingProxyDriver is not loaded/registered.
    assertThatThrownBy(() -> DriverManager.getDriver("jdbc:proxy::jdbc:dremio:zk=localhost:123456"))
        .isInstanceOf(SQLException.class)
        .hasMessage("No suitable driver");

    assertThatThrownBy(
            () -> DriverManager.getConnection("jdbc:proxy::jdbc:dremio:zk=localhost:123456", null))
        .isInstanceOf(SQLException.class)
        .hasMessage("No suitable driver found for jdbc:proxy::jdbc:dremio:zk=localhost:123456");

    // 3.  Load TracingProxyDriver.
    Class.forName("com.dremio.jdbc.proxy.TracingProxyDriver");

    // 4.  Confirm that Dremio driver still is not registered.
    assertThatThrownBy(
            () -> DriverManager.getConnection("jdbc:proxy::jdbc:dremio:zk=localhost:123456", null))
        .isInstanceOf(ProxySetupSQLException.class)
        .hasMessage(
            "Error getting driver from DriverManager for proxied URL"
                + " \"jdbc:dremio:zk=localhost:123456\" (from proxy driver URL"
                + " \"jdbc:proxy::jdbc:dremio:zk=localhost:123456\" (after third colon))"
                + ": java.sql.SQLException: No suitable driver");

    // 5.  Test that TracingProxyDriver can load and use a specified Driver class.
    final Driver driver =
        DriverManager.getDriver(
            "jdbc:proxy:com.dremio.jdbc.Driver:jdbc:dremio:zk=localhost:123456");

    assertThat(driver.acceptsURL("jdbc:proxy::jdbc:dremio:zk=localhost:123456")).isTrue();
    assertThat(driver.acceptsURL("jdbc:dremio:zk=localhost:123456")).isFalse();

    // 7.  Test minimally that driver can get connection that works.
    final Connection proxyConnection =
        DriverManager.getConnection("jdbc:proxy::jdbc:dremio:zk=localhost:123456", null);
    assertThat(proxyConnection).isNotNull();

    final DatabaseMetaData dbMetaData = proxyConnection.getMetaData();
    assertThat(dbMetaData).isInstanceOf(DatabaseMetaData.class);
  }
} // class TracingProxyDriverClassLoadingTest
