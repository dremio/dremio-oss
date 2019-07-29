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
package com.dremio.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.test.DremioTest;
import com.google.common.io.Resources;

/**
 * (Some) unit and integration tests for com.dremio.jdbc.Driver.
 */
public class DriverTest extends DremioTest {

  // TODO: Move Jetty status server disabling to DremioTest.
  private static final String STATUS_SERVER_PROPERTY_NAME =
      ExecConstants.HTTP_ENABLE;

  private static final String origJettyPropValue =
      System.getProperty( STATUS_SERVER_PROPERTY_NAME, "true" );

  // Disable Jetty status server so unit tests run (outside Maven setup).
  // (TODO:  Move this to base test class and/or have Jetty try other ports.)
  @BeforeClass
  public static void setUpClass() {
    System.setProperty( STATUS_SERVER_PROPERTY_NAME, "false" );
  }

  @AfterClass
  public static void tearDownClass() {
    System.setProperty( STATUS_SERVER_PROPERTY_NAME, origJettyPropValue );
  }

  private Driver uut = new Driver();


  ////////////////////////////////////////
  // Tests of methods defined by JDBC/java.sql.Driver:

  // Tests for connect() (defined by JDBC/java.sql.Driver):

  @Test
  public void test_connect_declinesEmptyUrl()
      throws SQLException
  {
    assertThat( uut.connect( "", null ), nullValue() );
  }

  @Test
  public void test_connect_declinesNonUrl()
      throws SQLException
  {
    assertThat( uut.connect( "whatever", null ), nullValue() );
  }

  @Test
  public void test_connect_declinesNonJdbcUrl()
      throws SQLException
  {
    assertThat( uut.connect( "file:///something", null ), nullValue() );
  }

  @Test
  public void test_connect_declinesNonDremioJdbcUrl()
      throws SQLException
  {
    assertThat( uut.connect( "jdbc:somedb:whatever", null ), nullValue() );
  }

  @Test
  public void test_connect_declinesNotQuiteDremioUrl()
      throws SQLException
  {
    assertThat( uut.connect( "jdbc:dremio", null ), nullValue() );
  }

  // TODO  Determine whether this "jdbc:dremio:" should be valid or error.
  @Ignore( "Just hangs, trying to connect to non-existent local zookeeper." )
  @Test
  public void test_connect_acceptsMinimalDremioJdbcUrl()
      throws SQLException
  {
    assertThat( uut.connect( "jdbc:dremio:", null ), nullValue() );
    fail( "Not implemented yet" );
  }

  // TODO:  Determine rules for Dremio JDBC URLs. (E.g., is "zk=..." always
  // required?  What other properties are allowed?  What is disallowed?)
  @Ignore( "Just hangs, trying to connect to non-existent local zookeeper." )
  @Test
  public void test_connect_DECIDEWHICHBogusDremioJdbcUrl()
      throws SQLException
  {
    assertThat( uut.connect( "jdbc:dremio:x=y;z;;a=b=c=d;what=ever", null ),
                nullValue() );
    fail( "Not implemented yet" );
  }

  // TODO:  Determine which other cases to test, including cases of Properties
  // parameter values to test.


  // Tests for acceptsURL(String) (defined by JDBC/java.sql.Driver):

  @Test
  public void test_acceptsURL_acceptsDremioUrlMinimal()
    throws SQLException
  {
    assertThat( uut.acceptsURL("jdbc:dremio:"), equalTo( true ) );
  }

  @Test
  public void test_acceptsURL_acceptsDremioPlusJunk()
    throws SQLException
  {
    assertThat( uut.acceptsURL("jdbc:dremio:should it check this?"),
                equalTo( true ) );
  }

  @Test
  public void test_acceptsURL_rejectsNonDremioJdbcUrl()
    throws SQLException
  {
    assertThat( uut.acceptsURL("jdbc:notdremio:whatever"), equalTo( false ) );
  }

  @Test
  public void test_acceptsURL_rejectsNonDremioJdbc2()
      throws SQLException
  {
    assertThat( uut.acceptsURL("jdbc:optiq:"), equalTo( false ) );
  }

  @Test
  public void test_acceptsURL_rejectsNonJdbcUrl()
      throws SQLException
  {
    assertThat( uut.acceptsURL("dremio:"), equalTo( false ) );
  }


  // Tests for getPropertyInfo(String, Properties) (defined by
  // JDBC/java.sql.Driver):
  // TODO:  Determine what properties (if any) should be returned.
  @Ignore( "Deferred pending need." )
  @Test
  public void test_getPropertyInfo()
      throws SQLException
  {
    fail( "Not implemented yet" );
  }


  // Tests for getMajorVersion() (defined by JDBC/java.sql.Driver):
  @Test
  public void test_getMajorVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(Resources.getResource("dremio-jdbc.properties").openStream());

    assertThat( uut.getMajorVersion(),
        org.hamcrest.CoreMatchers.is( Integer.parseInt(properties.getProperty("driver.version.major"))) );
  }


  // Tests for getMinorVersion() (defined by JDBC/java.sql.Driver):
  @Test
  public void test_getMinorVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(Resources.getResource("dremio-jdbc.properties").openStream());

    assertThat( uut.getMinorVersion(),
        org.hamcrest.CoreMatchers.is( Integer.parseInt(properties.getProperty("driver.version.minor"))) );
  }


  // Tests for jdbcCompliant() (defined by JDBC/java.sql.Driver):
  // TODO  Determine what we choose to return.  If it doesn't match what
  // java.sql.Driver's documentation says, document that on DremioResultSet and
  // where users (programmers) can see it.
  @Ignore( "Seemingly: Bug: Returns true, but hasn't passed compliance tests." )
  @Test
  public void test_jdbcCompliant() {
    // Expect false because not known to have "[passed] the JDBC compliance
    // tests."
    assertThat( uut.jdbcCompliant(), equalTo( false ) );
  }

  // Tests for XXX (defined by JDBC/java.sql.Driver):
  // Defined by JDBC/java.sql.Driver: getParentLogger()
  @Ignore( "Deferred pending need." )
  @Test
  public void test_getParentLogger()
  {
    fail( "Not implemented yet" );
  }


  // Tests for XXX (defined by JDBC/java.sql.Driver):
  // Defined by JDBC/java.sql.Driver: "[driver] should create and instance of
  // itself and register it with the DriverManager.
  @Test
  public void test_Driver_registersWithManager()
    throws SQLException
  {
    assertThat( DriverManager.getDriver( "jdbc:dremio:whatever" ),
                instanceOf( Driver.class ) );
  }


  ////////////////////////////////////////
  // Tests of methods defined by net.hydromatic.avatica.UnregisteredDriver.


  @Ignore( "Deferred pending need." )
  @Test
  public void test_load() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getConnectStringPrefix() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getFactoryClassNameJdbcVersion()
  {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_createDriverVersion() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_createHandler() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_unregisteredDriver() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_createFactory() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_createHandler1() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getFactoryClassNameJdbcVersion1()
  {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_createDriverVersion1() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getConnectionProperties() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_instantiateFactory() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getConnectStringPrefix1() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_getDriverVersion() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_register() {
    fail( "Not implemented yet" );
  }

  ////////////////////////////////////////


  @Ignore( "Deferred pending need." )
  @Test
  public void test_hashCode() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_equals() {
    fail( "Not implemented yet" );
  }

  @Ignore( "Deferred pending need." )
  @Test
  public void test_toString() {
    fail( "Not implemented yet" );
  }

}
