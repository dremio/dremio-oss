/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.jdbc.test.JdbcAssert;

/**
 * Test compatibility with older versions of the server
 */
@Ignore("DX-2490")
public class LegacyDatabaseMetaDataGetColumnsTest extends DatabaseMetaDataGetColumnsTest {
  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Properties defaultProperties = JdbcAssert.getDefaultProperties();
    defaultProperties.setProperty("server.metadata.disabled", "true");

    setupConnection(defaultProperties );
    setUpMetadataToCheck();
  }

  // Override because of DRILL-1959

  @Override
  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeString() throws SQLException {
    assertThat( rowsMetadata.getColumnTypeName( 22 ), equalTo( "INTEGER" ) );
  }

  @Override
  @Test
  public void test_SOURCE_DATA_TYPE_hasRightTypeCode() throws SQLException {
    assertThat( rowsMetadata.getColumnType( 22 ), equalTo( Types.INTEGER ) );
  }

  @Override
  @Test
  public void test_SOURCE_DATA_TYPE_hasRightClass() throws SQLException {
    assertThat( rowsMetadata.getColumnClassName( 22 ),
                equalTo( Integer.class.getName() ) );
  }
}
