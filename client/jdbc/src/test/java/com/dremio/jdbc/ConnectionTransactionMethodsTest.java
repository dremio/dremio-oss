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

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Test;


/**
 * Test for Dremio's implementation of Connection's main transaction-related methods.
 */
public class ConnectionTransactionMethodsTest extends JdbcWithServerTestBase {

  ////////////////////////////////////////
  // Transaction mode methods:

  //////////
  // Transaction isolation level:

  @Test
  public void testGetTransactionIsolationSaysNone() throws SQLException {
    assertThat(getConnection().getTransactionIsolation()).isEqualTo(TRANSACTION_NONE);
  }

  @Test
  public void testSetTransactionIsolationNoneExitsNormally() throws SQLException {
    getConnection().setTransactionIsolation(TRANSACTION_NONE);
  }

  // Test trying to set to unsupported isolation levels:

  // (Sample message:  "Can't change transaction isolation level to
  // Connection.TRANSACTION_REPEATABLE_READ (from Connection.TRANSACTION_NONE).
  // (Dremio is not transactional.)" (as of 2015-04-22))

  @Test
  public void testSetTransactionIsolationReadUncommittedThrows() {
    assertThatThrownBy(() -> getConnection().setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED))
      .isInstanceOf(SQLFeatureNotSupportedException.class)
      .hasMessageContaining("TRANSACTION_READ_UNCOMMITTED")
      .hasMessageContaining("transaction isolation level")
      .hasMessageContaining("TRANSACTION_NONE");
  }

  @Test
  public void testSetTransactionIsolationReadCommittedThrows() {
    assertThatThrownBy(() -> getConnection().setTransactionIsolation(TRANSACTION_READ_COMMITTED))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testSetTransactionIsolationRepeatableReadThrows() {
    assertThatThrownBy(() -> getConnection().setTransactionIsolation(TRANSACTION_REPEATABLE_READ))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testSetTransactionIsolationSerializableThrows() {
    assertThatThrownBy(() -> getConnection().setTransactionIsolation(TRANSACTION_SERIALIZABLE))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testSetTransactionIsolationBadIntegerThrows() {
    // not any TRANSACTION_* value
    assertThatThrownBy(() -> getConnection().setTransactionIsolation(15))
      .isInstanceOf(JdbcApiSqlException.class);
  }

  //////////
  // Auto-commit mode.

  @Test
  public void testGetAutoCommitSaysAuto() throws SQLException {
    // Auto-commit should always be true.
    assertThat(getConnection().getAutoCommit()).isEqualTo(true);
  }

  @Test
  public void testSetAutoCommitTrueExitsNormally() throws SQLException {
    // Setting auto-commit true (redundantly) shouldn't throw exception.
    getConnection().setAutoCommit(true);
  }

  ////////////////////////////////////////
  // Transaction operation methods:

  @Test
  public void testCommitThrows() {
    // Should fail saying because in auto-commit mode (or maybe because not supported).
    assertThatThrownBy(() -> getConnection().commit())
      .isInstanceOf(JdbcApiSqlException.class);
  }

  @Test
  public void testRollbackThrows() {
    // Should fail saying because in auto-commit mode (or maybe because not supported).
    assertThatThrownBy(() -> getConnection().rollback())
      .isInstanceOf(JdbcApiSqlException.class);
  }

  ////////////////////////////////////////
  // Savepoint methods:

  @Test
  public void testSetSavepointUnamed() {
    assertThatThrownBy(() -> getConnection().setSavepoint())
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testSetSavepointNamed() {
    assertThatThrownBy(() -> getConnection().setSavepoint("savepoint name"))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testRollbackSavepoint() {
    assertThatThrownBy(() -> getConnection().rollback(null))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testReleaseSavepoint() {
    assertThatThrownBy(() -> getConnection().releaseSavepoint(null))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

}
