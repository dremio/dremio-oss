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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.slf4j.Logger;

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.rpc.RpcException;
import com.dremio.jdbc.AlreadyClosedSqlException;
import com.dremio.jdbc.DremioConnection;
import com.dremio.jdbc.DremioConnectionConfig;
import com.dremio.jdbc.InvalidParameterSqlException;
import com.dremio.jdbc.JdbcApiSqlException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * Dremio's implementation of {@link Connection}.
 */
// (Was abstract to avoid errors _here_ if newer versions of JDBC added
// interface methods, but now newer versions would probably use Java 8's default
// methods for compatibility.)
class DremioConnectionImpl extends AvaticaConnection
                          implements DremioConnection {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioConnection.class);

  final DremioStatementRegistry openStatementsRegistry = new DremioStatementRegistry();
  final DremioConnectionConfig config;

  private final DremioClient client;
  private final TimeZone timeZone;

  protected DremioConnectionImpl(DriverImpl driver, AvaticaFactory factory,
                                String url, Properties info) throws SQLException {
    super(driver, factory, url, info);

    // Initialize transaction-related settings per Dremio behavior.
    super.setTransactionIsolation( TRANSACTION_NONE );
    super.setAutoCommit( true );

    this.config = new DremioConnectionConfig(info);
    this.timeZone = getTimeZone(this.config.timeZone());

    try {
      if (config.isLocal()) {
        throw new UnsupportedOperationException("Dremio JDBC driver doesn't not support local mode operation");
      }

      this.client = new DremioClient(driver.getSabotConfig(), config.isDirect());
      final String connect = config.getZookeeperConnectionString();
      this.client.setClientName("Dremio JDBC Driver");
      this.client.connect(connect, info);
    } catch (OutOfMemoryException e) {
      throw new SQLException("Failure creating root allocator", e);
    } catch (RpcException e) {
      // (Include cause exception's text in wrapping exception's text so
      // it's more likely to get to user (e.g., via SQLLine), and use
      // toString() since getMessage() text doesn't always mention error:)
      throw DremioExceptionMapper.map(e, "Failure in connecting to Dremio: %s", e.toString());
    }
  }

  @Override
  protected AvaticaStatement lookupStatement(StatementHandle h) throws SQLException {
    return super.lookupStatement(h);
  }

  @Override
  protected ExecuteResult prepareAndExecuteInternal(AvaticaStatement statement, String sql, long maxRowCount)
      throws SQLException, NoSuchStatementException {
    try {
      return super.prepareAndExecuteInternal(statement, sql, maxRowCount);
    } catch(RuntimeException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
      throw e;
    }
  }

  /**
   * Throws AlreadyClosedSqlException <i>iff</i> this Connection is closed.
   *
   * @throws  AlreadyClosedSqlException  if Connection is closed
   */
  private void throwIfClosed() throws AlreadyClosedSqlException, SQLException {
    if ( isClosed() ) {
      throw new AlreadyClosedSqlException( "Connection is already closed." );
    }
  }

  @Override
  public DremioConnectionConfig getConfig() {
    return config;
  }

  @Override
  public DremioClient getClient() {
    return client;
  }

  @Override
  public void setAutoCommit( boolean autoCommit ) throws SQLException {
    throwIfClosed();
    if ( ! autoCommit ) {
      throw new SQLFeatureNotSupportedException(
          "Can't turn off auto-committing; transactions are not supported.  "
          + "(Dremio is not transactional.)" );
    }
    assert getAutoCommit() : "getAutoCommit() = " + getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    throwIfClosed();
    if ( getAutoCommit() ) {
      throw new JdbcApiSqlException( "Can't call commit() in auto-commit mode." );
    }
    else {
      // (Currently not reachable.)
      throw new SQLFeatureNotSupportedException(
          "Connection.commit() is not supported.  (Dremio is not transactional.)" );
    }
  }

  @Override
  public void rollback() throws SQLException {
    throwIfClosed();
    if ( getAutoCommit()  ) {
      throw new JdbcApiSqlException( "Can't call rollback() in auto-commit mode." );
    }
    else {
      // (Currently not reachable.)
      throw new SQLFeatureNotSupportedException(
          "Connection.rollback() is not supported.  (Dremio is not transactional.)" );
    }
  }


  @Override
  public Savepoint setSavepoint() throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Dremio is not transactional.)" );
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Dremio is not transactional.)" );
  }

  @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Dremio is not transactional.)" );
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throwIfClosed();
    throw new SQLFeatureNotSupportedException(
        "Savepoints are not supported.  (Dremio is not transactional.)" );
  }


  private String isolationValueToString( final int level ) {
    switch ( level ) {
      case TRANSACTION_NONE:             return "TRANSACTION_NONE";
      case TRANSACTION_READ_UNCOMMITTED: return "TRANSACTION_READ_UNCOMMITTED";
      case TRANSACTION_READ_COMMITTED:   return "TRANSACTION_READ_COMMITTED";
      case TRANSACTION_REPEATABLE_READ:  return "TRANSACTION_REPEATABLE_READ";
      case TRANSACTION_SERIALIZABLE:     return "TRANSACTION_SERIALIZABLE";
      default:
        return "<Unknown transaction isolation level value " + level + ">";
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throwIfClosed();
    switch ( level ) {
      case TRANSACTION_NONE:
        // No-op.  (Is already set in constructor, and we disallow changing it.)
        break;
      case TRANSACTION_READ_UNCOMMITTED:
      case TRANSACTION_READ_COMMITTED:
      case TRANSACTION_REPEATABLE_READ:
      case TRANSACTION_SERIALIZABLE:
          throw new SQLFeatureNotSupportedException(
              "Can't change transaction isolation level to Connection."
              + isolationValueToString( level ) + " (from Connection."
              + isolationValueToString( getTransactionIsolation() ) + ")."
              + "  (Dremio is not transactional.)" );
      default:
        // Invalid value (or new one unknown to code).
        throw new JdbcApiSqlException(
            "Invalid transaction isolation level value " + level );
        //break;
    }
  }

  @Override
  public void setNetworkTimeout( Executor executor, int milliseconds )
      throws AlreadyClosedSqlException,
             JdbcApiSqlException,
             SQLFeatureNotSupportedException,
             SQLException {
    throwIfClosed();
    if ( null == executor ) {
      throw new InvalidParameterSqlException(
          "Invalid (null) \"executor\" parameter to setNetworkTimeout(...)" );
    }
    else if ( milliseconds < 0 ) {
      throw new InvalidParameterSqlException(
          "Invalid (negative) \"milliseconds\" parameter to"
          + " setNetworkTimeout(...) (" + milliseconds + ")" );
    }
    else {
      if ( 0 != milliseconds ) {
        throw new SQLFeatureNotSupportedException(
            "Setting network timeout is not supported." );
      }
    }
  }

  @Override
  public int getNetworkTimeout() throws AlreadyClosedSqlException, SQLException
  {
    throwIfClosed();
    return 0;  // (No timeout.)
  }


  @Override
  public DremioStatementImpl createStatement(int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    throwIfClosed();
    DremioStatementImpl statement =
        (DremioStatementImpl) super.createStatement(resultSetType,
                                                   resultSetConcurrency,
                                                   resultSetHoldability);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    throwIfClosed();
    DremioPreparedStatementImpl statement =
        (DremioPreparedStatementImpl) super.prepareStatement(sql,
                                                            resultSetType,
                                                            resultSetConcurrency,
                                                            resultSetHoldability);
    return statement;
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }


  // Note:  Using dynamic proxies would reduce the quantity (450?) of method
  // overrides by eliminating those that exist solely to check whether the
  // object is closed.  It would also eliminate the need to throw non-compliant
  // RuntimeExceptions when Avatica's method declarations won't let us throw
  // proper SQLExceptions. (Check performance before applying to frequently
  // called ResultSet.)

  // No isWrapperFor(Class<?>) (it doesn't throw SQLException if already closed).
  // No unwrap(Class<T>) (it doesn't throw SQLException if already closed).

  @Override
  public AvaticaStatement createStatement() throws SQLException {
    throwIfClosed();
    return super.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throwIfClosed();
    return super.prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throwIfClosed();
    try {
      return super.nativeSQL(sql);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }


  @Override
  public boolean getAutoCommit() throws SQLException {
    throwIfClosed();
    return super.getAutoCommit();
  }

  // No close() (it doesn't throw SQLException if already closed).

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throwIfClosed();
    return super.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throwIfClosed();
    super.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwIfClosed();
    return super.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throwIfClosed();
    super.setCatalog(catalog);
  }

  @Override
  public synchronized String getCatalog() throws SQLException {
    throwIfClosed();
    String catalog = super.getCatalog();
    if (null == catalog) {
      // There is no catalog set in the local properties initially, so lazily load it from Dremio. Note that Dremio
      // currently only ever has a single catalog.
      try (final ResultSet rs = getMetaData().getCatalogs()) {
        if (rs.next()) {
          catalog = rs.getString(1);
          super.setCatalog(catalog);
        }
      }
    }

    return catalog;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throwIfClosed();
    return super.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throwIfClosed();
    return super.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throwIfClosed();
    super.clearWarnings();
  }

  @Override
  public Statement createStatement(int resultSetType,
                                   int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    return super.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    return super.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql, resultSetType, resultSetConcurrency);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Map<String,Class<?>> getTypeMap() throws SQLException {
    throwIfClosed();
    try {
      return super.getTypeMap();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setTypeMap(Map<String,Class<?>> map) throws SQLException {
    throwIfClosed();
    try {
      super.setTypeMap(map);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throwIfClosed();
    super.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    throwIfClosed();
    return super.getHoldability();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareCall(sql, resultSetType, resultSetConcurrency,
                               resultSetHoldability);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            int autoGeneratedKeys) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, autoGeneratedKeys);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            int columnIndexes[]) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, columnIndexes);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public PreparedStatement prepareStatement(String sql,
                                            String columnNames[]) throws SQLException {
    throwIfClosed();
    try {
      return super.prepareStatement(sql, columnNames);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Clob createClob() throws SQLException {
    throwIfClosed();
    try {
      return super.createClob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Blob createBlob() throws SQLException {
    throwIfClosed();
    try {
      return super.createBlob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public NClob createNClob() throws SQLException {
    throwIfClosed();
    try {
      return super.createNClob();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throwIfClosed();
    try {
      return super.createSQLXML();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return super.isValid(timeout) && client.isActive();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      throwIfClosed();
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), null, e);
    }
    try {
      super.setClientInfo(name,  value);
    }
    catch (UnsupportedOperationException e) {
      SQLFeatureNotSupportedException intended =
          new SQLFeatureNotSupportedException(e.getMessage(), e);
      throw new SQLClientInfoException(e.getMessage(), null, intended);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      throwIfClosed();
    } catch (SQLException e) {
      throw new SQLClientInfoException(e.getMessage(), null, e);
    }
    try {
      super.setClientInfo(properties);
    }
    catch (UnsupportedOperationException e) {
      SQLFeatureNotSupportedException intended =
          new SQLFeatureNotSupportedException(e.getMessage(), e);
      throw new SQLClientInfoException(e.getMessage(), null, intended);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throwIfClosed();
    try {
      return super.getClientInfo(name);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throwIfClosed();
    try {
      return super.getClientInfo();
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throwIfClosed();
    try {
      return super.createArrayOf(typeName, elements);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throwIfClosed();
    try {
      return super.createStruct(typeName, attributes);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throwIfClosed();
    super.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    throwIfClosed();
    return super.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throwIfClosed();
    try {
      super.abort(executor);
    }
    catch (UnsupportedOperationException e) {
      throw new SQLFeatureNotSupportedException(e.getMessage(), e);
    }
  }

  /**
   * Retrieve either the TimeZone associated with the String ID or the JVM default.
   *
   * @param timeZone The String code for the TimeZone, ex UTC, PST.
   * @return The TimeZone associated with the code, or the JVM default if none exists.
   */
  private TimeZone getTimeZone(String timeZone) {
    if (!Strings.isNullOrEmpty(timeZone)) {
      try {
        return TimeZone.getTimeZone(ZoneId.of(timeZone));
      } catch (DateTimeException e) {
        // Eat the exception to hit the default return.
      }
    }

    return TimeZone.getDefault();
  }

  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

  // do not make public
  AvaticaFactory getFactory() {
    return factory;
  }

  private static void closeOrWarn(final AutoCloseable autoCloseable, final String message, final Logger logger) {
    if (autoCloseable == null) {
      return;
    }

    try {
      autoCloseable.close();
    } catch(Exception e) {
      logger.warn(message, e);
    }
  }

  // TODO this should be an AutoCloseable, and this should be close()
  void cleanup() {
    // First close any open JDBC Statement objects, to close any open ResultSet
    // objects and release their buffers/vectors.
    openStatementsRegistry.close();

    // TODO all of these should use DeferredException when it is available from DRILL-2245
    closeOrWarn(client, "Exception while closing client.", logger);
  }
}
