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

package com.dremio.jdbc.impl;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLXML;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.QueryState;

import com.dremio.exec.client.DremioClient;
import com.dremio.exec.client.ServerMethod;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.rpc.RpcFuture;


/**
 * Implementation of {@link net.hydromatic.avatica.AvaticaFactory} for Dremio and
 * JDBC 4.1 (corresponds to JDK 1.7).
 */
// Note:  Must be public so net.hydromatic.avatica.UnregisteredDriver can
// (reflectively) call no-args constructor.
public class DremioJdbc41Factory extends DremioFactory {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioJdbc41Factory.class);

  /** Creates a factory for JDBC version 4.1. */
  // Note:  Must be public so net.hydromatic.avatica.UnregisteredDriver can
  // (reflectively) call this constructor.
  public DremioJdbc41Factory() {
    this(4, 1);
  }

  /** Creates a JDBC factory with given major/minor version number. */
  protected DremioJdbc41Factory(int major, int minor) {
    super(major, minor);
  }


  @Override
  DremioConnectionImpl newConnection(DriverImpl driver,
                                     DremioFactory factory,
                                     String url,
                                     Properties info) throws SQLException {
    return new DremioConnectionImpl(driver, factory, url, info);
  }

  @Override
  public DremioDatabaseMetaDataImpl newDatabaseMetaData(AvaticaConnection connection) {
    return new DremioDatabaseMetaDataImpl(connection);
  }


  @Override
  public DremioStatementImpl newStatement(AvaticaConnection connection,
                                         StatementHandle h,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) {
    return new DremioStatementImpl((DremioConnectionImpl) connection,
                                  h,
                                  resultSetType,
                                  resultSetConcurrency,
                                  resultSetHoldability);
  }

  @Override
  public DremioJdbc41PreparedStatement newPreparedStatement(AvaticaConnection connection,
                                                       StatementHandle h,
                                                       Meta.Signature signature,
                                                       int resultSetType,
                                                       int resultSetConcurrency,
                                                       int resultSetHoldability)
      throws SQLException {
    DremioConnectionImpl dremioConnection = (DremioConnectionImpl) connection;
    DremioClient client = dremioConnection.getClient();
    if (dremioConnection.getConfig().isServerPreparedStatementDisabled() || !client.getSupportedMethods().contains(ServerMethod.PREPARED_STATEMENT)) {
      // fallback to client side prepared statement
      return new DremioJdbc41PreparedStatement(dremioConnection, h, signature, null, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
    return newServerPreparedStatement(dremioConnection, h, signature, resultSetType,
        resultSetConcurrency, resultSetHoldability);
  }

  private DremioJdbc41PreparedStatement newServerPreparedStatement(DremioConnectionImpl connection,
                                                                  StatementHandle h,
                                                                  Meta.Signature signature,
                                                                  int resultSetType,
                                                                  int resultSetConcurrency,
                                                                  int resultSetHoldability)
      throws SQLException {
    String sql = signature.sql;

    try {
      RpcFuture<CreatePreparedStatementResp> respFuture = connection.getClient().createPreparedStatement(signature.sql);

      CreatePreparedStatementResp resp;
      try {
        resp = respFuture.get();
      } catch (InterruptedException e) {
        // Preserve evidence that the interruption occurred so that code higher up
        // on the call stack can learn of the interruption and respond to it if it
        // wants to.
        Thread.currentThread().interrupt();

        throw new SQLException( "Interrupted", e );
      }

      final RequestStatus status = resp.getStatus();
      if (status != RequestStatus.OK) {
        final String errMsgFromServer = resp.getError() != null ? resp.getError().getMessage() : "";

        if (status == RequestStatus.TIMEOUT) {
          logger.error("Request timed out to create prepare statement: {}", errMsgFromServer);
          throw new SQLTimeoutException("Failed to create prepared statement: " + errMsgFromServer);
        }

        if (status == RequestStatus.FAILED) {
          logger.error("Failed to create prepared statement: {}", errMsgFromServer);
          throw new SQLException("Failed to create prepared statement: " + resp.getError());
        }

        logger.error("Failed to create prepared statement. Unknown status: {}, Error: {}", status, errMsgFromServer);
        throw new SQLException(String.format(
            "Failed to create prepared statement. Unknown status: %s, Error: %s", status, errMsgFromServer));
      }

      return new DremioJdbc41PreparedStatement(connection,
          h,
          signature,
          resp.getPreparedStatement(),
          resultSetType,
          resultSetConcurrency,
          resultSetHoldability);
    } catch (SQLException e) {
      throw e;
    } catch (RuntimeException e) {
      throw Helper.INSTANCE.createException("Error while preparing statement [" + sql + "]", e);
    } catch (Exception e) {
      throw Helper.INSTANCE.createException("Error while preparing statement [" + sql + "]", e);
    }

  }

  @Override
  public DremioResultSetImpl newResultSet(AvaticaStatement statement,
                                         QueryState state,
                                         Meta.Signature signature,
                                         TimeZone timeZone,
                                         Meta.Frame firstFrame) {
    final ResultSetMetaData metaData =
        newResultSetMetaData(statement, signature);
    return new DremioResultSetImpl(statement, state, signature, metaData, timeZone, firstFrame);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
                                                Meta.Signature signature) {
    return new DremioResultSetMetaDataImpl(statement, null, signature);
  }


  /**
   * JDBC 4.1 version of {@link DremioPreparedStatementImpl}.
   */
  private static class DremioJdbc41PreparedStatement extends DremioPreparedStatementImpl {

    DremioJdbc41PreparedStatement(DremioConnectionImpl connection,
                                 StatementHandle h,
                                 Meta.Signature signature,
                                 com.dremio.exec.proto.UserProtos.PreparedStatement pstmt,
                                 int resultSetType,
                                 int resultSetConcurrency,
                                 int resultSetHoldability) throws SQLException {
      super(connection, h, signature, pstmt,
            resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    // These don't need throwIfClosed(), since getParameter already calls it.

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
      getSite(parameterIndex).setRowId(x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
      getSite(parameterIndex).setNString(value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value,
                                    long length) throws SQLException {
      getSite(parameterIndex).setNCharacterStream(value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
      getSite(parameterIndex).setNClob(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader,
                        long length) throws SQLException {
      getSite(parameterIndex).setClob(reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream,
                        long length) throws SQLException {
      getSite(parameterIndex).setBlob(inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader,
                         long length) throws SQLException {
      getSite(parameterIndex).setNClob(reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      getSite(parameterIndex).setSQLXML(xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x,
                               long length) throws SQLException {
      getSite(parameterIndex).setAsciiStream(x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x,
                                long length) throws SQLException {
      getSite(parameterIndex).setBinaryStream(x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader,
                                   long length) throws SQLException {
      getSite(parameterIndex).setCharacterStream(reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex,
                               InputStream x) throws SQLException {
      getSite(parameterIndex).setAsciiStream(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex,
                                InputStream x) throws SQLException {
      getSite(parameterIndex).setBinaryStream(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex,
                                   Reader reader) throws SQLException {
      getSite(parameterIndex).setCharacterStream(reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex,
                                    Reader value) throws SQLException {
      getSite(parameterIndex).setNCharacterStream(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
      getSite(parameterIndex).setClob(reader);
    }

    @Override
    public void setBlob(int parameterIndex,
                        InputStream inputStream) throws SQLException {
      getSite(parameterIndex).setBlob(inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      getSite(parameterIndex).setNClob(reader);
    }

  }

}

// End DremioJdbc41Factory.java
