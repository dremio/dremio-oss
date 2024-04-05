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

import com.dremio.exec.client.ServerMethod;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

class DremioMetaImpl extends MetaImpl {
  // TODO:  Use more central version of these constants if available.

  /** JDBC conventional(?) number of fractional decimal digits for REAL. */
  static final int DECIMAL_DIGITS_REAL = 7;

  /** JDBC conventional(?) number of fractional decimal digits for FLOAT. */
  static final int DECIMAL_DIGITS_FLOAT = DECIMAL_DIGITS_REAL;

  /** JDBC conventional(?) number of fractional decimal digits for DOUBLE. */
  static final int DECIMAL_DIGITS_DOUBLE = 15;

  /** Radix used to report precisions of "datetime" types. */
  static final int RADIX_DATETIME = 10;

  /** Radix used to report precisions of interval types. */
  static final int RADIX_INTERVAL = 10;

  private final DremioConnectionImpl connection;
  private volatile DremioMeta delegate;

  DremioMetaImpl(DremioConnectionImpl connection) {
    super(connection);
    this.connection = connection;
  }

  private DremioMeta getDelegate() throws SQLException {
    if (delegate == null) {
      synchronized (this) {
        if (delegate == null) {
          // Choosing the right meta helper
          final Collection<ServerMethod> requiredMetaMethods =
              Arrays.asList(
                  ServerMethod.GET_CATALOGS,
                  ServerMethod.GET_SCHEMAS,
                  ServerMethod.GET_TABLES,
                  ServerMethod.GET_COLUMNS);
          if (connection.getConfig().isServerMetadataDisabled()
              || !connection.getClient().getSupportedMethods().containsAll(requiredMetaMethods)) {
            delegate = new DremioMetaClientImpl(connection);
          } else {
            delegate = new DremioMetaServerImpl(connection);
          }
        }
      }
    }
    return delegate;
  }

  static Signature newSignature(String sql) {
    return new Signature(
        new DremioColumnMetaDataList(),
        sql,
        Collections.<AvaticaParameter>emptyList(),
        Collections.<String, Object>emptyMap(),
        null, // CursorFactory set to null, as SQL requests use DremioCursor
        StatementType.SELECT);
  }

  protected static ColumnMetaData.StructType fieldMetaData(Class<?> clazz) {
    return MetaImpl.fieldMetaData(clazz);
  }

  /** Implements {@link DatabaseMetaData#getTables}. */
  @Override
  public MetaResultSet getTables(
      ConnectionHandle ch,
      String catalog,
      final Pat schemaPattern,
      final Pat tableNamePattern,
      final List<String> typeList) {
    try {
      return getDelegate().getTables(catalog, schemaPattern, tableNamePattern, typeList);
    } catch (SQLException e) {
      // Wrap in SQLExecutionError because Avatica's abstract method declarations
      // didn't allow for SQLException!
      throw new SQLExecutionError(e.getMessage(), e);
    }
  }

  /** Implements {@link DatabaseMetaData#getColumns}. */
  @Override
  public MetaResultSet getColumns(
      ConnectionHandle ch,
      String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    try {
      return getDelegate().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    } catch (SQLException e) {
      // Wrap in SQLExecutionError because Avatica's abstract method declarations
      // didn't allow for SQLException!
      throw new SQLExecutionError(e.getMessage(), e);
    }
  }

  /** Implements {@link DatabaseMetaData#getSchemas}. */
  @Override
  public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
    try {
      return getDelegate().getSchemas(catalog, schemaPattern);
    } catch (SQLException e) {
      // Wrap in SQLExecutionError because Avatica's abstract method declarations
      // didn't allow for SQLException!
      throw new SQLExecutionError(e.getMessage(), e);
    }
  }

  /** Implements {@link DatabaseMetaData#getCatalogs}. */
  @Override
  public MetaResultSet getCatalogs(ConnectionHandle ch) {
    try {
      return getDelegate().getCatalogs();
    } catch (SQLException e) {
      // Wrap in SQLExecutionError because Avatica's abstract method declarations
      // didn't allow for SQLException!
      throw new SQLExecutionError(e.getMessage(), e);
    }
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    StatementHandle result = super.createStatement(ch);
    result.signature = newSignature(sql);

    return result;
  }

  @Override
  public ExecuteResult prepareAndExecute(
      StatementHandle h, String sql, long maxRowCount, PrepareCallback callback) {
    return prepareAndExecute(h, sql, maxRowCount, -1, callback);
  }

  @Override
  public ExecuteResult prepareAndExecute(
      StatementHandle h,
      String sql,
      long maxRowCount,
      int maxRowsInFirstFrame,
      PrepareCallback callback) {
    final Signature signature = newSignature(sql);
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        callback.assign(signature, null, -1);
      }
      callback.execute();
      final MetaResultSet metaResultSet =
          MetaResultSet.create(h.connectionId, h.id, false, signature, null);
      return new ExecuteResult(Collections.singletonList(metaResultSet));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle h, List<String> sqlCommands) throws NoSuchStatementException {
    final List<Long> updateCounts = new ArrayList<>();
    final Meta.PrepareCallback callback =
        new Meta.PrepareCallback() {
          private final Object monitor = new Object();
          long updateCount;

          @Override
          public Object getMonitor() {
            return monitor;
          }

          @Override
          public void clear() throws SQLException {}

          @Override
          public void assign(Meta.Signature signature, Meta.Frame firstFrame, long updateCount)
              throws SQLException {
            this.updateCount = updateCount;
          }

          @Override
          public void execute() throws SQLException {
            updateCounts.add(updateCount);
          }
        };

    for (String sqlCommand : sqlCommands) {
      prepareAndExecute(h, sqlCommand, -1L, -1, callback);
    }
    return new ExecuteBatchResult(Longs.toArray(updateCounts));
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    // Signature might have been zeroed by AvaticaConnection#executeQueryInternal()
    // Get it from the original handle
    final AvaticaStatement stmt;
    try {
      stmt = connection.lookupStatement(h);
    } catch (SQLException e) {
      throw new NoSuchStatementException(h);
    }
    MetaResultSet metaResultSet =
        MetaResultSet.create(h.connectionId, h.id, false, stmt.handle.signature, null);
    return new ExecuteResult(ImmutableList.of(metaResultSet));
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, long maxRowCount)
      throws NoSuchStatementException {
    return execute(h, parameterValues, AvaticaUtils.toSaturatedInt(maxRowCount));
  }

  @Override
  public ExecuteBatchResult executeBatch(
      StatementHandle h, List<List<TypedValue>> parameterValueLists)
      throws NoSuchStatementException {
    final List<Long> updateCounts = new ArrayList<>();
    for (List<TypedValue> parameterValueList : parameterValueLists) {
      ExecuteResult executeResult = execute(h, parameterValueList, -1);
      final long updateCount =
          executeResult.resultSets.size() == 1 ? executeResult.resultSets.get(0).updateCount : -1L;
      updateCounts.add(updateCount);
    }
    return new ExecuteBatchResult(Longs.toArray(updateCounts));
  }

  @Override
  public void openConnection(ConnectionHandle ch, Map<String, String> info) {
    super.openConnection(ch, info);
  }

  @Override
  public void commit(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(ConnectionHandle ch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
      throws NoSuchStatementException, MissingResultsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean syncResults(StatementHandle sh, QueryState state, long offset)
      throws NoSuchStatementException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeStatement(StatementHandle h) {
    // Nothing
  }
}
