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
package com.dremio.exec.store.hive;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.exceptions.UserException;
import com.dremio.hive.thrift.TException;

/**
 * HiveMetaStoreClient to create and maintain (reconnection cases) connection to Hive metastore with given user
 * credentials and check authorization privileges if set.
 */
public class HiveClientWithAuthz extends HiveClientImpl {
  /**
   * We need the process user HiveClient in order to get delegation token in reconnecting cases.
   */
  private final HiveClient processUserClient;
  private final UserGroupInformation ugiForRpc;
  private final String userName;
  private final boolean needDelegationToken;

  private HiveAuthorizationHelper authorizer;

  HiveClientWithAuthz(final HiveConf hiveConf, final UserGroupInformation ugiForRpc, final String userName,
                      final HiveClient processUserClient, final boolean needDelegationToken) {
    super(hiveConf);
    this.processUserClient = processUserClient;
    this.ugiForRpc = ugiForRpc;
    this.userName = userName;
    this.needDelegationToken = needDelegationToken;
  }

  @Override
  void connect() throws MetaException {
    doAsCommand(
      (PrivilegedExceptionAction<Void>) () -> {
        final HiveConf hiveConfCopy = new HiveConf(hiveConf);
        hiveConfCopy.set("user.name", userName);
        hiveConfCopy.set("proxy.user.name", userName);
        // skip registering Hive functions as this could be expensive, especially on Glue, and we don't have any
        // need for them
        client = Hive.getWithFastCheck(hiveConfCopy, false).getMSC();
        return null;
      },
        ugiForRpc,
        "Failed to connect to Hive metastore"
    );

    // Hive authorization helper needs the query user
    // (not the user from metastore client used to communicate with metastore)
    this.authorizer = new HiveAuthorizationHelper(client, hiveConf, userName);
  }

  @Override
  void reconnect() throws MetaException {
    if (needDelegationToken) {
      getAndSetDelegationToken(hiveConf, ugiForRpc, processUserClient);
    }
    doAsCommand(
      (PrivilegedExceptionAction<Void>) () -> {
        client.reconnect();
        return null;
      },
        ugiForRpc,
        "Failed to reconnect to Hive metastore"
    );
  }

  @Override
  public List<String> getDatabases(boolean ignoreAuthzErrors) throws TException {
    try {
      authorizer.authorizeShowDatabases();
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      if (ignoreAuthzErrors) {
        return Collections.emptyList();
      }
      throw UserException.permissionError(e).build(logger);
    }

    return super.getDatabases(ignoreAuthzErrors);
  }

  @Override
  public void checkState(boolean ignoreAuthzErrors) throws TException {
    try {
      authorizer.authorizeShowDatabases();
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      if (!ignoreAuthzErrors) {
        throw UserException.permissionError(e).build(logger);
      }
    }
    super.checkState(ignoreAuthzErrors);
  }

  @Override
  public List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException {
    try {
      authorizer.authorizeShowTables(dbName);
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      if (ignoreAuthzErrors) {
        return Collections.emptyList();
      }
      throw UserException.permissionError(e).build(logger);
    }

    return super.getTableNames(dbName, ignoreAuthzErrors);
  }

  @Override
  public Table getTable(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException {
    try {
      authorizer.authorizeReadTable(dbName, tableName);
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      if (!ignoreAuthzErrors) {
        throw UserException.permissionError(e).build(logger);
      } else {
        return null;
      }
    }
    return super.getTable(dbName, tableName, ignoreAuthzErrors);
  }


  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    try {
      authorizer.authorizeCreateTable(tbl.getDbName(), tbl.getTableName());
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      throw UserException.permissionError(e).build(logger);
    }
    super.createTable(tbl);

  }


  @Override
  public void dropTable(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException {
    try {
      authorizer.authorizeDropTable(dbName, tableName);
    } catch (final HiveAccessControlException | HiveAuthzPluginException e) {
      if (!ignoreAuthzErrors) {
        throw UserException.permissionError(e).build(logger);
      } else {
        return;
      }
    }
    super.dropTable(dbName, tableName, ignoreAuthzErrors);
  }

  @Override
  public List<HivePrivilegeObject> getRowFilterAndColumnMasking(
      List<HivePrivilegeObject> inputHiveObjects) throws SemanticException {
    if(authorizer.isAuthEnabled()) {
      HiveAuthzContext.Builder contextBuilder = new HiveAuthzContext.Builder();
      contextBuilder.setUserIpAddress("");
      contextBuilder.setCommandString("QUERY");

      return authorizer.getAuthorizer().applyRowFilterAndColumnMasking(contextBuilder.build(), inputHiveObjects);
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void checkDmlPrivileges(String dbName, String tableName, List<HivePrivObjectActionType> actionTypes) {
    try {
      authorizer.authorizeDml(dbName, tableName, actionTypes);
    } catch (HiveAccessControlException | HiveAuthzPluginException e) {
      throw UserException.permissionError(e).buildSilently();
    }
  }

  @Override
  public void checkCreateTablePrivileges(String dbName, String tableName) {
    try {
      authorizer.authorizeCreateTable(dbName, tableName);
    } catch (HiveAccessControlException | HiveAuthzPluginException e) {
      throw UserException.permissionError(e).buildSilently();
    }
  }

  @Override
  public void checkTruncateTablePrivileges(String dbName, String tableName) {
    try {
      authorizer.authorizeTruncateTable(dbName, tableName);
    } catch (HiveAccessControlException | HiveAuthzPluginException e) {
      throw UserException.permissionError(e).buildSilently();
    }
  }

  @Override
  public void checkAlterTablePrivileges(String dbName, String tableName) {
    try {
      authorizer.authorizeAlterTable(dbName, tableName);
    } catch (HiveAccessControlException | HiveAuthzPluginException e) {
      throw UserException.permissionError(e).buildSilently();
    }
  }
}
