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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.hive.thrift.TException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Wrapper around HiveMetaStoreClient to provide additional capabilities such as caching, reconnecting with user
 * credentials and higher level APIs to get the metadata in form that Dremio needs directly.
 */
class HiveClientImpl implements HiveClient {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveClientImpl.class);

  final HiveConf hiveConf;

  IMetaStoreClient client;

  /**
   * Create a HiveMetaStoreClient for cases where:
   *   1. Impersonation is enabled and
   *   2. either storage (in remote HiveMetaStore server) or SQL standard based authorization (in Hive storage plugin)
   *      is enabled
   * @param processUserMetaStoreClient MetaStoreClient of process user. Useful for generating the delegation tokens when
   *                                   SASL (KERBEROS or custom SASL implementations) is enabled.
   * @param hiveConf Conf including authorization configuration
   * @param userName User who is trying to access the Hive metadata
   * @param ugiForRpc The user context for executing RPCs.
   * @return A connected and authorized HiveClient.
   * @throws MetaException
   */
  static HiveClient createConnectedClientWithAuthz(final HiveClient processUserMetaStoreClient,
      final HiveConf hiveConf, final String userName, final UserGroupInformation ugiForRpc) throws MetaException {

    try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      HiveConf hiveConfForClient = hiveConf;
      boolean needDelegationToken = false;
      final boolean impersonationEnabled = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      String connectionUserName = HiveImpersonationUtil.resolveUserName(userName);

      if (impersonationEnabled && hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
        // When SASL is enabled for proxy user create a delegation token. Currently HiveMetaStoreClient can create
        // client transport for proxy users only when the authentication mechanims is DIGEST (through use of
        // delegation tokens).
        hiveConfForClient = new HiveConf(hiveConf);
        getAndSetDelegationToken(hiveConfForClient, ugiForRpc, processUserMetaStoreClient);
        needDelegationToken = true;
      }

      if (impersonationEnabled) {
        // if impersonation is enabled, use the UGI username as a connection username
        connectionUserName = ugiForRpc.getUserName();
      }

      final HiveClientImpl client = new HiveClientWithAuthz(hiveConfForClient, ugiForRpc,
        connectionUserName, processUserMetaStoreClient, needDelegationToken);
      client.connect();
      return client;
    } catch (RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException("Failure setting up HiveMetaStore client.", e);
    }
  }

  /**
   * Helper method that gets the delegation token using <i>processHiveClient</i> for given <i>proxyUserName</i>
   * and sets it in proxy user UserGroupInformation and proxy user HiveConf.
   */
  static void getAndSetDelegationToken(final HiveConf proxyUserHiveConf, final UserGroupInformation proxyUGI,
      final HiveClient processHiveClient) {
    checkNotNull(processHiveClient, "process user Hive client required");
    checkNotNull(proxyUserHiveConf, "Proxy user HiveConf required");
    checkNotNull(proxyUGI, "Proxy user UserGroupInformation required");

    try {
      final String delegationToken = processHiveClient.getDelegationToken(proxyUGI.getUserName());
      Utils.setTokenStr(proxyUGI, delegationToken, "DremioDelegationTokenForHiveMetaStoreServer");
      proxyUserHiveConf.set("hive.metastore.token.signature", "DremioDelegationTokenForHiveMetaStoreServer");
    } catch (Exception e) {
      final String processUsername = HiveImpersonationUtil.getProcessUserUGI().getShortUserName();
      throw UserException.permissionError(e)
          .message("Failed to generate Hive metastore delegation token for user %s. " +
              "Check Hadoop services (including metastore) have correct proxy user impersonation settings (%s, %s) " +
                  "and services are restarted after applying those settings.",
              proxyUGI.getUserName(),
              String.format("hadoop.proxyuser.%s.hosts", processUsername),
              String.format("hadoop.proxyuser.%s.groups", processUsername)
          )
          .addContext("Proxy user", proxyUGI.getUserName())
          .build(logger);
    }
  }

  /**
   * Create a DrillMetaStoreClient that can be shared across multiple users. This is created when impersonation is
   * disabled.
   *
   * @param hiveConf
   * @return
   * @throws MetaException
   */
  static HiveClient createConnectedClient(final HiveConf hiveConf) throws MetaException {
    final HiveClientImpl hiveClient = new HiveClientImpl(hiveConf);
    hiveClient.connect();
    return hiveClient;
  }

  HiveClientImpl(final HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  void connect() throws MetaException {
    Preconditions.checkState(this.client == null,
        "Already connected. If need to reconnect use reconnect() method.");
    reloginExpiringKeytabUser();

    try {
      doAsCommand(
        (PrivilegedExceptionAction<Void>) () -> {
          try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
            // skip registering Hive functions as this could be expensive, especially on Glue, and we don't have any
            // need for them
            client = Hive.getWithFastCheck(hiveConf, false).getMSC();
          }
          return null;
        },
          HiveImpersonationUtil.getProcessUserUGI(),
          "Failed to connect to Hive Metastore"
      );
    } catch (UndeclaredThrowableException e) {
      // If an exception is thrown from doAsCommand() above (internally in UserGroupInformation#doAs), it will get
      // wrapped as an UndeclaredThrowableException. We want to identify and rethrow MetaExceptions that have occurred.
      Throwables.propagateIfInstanceOf(e.getUndeclaredThrowable(), MetaException.class);
      throw e;
    }
  }

  @Override
  public List<String> getDatabases(boolean ignoreAuthzErrors) throws TException{
    return doCommand((RetryableClientCommand<List<String>>) client -> client.getAllDatabases());
  }

  @Override
  public boolean databaseExists(final String dbName) {
    try {
      return doCommand(client -> client.getDatabase(dbName) != null);
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      logger.info("Failure while trying to read database '{}'", dbName, e);
      return false;
    }
  }

  @Override
  public String getDatabaseLocationUri(final String dbName) {
    try {
      return doCommand(client -> Optional.of(client.getDatabase(dbName)).map(db -> db.getLocationUri()).orElse(null));
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      logger.info("Failure while trying to read database location uri '{}'", dbName, e);
      return null;
    }
  }

  @Override
  public List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException{
    return doCommand((RetryableClientCommand<List<String>>) client -> client.getAllTables(dbName));
  }

  @Override
  public boolean tableExists(final String dbName, final String tableName) {
    try {
      return (getTableWithoutTableTypeChecking(dbName, tableName, true) != null);
    } catch (TException te) {
      logger.info("Failure while trying to read table '{}' from db '{}'", tableName, dbName, te);
      return false;
    }
  }

  private Table getTableWithoutTableTypeChecking(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException{
    return doCommand((RetryableClientCommand<Table>) client -> {
      try{
        return client.getTable(dbName, tableName);
      }catch(NoSuchObjectException e){
        return null;
      }
    });
  }

  @Override
  public Table getTable(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException{

    Table table = getTableWithoutTableTypeChecking(dbName, tableName, ignoreAuthzErrors);

    if(table == null){
      return null;
    }

    TableType type = TableType.valueOf(table.getTableType());
    switch (type) {
      case EXTERNAL_TABLE:
      case MANAGED_TABLE:
        return table;

      case VIRTUAL_VIEW:
        throw UserException.unsupportedError().message("Hive views are not supported").buildSilently();
      case INDEX_TABLE:
      default:
        return null;
    }
  }


  @Override
  public void dropTable(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException {
    doCommand((RetryableClientCommand<Table>) client -> {
      try {
        client.dropTable(dbName, tableName, true, false, false);
      } catch (NoSuchObjectException | UnknownTableException e) {
        logger.warn("Database '{}', table '{}', dropTable failed since the table doesn't exist", dbName, tableName);
        throw e;
      }
      return null;
    });
  }

  @Override
  public List<Partition> getPartitionsByName(final String dbName, final String tableName, final List<String> partitionNames) throws TException {
    return doCommand(client -> {
      logger.trace("Database '{}', table '{}', Begin retrieval of partitions by name using batch size '{}'", dbName, tableName, partitionNames.size());

      try {
        final List<Partition> partitions = client.getPartitionsByNames(dbName, tableName, partitionNames);

        if (null == partitions) {
          throw UserException
            .connectionError()
            .message("Database '%s', table '%s', No partitions for table.", dbName, tableName)
            .build(logger);
        }

        logger.debug("Database '{}', table '{}', Retrieved partition count: '{}'", dbName, tableName, partitions.size());

        return partitions;
      } catch (TException e) {
        logger
          .error(
            "Database '{}', table '{}', Failure reading partitions by names: '{}'",
            dbName, tableName, Joiner.on(",").join(partitionNames), e);
        throw e;
      }
    });
  }

  @Override
  public List<String> getPartitionNames(final String dbName, final String tableName) throws TException {
    return doCommand(client -> {
      try {
        final List<String> allPartitionNames = client.listPartitionNames(dbName, tableName, (short) -1);

        if (null == allPartitionNames) {
          logger.debug("Database '{}', table '{}', No partition names for table.", dbName, tableName);
          return Collections.emptyList();
        }

        return allPartitionNames;
      } catch (TException e) {
        logger
          .error(
            "Database '{}', table '{}', Failure reading partition names.",
            dbName, tableName, e);
        throw e;
      }
    });
  }

  @Override
  public String getDelegationToken(final String proxyUser) throws TException {
    return doCommand((RetryableClientCommand<String>)
      client -> client.getDelegationToken(proxyUser, HiveImpersonationUtil.getProcessUserName()));
  }

  @Override
  public List<HivePrivilegeObject> getRowFilterAndColumnMasking(
    List<HivePrivilegeObject> inputHiveObjects) throws SemanticException {
    return Collections.emptyList();
  }

  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    doCommand((RetryableClientCommand<Void>) client -> {
      client.createTable(tbl);
      return null;
    });
  }

  @Override
  public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
    return doCommand((RetryableClientCommand<LockResponse>)
      client -> client.lock(request));
  }

  @Override
  public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
    doCommand((RetryableClientCommand<Void>) client -> {
      client.unlock(lockid);
      return null;
    });
  }

  @Override
  public LockResponse checkLock(long lockid) throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return doCommand((RetryableClientCommand<LockResponse>)
      client -> client.checkLock(lockid));
  }

  private interface RetryableClientCommand<T> {
    T run(IMetaStoreClient client) throws TException;
  }

  private synchronized <T> T doCommand(RetryableClientCommand<T> cmd) throws TException{
    T value;

    try {
      // Hive client can not be used for multiple requests at the same time.
      try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        value = cmd.run(client);
      }
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (TException e) {
      logger.warn("Failure to run Hive command. Will retry once. ", e);
      try {
        client.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      reconnect();

      try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
        value = cmd.run(client);
      }
    }

    return value;
  }

  void reconnect() throws MetaException{
    reloginExpiringKeytabUser();
    doAsCommand(
      (PrivilegedExceptionAction<Void>) () -> {
        try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
          client.reconnect();
        }
        return null;
      },
        HiveImpersonationUtil.getProcessUserUGI(),
        "Failed to reconnect to Hive metastore"
    );
  }

  private void reloginExpiringKeytabUser() throws MetaException {
    if(UserGroupInformation.isSecurityEnabled()) {
      // renew the TGT if required
      try {
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        if (ugi.isFromKeytab()) {
          ugi.checkTGTAndReloginFromKeytab();
        }
      } catch (IOException e) {
        final String msg = "Error doing relogin using keytab " + e.getMessage();
        logger.error(msg, e);
        throw new MetaException(msg);
      }
    }
  }

  @Override
  public void close() {
    try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      client.close();
    }
  }

  <T> T doAsCommand(final PrivilegedExceptionAction<T> cmd, UserGroupInformation ugi, String errMsg) {
    checkNotNull(ugi, "UserGroupInformation object required");
    try {
      return ugi.doAs((PrivilegedExceptionAction<T>) () -> {
        try(Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
          return cmd.run();
        }
      });
    } catch (final InterruptedException | IOException e) {
      throw new RuntimeException(String.format("%s, doAs User: %s", errMsg, ugi.getUserName()), e);
    }
  }

  @Override
  public IMetaStoreClient getMetastoreClient() {
    return this.client;
  }

  @Override
  public void checkDmlPrivileges(String dbName, String tableName, List<HivePrivObjectActionType> actionTypes) {
    // do nothing - HiveClientWithAuthz overrides this to check based on user
  }

  @Override
  public void checkCreateTablePrivileges(String dbName, String tableName) {
    // do nothing - HiveClientWithAuthz overrides this to check based on user
  }

  @Override
  public void checkTruncateTablePrivileges(String dbName, String tableName) {
    // do nothing - HiveClientWithAuthz overrides this to check based on user
  }

  @Override
  public void checkAlterTablePrivileges(String dbName, String tableName) {
    // do nothing - HiveClientWithAuthz overrides this to check based on user
  }
}
