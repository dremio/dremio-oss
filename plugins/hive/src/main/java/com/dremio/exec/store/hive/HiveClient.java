/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.hive;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.dremio.exec.util.ImpersonationUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Override HiveMetaStoreClient to provide additional capabilities such as caching, reconnecting with user
 * credentials and higher level APIs to get the metadata in form that Drill needs directly.
 */
public class HiveClient implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveClient.class);

  final HiveConf hiveConf;

  HiveMetaStoreClient client;

  /**
   * Create a DrillHiveMetaStoreClient for cases where:
   *   1. Drill impersonation is enabled and
   *   2. either storage (in remote HiveMetaStore server) or SQL standard based authorization (in Hive storage plugin)
   *      is enabled
   * @param processUserMetaStoreClient MetaStoreClient of process user. Useful for generating the delegation tokens when
   *                                   SASL (KERBEROS or custom SASL implementations) is enabled.
   * @param hiveConf Conf including authorization configuration
   * @param userName User who is trying to access the Hive metadata
   * @return
   * @throws MetaException
   */
  public static HiveClient createClientWithAuthz(final HiveClient processUserMetaStoreClient,
      final HiveConf hiveConf, final String userName) throws MetaException {
    try {
      HiveConf hiveConfForClient = hiveConf;
      final UserGroupInformation ugiForRpc; // UGI credentials to use for RPC communication with Hive MetaStore server
      boolean needDelegationToken = false;
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
        // If the user impersonation is disabled in Hive storage plugin (not Drill impersonation), use the process
        // user UGI credentials.
        ugiForRpc = ImpersonationUtil.getProcessUserUGI();
      } else {
        ugiForRpc = ImpersonationUtil.createProxyUgi(userName);
        if (hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
          // When SASL is enabled for proxy user create a delegation token. Currently HiveMetaStoreClient can create
          // client transport for proxy users only when the authentication mechanims is DIGEST (through use of
          // delegation tokens).
          hiveConfForClient = new HiveConf(hiveConf);
          getAndSetDelegationToken(hiveConfForClient, ugiForRpc, userName, processUserMetaStoreClient);
          needDelegationToken = true;
        }
      }

      final HiveClient client = new HiveClientWithAuthz(hiveConfForClient, ugiForRpc, userName,
          processUserMetaStoreClient, needDelegationToken);
      client.connect();
      return client;
    } catch (final Exception e) {
      throw new RuntimeException("Failure setting up HiveMetaStore client.", e);
    }
  }

  /**
   * Helper method that gets the delegation token using <i>processHiveClient</i> for given <i>proxyUserName</i>
   * and sets it in proxy user UserGroupInformation and proxy user HiveConf.
   */
  protected static void getAndSetDelegationToken(final HiveConf proxyUserHiveConf, final UserGroupInformation proxyUGI,
      final String proxyUserName, final HiveClient processHiveClient) {
    checkNotNull(processHiveClient, "process user Hive client required");
    checkNotNull(proxyUserHiveConf, "Proxy user HiveConf required");
    checkNotNull(proxyUGI, "Proxy user UserGroupInformation required");
    checkArgument(!Strings.isNullOrEmpty(proxyUserName), "valid proxy username required");

    try {
      final String delegationToken = processHiveClient.getDelegationToken(proxyUserName);
      Utils.setTokenStr(proxyUGI, delegationToken, "DremioDelegationTokenForHiveMetaStoreServer");
      proxyUserHiveConf.set("hive.metastore.token.signature", "DremioDelegationTokenForHiveMetaStoreServer");
    } catch (Exception e) {
      throw new RuntimeException("Couldn't generate Hive metastore delegation token for user " + proxyUserName);
    }
  }

  /**
   * Create a DrillMetaStoreClient that can be shared across multiple users. This is created when impersonation is
   * disabled.
   * @param hiveConf
   * @return
   * @throws MetaException
   */
  public static HiveClient createClient(final HiveConf hiveConf)
      throws MetaException {
    final HiveClient hiveClient = new HiveClient(hiveConf);
    hiveClient.connect();
    return hiveClient;
  }

  HiveClient(final HiveConf hiveConf) throws MetaException {
    this.hiveConf = hiveConf;
  }

  void connect() throws MetaException {
    Preconditions.checkState(this.client == null,
        "Already connected. If need to reconnect use reconnect() method.");
    reloginExpiringKeytabUser();
    doAsCommand(
        new UGIDoAsCommand<Void>() {
          @Override
          public Void run() throws Exception {
            client = new HiveMetaStoreClient(hiveConf);
            return null;
          }
        },
        ImpersonationUtil.getProcessUserUGI(),
        "Failed to connect to Hive Metastore"
    );
  }

  List<String> getDatabases(boolean ignoreAuthzErrors) throws TException{
    return doCommand(new RetryableClientCommand<List<String>>(){
      @Override
      public List<String> run(HiveMetaStoreClient client) throws TException {
        return client.getAllDatabases();
      }});
  }

  boolean databaseExists(final String dbName){
    try {
      return doCommand(new RetryableClientCommand<Boolean>(){
        @Override
        public Boolean run(HiveMetaStoreClient client) throws TException {
          return client.getDatabase(dbName) != null;
        }});
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      logger.info("Failure while trying to read database {}", dbName, e);
      return false;
    }
  }

  List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException{
    return doCommand(new RetryableClientCommand<List<String>>(){
      @Override
      public List<String> run(HiveMetaStoreClient client) throws TException {
        return client.getAllTables(dbName);
      }});
  }

  boolean tableExists(final String dbName, final String tableName) {
    try {
      return (getTable(dbName, tableName, true) != null);
    } catch (TException te) {
      logger.info("Failure while trying to read table {} from db {}", tableName, dbName, te);
      return false;
    }
  }

  Table getTable(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException{
    return doCommand(new RetryableClientCommand<Table>(){
      @Override
      public Table run(HiveMetaStoreClient client) throws TException {
        try{
          Table table = client.getTable(dbName, tableName);
          if(table == null){
            return null;
          }

          TableType type = TableType.valueOf(table.getTableType());
          switch(type){
          case EXTERNAL_TABLE:
          case MANAGED_TABLE:
            return table;

          case VIRTUAL_VIEW:
          case INDEX_TABLE:
          default:
            return null;
          }
        }catch(NoSuchObjectException e){
          return null;
        }
      }});
  }

  List<Partition> getPartitions(final String dbName, final String tableName) throws TException{
    return doCommand(new RetryableClientCommand<List<Partition>>(){
      @Override
      public List<Partition> run(HiveMetaStoreClient client) throws TException {
        return client.listPartitions(dbName, tableName, (short) -1);
      }});
  }

  String getDelegationToken(final String proxyUser) throws TException {
    return doCommand(new RetryableClientCommand<String>() {
      @Override
      public String run(HiveMetaStoreClient client) throws TException {
        return client.getDelegationToken(proxyUser, ImpersonationUtil.getProcessUserName());
      }
    });
  }

  private interface RetryableClientCommand<T> {
    T run(HiveMetaStoreClient client) throws TException;
  }

  private <T> T doCommand(RetryableClientCommand<T> cmd) throws TException{
    T value = null;
    try {
      // Hive client can not be used for multiple requests at the same time.
      synchronized (client) {
        value = cmd.run(client);
      }
    } catch (NoSuchObjectException | MetaException e) {
      throw e;
    } catch (TException e) {
      logger.warn("Failure to run Hive command. Will retry once. ", e);
      try {
        client.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      reconnect();
      value = cmd.run(client);
    }

    return value;
  }

  void reconnect() throws MetaException{
    reloginExpiringKeytabUser();
    doAsCommand(
        new UGIDoAsCommand<Void>() {
          @Override
          public Void run() throws Exception {
            client.reconnect();
            return null;
          }
        },
        ImpersonationUtil.getProcessUserUGI(),
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
    client.close();
  }

  protected interface UGIDoAsCommand<T> {
    T run() throws Exception;
  }

  protected<T> T doAsCommand(final UGIDoAsCommand<T> cmd, UserGroupInformation ugi, String errMsg) {
    checkNotNull(ugi, "UserGroupInformation object required");
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return cmd.run();
        }
      });
    } catch (final InterruptedException | IOException e) {
      throw new RuntimeException(String.format("%s, doAs User: %s", errMsg, ugi.getUserName()), e);
    }
  }
}
