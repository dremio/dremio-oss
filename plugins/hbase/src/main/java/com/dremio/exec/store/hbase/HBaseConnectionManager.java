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
package com.dremio.exec.store.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.util.ImpersonationUtil;

/**
 * A Connection implementation that supports quick connection validation and renewing connection as necessary.
 */
public class HBaseConnectionManager implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseConnectionManager.class);

  private final Configuration config;
  private volatile Connection connection;

  public HBaseConnectionManager(Configuration config) {
    this.config = config;
  }

  public void validate() {
    final Configuration testConfig = new Configuration(config);
    testConfig.set("hbase.client.retries.number", "3");
    testConfig.set("hbase.client.pause", "1000");
    testConfig.set("zookeeper.recovery.retry", "1");

    // make sure we can create a connection.
    doAsCommand(
      new UGIDoAsCommand<Void>() {
        @Override
        public Void run() throws Exception {
          try(
            Connection c = ConnectionFactory.createConnection(testConfig);
            Admin admin = c.getAdmin();
          ) {
            admin.listNamespaceDescriptors();
          }catch(IOException ex) {
            throw UserException.dataReadError(ex).message("Failure while connecting to HBase.").build(logger);
          }
          return null;
        }
      },
      ImpersonationUtil.getProcessUserUGI(),
      "Failed to connect to HBase"
    );
  }

  public Connection getConnection() {
    if(true) {
      // DX-9766: Kept here for testing.
      // return HBaseConnectionManagerLegacy.getConnection(pluginId);
    }

    final Connection connection = this.connection;
    if(isValid(connection)) {
      return new ManagedConnection(connection);
    }

    return new ManagedConnection(newConnectionIfNotValid());
  }

  private synchronized Connection newConnectionIfNotValid() {
    if(isValid(connection)) {
      return connection;
    }

    try {
      close();
    } catch(IOException ex) {
      logger.warn("Failure while closing connection: {}.", connection, ex);
    }

    logger.info("No valid HBase connection available, creating one.");

    doAsCommand(
      new UGIDoAsCommand<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            connection = ConnectionFactory.createConnection(config);
            logger.info("Connection created: {}.", connection);
          } catch (IOException ex) {
            throw UserException.dataReadError(ex).message("Failure while connecting to HBase.").build(logger);
          }
          return null;
        }
      },
      ImpersonationUtil.getProcessUserUGI(),
        "Failed to connect to HBase"
    );
    return this.connection;
  }

  private static boolean isValid(Connection conn) {
    return conn != null
        && !conn.isAborted()
        && !conn.isClosed();
  }

  public synchronized void close() throws IOException {
    if(connection != null) {
      connection.close();
      connection = null;
    }
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

  /**
   * Managed connection that doesn't allow consumers to close this connection.
   */
  private static class ManagedConnection implements Connection {

    private final Connection delegate;

    public ManagedConnection(Connection delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public void abort(String why, Throwable e) {
      delegate.abort(why, e);
    }

    @Override
    public boolean isAborted() {
      return delegate.isAborted();
    }

    @Override
    public Configuration getConfiguration() {
      return delegate.getConfiguration();
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
      return delegate.getTable(tableName);
    }

    @Override
    public Table getTable(TableName tableName, java.util.concurrent.ExecutorService pool) throws IOException {
      return delegate.getTable(tableName, pool);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
      return delegate.getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
      return delegate.getBufferedMutator(params);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
      return delegate.getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException {
      return delegate.getAdmin();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean isClosed() {
      return delegate.isClosed();
    }

  }
}
