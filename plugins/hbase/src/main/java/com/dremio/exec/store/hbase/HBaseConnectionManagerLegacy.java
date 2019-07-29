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
package com.dremio.exec.store.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Kept until DX-9766 is resolved
 *
 * <p>A singleton class which manages the lifecycle of HBase connections.</p>
 * <p>One connection per storage plugin instance is maintained.</p>
 */
@Deprecated
public final class HBaseConnectionManagerLegacy {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseConnectionManager.class);

  public static final HBaseConnectionManagerLegacy INSTANCE = new HBaseConnectionManagerLegacy();

  private final LoadingCache<HBaseConnectionKey, Connection> connectionCache;

  private HBaseConnectionManagerLegacy() {
    LoaderListener ll = new LoaderListener();
    this.connectionCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS) // Connections will be closed after 1 hour of inactivity
        .removalListener(ll)
        .build(ll);
  }

  private boolean isValid(Connection conn) {
    return conn != null
        && !conn.isAborted()
        && !conn.isClosed();
  }

  private class LoaderListener extends CacheLoader<HBaseConnectionKey, Connection> implements RemovalListener<HBaseConnectionKey, Connection> {
    @Override
    public Connection load(HBaseConnectionKey key) throws Exception {
      Connection connection = ConnectionFactory.createConnection(key.getHBaseConf());
      logger.info("HBase connection '{}' created.", connection);
      return connection;
    }

    @Override
    public void onRemoval(RemovalNotification<HBaseConnectionKey, Connection> notification) {
      try {
        Connection conn = notification.getValue();
        if (isValid(conn)) {
          conn.close();
        }
        logger.info("HBase connection '{}' closed.", conn);
      } catch (Throwable t) {
        logger.warn("Error while closing HBase connection.", t);
      }
    }
  }

  public static Connection getConnection(StoragePluginId pluginId) {
    return INSTANCE.getConnection(new HBaseConnectionKey(pluginId));
  }

  public Connection getConnection(HBaseConnectionKey key) {
    checkNotNull(key);
    try {
      Connection conn = connectionCache.get(key);
      if (!isValid(conn)) {
        key.lock(); // invalidate the connection with a per storage plugin lock
        try {
          conn = connectionCache.get(key);
          if (!isValid(conn)) {
            connectionCache.invalidate(key);
            conn = connectionCache.get(key);
          }
        } finally {
          key.unlock();
        }
      }
      return conn;
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw UserException.dataReadError(e.getCause()).build(logger);
    }
  }

  public void closeConnection(HBaseConnectionKey key) {
    checkNotNull(key);
    connectionCache.invalidate(key);
  }

  /**
   * An internal class which serves the key in a map of {@link HBaseStoragePlugin} => {@link Connection}.
   */
  static class HBaseConnectionKey {

    private final ReentrantLock lock = new ReentrantLock();
    private final StoragePluginId pluginId;

    public HBaseConnectionKey(StoragePluginId pluginId) {
      super();
      this.pluginId = pluginId;
    }

    public void lock() {
      lock.lock();
    }

    public void unlock() {
      lock.unlock();
    }

    public Configuration getHBaseConf() {
      HBaseConf config = pluginId.getConnectionConf();
      return config.getHBaseConf();
    }

    @Override
    public int hashCode() {
      return pluginId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() != obj.getClass()) {
        return false;
      }

      return pluginId.equals(((HBaseConnectionKey) obj).pluginId);
    }

  }


}
