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
package com.dremio.exec.store.dfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY;
import static org.apache.hadoop.fs.FileSystem.SHUTDOWN_HOOK_PRIORITY;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;

/**
 * Similar to the cache in {@link FileSystem} with addition of unique set of properties to cache
 * key.
 */
public class DremioFileSystemCache {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioFileSystemCache.class);
  private static final String disableDremioCacheName = "fs.impl.disable.dremio.cache";

  private final ClientFinalizer clientFinalizer = new ClientFinalizer();

  private final Map<Key, FileSystem> map = new HashMap<>();
  private final Set<Key> toAutoClose = new HashSet<>();

  public FileSystem get(URI uri, Configuration conf, List<String> uniqueConnectionProps)
      throws IOException {
    final Key key = new Key(uri, conf, uniqueConnectionProps);

    FileSystem fs;
    synchronized (this) {
      fs = map.get(key);
    }
    if (fs != null) {
      return fs;
    }

    final String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());

    // Clone the conf and set cache to disable, so that a new instance is created rather than
    // returning an existing
    // one in Hadoop's FileSystem cache. TODO: worry if cloning conf blows up heap memory. We could
    // use the existing
    // conf object but it is shared by muliple threads
    final Configuration cloneConf = new Configuration(conf);
    cloneConf.set(disableCacheName, "true");
    fs = FileSystem.get(uri, cloneConf);

    /** Check if user does not want to cache in Dremio cache */
    final boolean disableDremioCache = conf.getBoolean(disableDremioCacheName, false);
    if (disableDremioCache
        || key.uniqueConnectionPropValues == null
        || key.uniqueConnectionPropValues.isEmpty()) {
      return fs;
    }

    synchronized (this) { // refetch the lock again
      FileSystem oldfs = map.get(key);
      if (oldfs != null) { // a file system is created while lock is releasing
        fs.close(); // close the new file system
        return oldfs; // return the old file system
      }

      // now insert the new file system into the map
      if (map.isEmpty() && !ShutdownHookManager.get().isShutdownInProgress()) {
        ShutdownHookManager.get().addShutdownHook(clientFinalizer, SHUTDOWN_HOOK_PRIORITY);
      }
      map.put(key, fs);
      if (conf.getBoolean(FS_AUTOMATIC_CLOSE_KEY, FS_AUTOMATIC_CLOSE_DEFAULT)) {
        toAutoClose.add(key);
      }
      return fs;
    }
  }

  /**
   * Close all FileSystem instances in the Cache.
   *
   * @param onlyAutomatic only close those that are marked for automatic closing
   */
  public synchronized void closeAll(boolean onlyAutomatic) throws IOException {
    List<IOException> exceptions = new ArrayList<>();

    // Make a copy of the keys in the map since we'll be modifying
    // the map while iterating over it, which isn't safe.
    List<Key> keys = new ArrayList<Key>();
    keys.addAll(map.keySet());

    for (Key key : keys) {
      final FileSystem fs = map.get(key);

      if (onlyAutomatic && !toAutoClose.contains(key)) {
        continue;
      }

      // remove from cache
      map.remove(key);
      toAutoClose.remove(key);

      if (fs != null) {
        try {
          fs.close();
        } catch (IOException ioe) {
          exceptions.add(ioe);
        }
      }
    }

    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  private class ClientFinalizer implements Runnable {
    @Override
    public synchronized void run() {
      try {
        closeAll(true);
      } catch (IOException e) {
        logger.info("DremioFileSystemCache.closeAll() threw an exception\n", e);
      }
    }
  }

  /** Key */
  private static class Key {
    final String scheme;
    final String authority;
    final UserGroupInformation ugi;
    final List<String> uniqueConnectionPropValues;

    Key(URI uri, Configuration conf, List<String> uniqueConnectionProps) throws IOException {
      scheme = uri.getScheme() == null ? "" : StringUtils.toLowerCase(uri.getScheme());
      authority = uri.getAuthority() == null ? "" : StringUtils.toLowerCase(uri.getAuthority());

      if (uniqueConnectionProps == null) {
        uniqueConnectionPropValues = null;
      } else {
        uniqueConnectionPropValues = new ArrayList<>(uniqueConnectionProps.size());
        for (String prop : uniqueConnectionProps) {
          uniqueConnectionPropValues.add(conf.get(prop));
        }
      }

      ugi = UserGroupInformation.getCurrentUser();
    }

    @Override
    public int hashCode() {
      return Objects.hash(scheme, authority, ugi, uniqueConnectionPropValues);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return com.google.common.base.Objects.equal(scheme, key.scheme)
          && com.google.common.base.Objects.equal(authority, key.authority)
          && com.google.common.base.Objects.equal(ugi, key.ugi)
          && com.google.common.base.Objects.equal(
              uniqueConnectionPropValues, key.uniqueConnectionPropValues);
    }

    @Override
    public String toString() {
      return "("
          + ugi.toString()
          + ")@"
          + scheme
          + "://"
          + authority
          + "with ["
          + uniqueConnectionPropValues
          + "]";
    }
  }
}
