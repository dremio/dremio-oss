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
package com.dremio.exec.store.hive.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.io.file.UriSchemes;
import com.google.common.cache.LoadingCache;

public class TestHadoopFsCacheWrapperPluginClassLoader {

  private HadoopFsCacheWrapperPluginClassLoader cacheWrapper;

  private static final String HOST = "localhost";
  private static final String PATH = "/sample/data";

  @Before
  public void before() throws Exception {
    cacheWrapper = new HadoopFsCacheWrapperPluginClassLoader();
    for (int i = 0; i < 10; i++) {
      URI uri = URI.create(String.format("%s://%s%s_%s", UriSchemes.HDFS_SCHEME, HOST, PATH, i));
      FileSystem unused = cacheWrapper.getHadoopFsSupplierPluginClassLoader(uri.toString(), new JobConf(), "dremio").get();
    }
  }

  @After
  public void after() throws Exception {
    cacheWrapper.close();
    cacheWrapper = null;
  }

  @Test
  public void testExactMatch() {
    LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache = cacheWrapper.getCache();
    long initialCacheSize = cache.size();
    URI uri = URI.create(String.format("%s://%s%s", UriSchemes.HDFS_SCHEME, HOST, PATH));
    String userName = "dremio";
    FileSystem fs = cacheWrapper.getHadoopFsSupplierPluginClassLoader(uri.toString(), new JobConf(), userName).get();
    assertNotNull("File system instance must be not null", fs);
    assertEquals(String.format("File system scheme must be %s", UriSchemes.HDFS_SCHEME), fs.getScheme(), UriSchemes.HDFS_SCHEME);
    assertEquals("Cache must contain exactly one element", cache.size(), initialCacheSize);
    assertTrue("All elements in the cache must have the same scheme, authority, and ugi", cache.asMap().keySet().stream().allMatch(k -> k.scheme.equals(uri.getScheme()) && k.authority.equals(uri.getAuthority()) && k.userName.equals(userName)));
  }

  @Test
  public void testSchemaMismatch() {
    LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache = cacheWrapper.getCache();
    long initialCacheSize = cache.size();
    URI uri = URI.create(String.format("%s://%s%s", UriSchemes.FILE_SCHEME, HOST, PATH));
    String userName = "dremio";
    FileSystem fs = cacheWrapper.getHadoopFsSupplierPluginClassLoader(uri.toString(), new JobConf(), userName).get();
    assertNotNull("File system instance must be not null", fs);
    assertEquals(String.format("File system scheme must be %s", UriSchemes.FILE_SCHEME), fs.getScheme(), UriSchemes.FILE_SCHEME);
    assertEquals("Cache must contain exactly two elements", cache.size(), initialCacheSize + 1);
    assertTrue("All elements in the cache must have the same authority and ugi", cache.asMap().keySet().stream().allMatch(k -> k.authority.equals(uri.getAuthority()) && k.userName.equals(userName)));
    assertTrue(String.format("One element in the cache must have \"%s\" schema", UriSchemes.FILE_SCHEME), cache.asMap().keySet().stream().anyMatch(k -> k.scheme.equals(uri.getScheme())));
  }

  @Test
  public void testAuthorityMismatch() {
    LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache = cacheWrapper.getCache();
    long initialCacheSize = cache.size();
    String host = "175.23.2.71";
    URI uri = URI.create(String.format("%s://%s%s", UriSchemes.HDFS_SCHEME, host, PATH));
    String userName = "dremio";
    FileSystem fs = cacheWrapper.getHadoopFsSupplierPluginClassLoader(uri.toString(), new JobConf(), userName).get();
    assertNotNull("File system instance must be not null", fs);
    assertEquals(String.format("File system scheme must be %s", UriSchemes.HDFS_SCHEME), fs.getScheme(), UriSchemes.HDFS_SCHEME);
    assertEquals("Cache must contain exactly two elements", cache.size(), initialCacheSize + 1);
    assertTrue("All elements in the cache must have the same scheme and ugi", cache.asMap().keySet().stream().allMatch(k -> k.scheme.equals(uri.getScheme()) && k.userName.equals(userName)));
    assertTrue(String.format("One element in the cache must have \"%s\" authority", host), cache.asMap().keySet().stream().anyMatch(k -> k.authority.equals(uri.getAuthority())));
  }

  @Test
  public void testUserMismatch() {
    LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache = cacheWrapper.getCache();
    long initialCacheSize = cache.size();
    URI uri = URI.create(String.format("%s://%s%s", UriSchemes.HDFS_SCHEME, HOST, PATH));
    String userName = "testUser";
    FileSystem fs = cacheWrapper.getHadoopFsSupplierPluginClassLoader(uri.toString(), new JobConf(), userName).get();
    assertNotNull("File system instance must be not null", fs);
    assertEquals(String.format("File system scheme must be %s", UriSchemes.HDFS_SCHEME), fs.getScheme(), UriSchemes.HDFS_SCHEME);
    assertEquals("Cache must contain exactly two elements", cache.size(), initialCacheSize + 1);
    assertTrue("All elements in the cache must have the same scheme and authority", cache.asMap().keySet().stream().allMatch(k -> k.scheme.equals(uri.getScheme()) && k.authority.equals(uri.getAuthority())));
    assertTrue(String.format("One element in the cache must have \"%s\" username", userName), cache.asMap().keySet().stream().anyMatch(k -> k.userName.equals(userName)));
  }

  @Test
  public void testClose() throws Exception {
    LoadingCache<HadoopFsCacheKeyPluginClassLoader, FileSystem> cache = cacheWrapper.getCache();
    cacheWrapper.close();
    assertEquals("Cache must be empty", 0, cache.size());
  }

}
