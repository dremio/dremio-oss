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
package com.dremio.plugins.icebergcatalog.dfs;

import static com.dremio.io.file.UriSchemes.DREMIO_AZURE_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_GCS_SCHEME;
import static com.dremio.io.file.UriSchemes.DREMIO_S3_SCHEME;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import com.dremio.io.file.UriSchemes;
import com.dremio.options.OptionManager;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestHadoopFileSystemCache {

  @Mock private LoadingCache<HadoopFileSystemCacheKey, FileSystem> mockCache;

  @Mock private OptionManager mockOptionManager;

  private HadoopFileSystemCache fsCache;

  @Before
  public void setup() {
    fsCache = new HadoopFileSystemCache(mockCache);
  }

  @After
  public void teardown() {
    fsCache = null;
  }

  @Test
  public void testCacheRemovalListener() throws IOException {
    fsCache = new HadoopFileSystemCache(mockOptionManager);
    LoadingCache<HadoopFileSystemCacheKey, FileSystem> cache = fsCache.getCache();
    FileSystem mockFileSystem = mock(FileSystem.class);
    doThrow(new IOException()).when(mockFileSystem).close();
    URI uri =
        URI.create(
            String.format(
                "%s://%s%s_%s", UriSchemes.HDFS_SCHEME, "hostname", "path", "testCacheOnRemoval"));
    HadoopFileSystemCacheKey hadoopFileSystemCacheKey =
        new HadoopFileSystemCacheKey(uri, new JobConf(), "dremio");
    cache.put(hadoopFileSystemCacheKey, mockFileSystem);
    cache.invalidate(hadoopFileSystemCacheKey);
    verify(mockFileSystem, times(1)).close();
  }

  @Test
  public void testBuildCacheKeyWithSystemUsername() throws IOException {
    fsCache = new HadoopFileSystemCache(mockOptionManager);
    LoadingCache<HadoopFileSystemCacheKey, FileSystem> cache = fsCache.getCache();
    URI uri = URI.create(String.format("%s://%s", UriSchemes.HDFS_SCHEME, "incomplete_hdfs_uri"));
    HadoopFileSystemCacheKey hadoopFileSystemCacheKey =
        new HadoopFileSystemCacheKey(uri, new JobConf(), SYSTEM_USERNAME);
    assertThatThrownBy(() -> cache.get(hadoopFileSystemCacheKey))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "java.io.IOException: Incomplete HDFS URI, no host: hdfs://incomplete_hdfs_uri");
  }

  @Test
  public void testBuildCacheKeyWithLoginUsername() throws IOException {
    fsCache = new HadoopFileSystemCache(mockOptionManager);
    LoadingCache<HadoopFileSystemCacheKey, FileSystem> cache = fsCache.getCache();
    URI uri = URI.create(String.format("%s://%s", UriSchemes.HDFS_SCHEME, "incomplete_hdfs_uri"));
    HadoopFileSystemCacheKey hadoopFileSystemCacheKey =
        new HadoopFileSystemCacheKey(
            uri, new JobConf(), UserGroupInformation.getLoginUser().getUserName());
    assertThatThrownBy(() -> cache.get(hadoopFileSystemCacheKey))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "java.io.IOException: Incomplete HDFS URI, no host: hdfs://incomplete_hdfs_uri");
  }

  @Test
  public void testLoadFile() {
    fsCache = new HadoopFileSystemCache(mockOptionManager);
    FileSystem fsNullScheme = fsCache.load("/path/to", new Configuration(), SYSTEM_USERNAME);
    assertTrue(fsNullScheme instanceof LocalFileSystem);
    FileSystem fsFileScheme = fsCache.load("file:///path/to", new Configuration(), SYSTEM_USERNAME);
    assertTrue(fsFileScheme instanceof LocalFileSystem);
  }

  @Test
  public void testLoadS3() {
    when(mockCache.get(any())).thenReturn(null);
    try (MockedStatic<FileSystemConfUtil> mockFsConfUtil = mockStatic(FileSystemConfUtil.class)) {
      fsCache.load("s3:///path/to", new Configuration(), SYSTEM_USERNAME);
      ArgumentCaptor<URI> argument = ArgumentCaptor.forClass(URI.class);
      mockFsConfUtil.verify(
          () -> FileSystemConfUtil.initializeConfiguration(argument.capture(), any()));
      assertEquals(DREMIO_S3_SCHEME, argument.getValue().getScheme());
    }
  }

  @Test
  public void testLoadAzure() {
    when(mockCache.get(any())).thenReturn(null);
    try (MockedStatic<FileSystemConfUtil> mockFsConfUtil = mockStatic(FileSystemConfUtil.class)) {
      fsCache.load("abfs://authority/path/to", new Configuration(), SYSTEM_USERNAME);
      ArgumentCaptor<URI> argument = ArgumentCaptor.forClass(URI.class);
      mockFsConfUtil.verify(
          () -> FileSystemConfUtil.initializeConfiguration(argument.capture(), any()));
      assertEquals(DREMIO_AZURE_SCHEME, argument.getValue().getScheme());
    }
  }

  @Test
  public void testLoadGCS() {
    when(mockCache.get(any())).thenReturn(null);
    try (MockedStatic<FileSystemConfUtil> mockFsConfUtil = mockStatic(FileSystemConfUtil.class)) {
      fsCache.load("gs://authority/path/to?query#fragment", new Configuration(), SYSTEM_USERNAME);
      ArgumentCaptor<URI> argument = ArgumentCaptor.forClass(URI.class);
      mockFsConfUtil.verify(
          () -> FileSystemConfUtil.initializeConfiguration(argument.capture(), any()));
      assertEquals(DREMIO_GCS_SCHEME, argument.getValue().getScheme());
    }
  }

  @Test
  public void testLoadHdfs() {
    when(mockCache.get(any())).thenReturn(null);
    try (MockedStatic<FileSystemConfUtil> mockFsConfUtil = mockStatic(FileSystemConfUtil.class)) {
      fsCache.load(
          "hdfs:///authority/path/to?query#fragment", new Configuration(), SYSTEM_USERNAME);
      ArgumentCaptor<URI> argument = ArgumentCaptor.forClass(URI.class);
      mockFsConfUtil.verify(
          () -> FileSystemConfUtil.initializeConfiguration(argument.capture(), any()));
      assertEquals("hdfs", argument.getValue().getScheme());
    }
  }

  @Test
  public void testLoadIOException() {
    try (MockedStatic<FileSystemConfUtil> mockFsConfUtil = mockStatic(FileSystemConfUtil.class)) {
      mockFsConfUtil
          .when(() -> FileSystemConfUtil.initializeConfiguration(any(), any()))
          .thenThrow(IOException.class);
      assertThatThrownBy(
              () ->
                  fsCache.load(
                      "gs://authority/path/to?query#fragment",
                      new Configuration(),
                      SYSTEM_USERNAME))
          .isInstanceOf(UserException.class);
    }
  }

  @Test
  public void testClose() throws Exception {
    fsCache.close();
    verify(mockCache, times(1)).invalidateAll();
    verify(mockCache, times(1)).cleanUp();
  }
}
