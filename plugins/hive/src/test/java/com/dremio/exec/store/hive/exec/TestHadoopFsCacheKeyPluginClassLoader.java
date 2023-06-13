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
import static org.junit.Assert.assertNotEquals;

import java.net.URI;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.dremio.io.file.UriSchemes;

public class TestHadoopFsCacheKeyPluginClassLoader {

  @Test
  public void testEquals() {
    URI uri = URI.create(String.format("%s://%s%s", UriSchemes.HDFS_SCHEME, "localhost", "/sample/data"));
    String userName = "dremio";
    HadoopFsCacheKeyPluginClassLoader key1 = new HadoopFsCacheKeyPluginClassLoader(uri, new JobConf(), userName);
    HadoopFsCacheKeyPluginClassLoader key2 = new HadoopFsCacheKeyPluginClassLoader(uri, new JobConf(), userName);
    assertEquals(key1, key2);
  }

  @Test
  public void testNotEquals() {
    String host1 = "localhost";
    String path1 = "/sample/data";
    URI uri1 = URI.create(String.format("%s://%s%s", UriSchemes.HDFS_SCHEME, host1, path1));
    URI uri2 = URI.create(String.format("%s://%s%s", UriSchemes.FILE_SCHEME, host1, path1));
    String userName1 = "dremio1";
    HadoopFsCacheKeyPluginClassLoader key1 = new HadoopFsCacheKeyPluginClassLoader(uri1, new JobConf(), userName1);
    HadoopFsCacheKeyPluginClassLoader key2 = new HadoopFsCacheKeyPluginClassLoader(uri2, new JobConf(), userName1);
    assertNotEquals(key1, key2);

    String host2 = "175.23.2.71";
    URI uri3 = URI.create(String.format("%s://%s%s", UriSchemes.FILE_SCHEME, host2, path1));
    HadoopFsCacheKeyPluginClassLoader key3 = new HadoopFsCacheKeyPluginClassLoader(uri3, new JobConf(), userName1);
    assertNotEquals(key2, key3);

    String path2 ="/sample/data2";
    URI uri4 = URI.create(String.format("%s://%s%s", UriSchemes.FILE_SCHEME, host2, path2));
    HadoopFsCacheKeyPluginClassLoader key4 = new HadoopFsCacheKeyPluginClassLoader(uri4, new JobConf(), userName1);
    assertEquals(key3, key4);

    String userName2 = "dremio2";
    HadoopFsCacheKeyPluginClassLoader key5 = new HadoopFsCacheKeyPluginClassLoader(uri4, new JobConf(), userName2);
    assertNotEquals(key4, key5);
  }
}
