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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unittests for {@link DremioFileSystemCache} */
public class TestDremioFileSystemCache {

  @BeforeClass
  public static void setup() {
    UserGroupInformation.createUserForTesting("newUser", new String[] {});
  }

  @Test
  public void withUniqueConnProps() throws Exception {
    final DremioFileSystemCache dfsc = new DremioFileSystemCache();
    final URI uri = URI.create("file:///path");
    final List<String> uniqueProps = ImmutableList.of("prop1", "prop2");

    Configuration conf1 = new Configuration();
    FileSystem fs1 = dfsc.get(uri, conf1, uniqueProps);

    // get a fs with change in unique props
    Configuration conf2 = new Configuration(conf1);
    conf2.set("prop1", "prop1Val");
    FileSystem fs2 = dfsc.get(uri, conf2, uniqueProps);

    // Expect a different filesystem
    assertTrue(fs1 != fs2);

    // Now get one more with same conf as fs2, expect to get the same entry as fs2 as it is cached
    // and matches
    FileSystem fs3 = dfsc.get(uri, conf2, uniqueProps);
    assertTrue(fs2 == fs3);

    // now create a third filesystem as different user and make sure we got a different filesystem
    // instance
    FileSystem fs4 = getAs("newUser", dfsc, uri, conf2, uniqueProps);
    assertTrue(fs2 != fs4);
    assertTrue(fs1 != fs4);

    // Now try to get a file system without unique props but same conf as fs1
    FileSystem fs5 = dfsc.get(uri, conf1, null);
    assertTrue(fs1 != fs5); // as this is created by Hadoop FileSystem cache

    // Now get one more same as fs5 conf and and expect it to be different as it is not retrieved
    // from Hadoop FileSystem cache.
    FileSystem fs6 = dfsc.get(uri, conf1, null);
    assertTrue(fs5 != fs6);
  }

  /**
   * This test is to make sure legacy scenarios through {@link DremioFileSystemCache} work as
   * expected.
   */
  @Test
  public void withoutUniqueConnProps() throws Exception {
    final DremioFileSystemCache dfsc = new DremioFileSystemCache();
    final URI uri = URI.create("file:///path");

    Configuration conf1 = new Configuration();

    // get a filesystem
    FileSystem fs1 = dfsc.get(uri, conf1, null);

    // get the same with slight configuration change
    Configuration conf2 = new Configuration(conf1);
    conf2.set("blah", "boo");
    FileSystem fs2 = dfsc.get(uri, conf2, null);

    // Make sure both are not of same instance as we expect the Hadoop FileSystem cache to be
    // disabled
    assertTrue(fs1 != fs2);

    // now create a third filesystem as different user and make sure we got a different filesystem
    // instance
    FileSystem fs3 = getAs("newUser", dfsc, uri, conf1, null);
    assertTrue(fs1 != fs3);
  }

  /**
   * This test is to make sure legacy scenarios through {@link DremioFileSystemCache} work as
   * expected.
   */
  @Test
  public void withoutUniqueConnPropsWithCacheExplicitlyDisabled() throws Exception {
    final DremioFileSystemCache dfsc = new DremioFileSystemCache();
    final URI uri = URI.create("file:///path");

    Configuration conf1 = new Configuration();
    final String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
    conf1.setBoolean(disableCacheName, true);

    // get a filesystem
    FileSystem fs1 = dfsc.get(uri, conf1, null);

    // get the same with slight configuration change
    Configuration conf2 = new Configuration(conf1);
    conf2.set("blah", "boo");
    FileSystem fs2 = dfsc.get(uri, conf2, null);

    // Make sure different instance as we expect the Hadoop FileSystem to not cache them
    assertTrue(fs1 != fs2);

    // now create a third filesystem as different user and make sure we got a different filesystem
    // instance
    FileSystem fs3 = getAs("newUser", dfsc, uri, conf1, null);
    assertTrue(fs1 != fs3);

    // make sure we got a different filesystem instance
    assertTrue(fs1 != fs3);
  }

  private FileSystem getAs(
      String user,
      DremioFileSystemCache dfsc,
      URI uri,
      Configuration conf,
      List<String> uniqueConnProps)
      throws Exception {
    UserGroupInformation newUserUGI =
        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    return newUserUGI.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return dfsc.get(uri, conf, uniqueConnProps);
          }
        });
  }
}
