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
package com.dremio.io.file;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.dremio.test.DremioTest;

/**
 * Test cases for {@code Path}
 */
public class TestPath extends DremioTest {
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  /*
   * Check that Path.of(URI.create(value)).toURI() == URI.create(value)
   */
  @Test
  public void testPathOfURI() {
    checkPathOfURI("/foo/bar");
    checkPathOfURI("/foo/bar/");
    checkPathOfURI("/foo/bar%20baz");
    checkPathOfURI("//foo/bar");
    checkPathOfURI("foo/bar");
    checkPathOfURI("hdfs:///foo/bar");
    checkPathOfURI("hdfs://foo/bar");
    checkPathOfURI("file:/foo/bar%20baz");
    checkPathOfURI("webhdfs://hostname.domain:12345/foo/bar%20baz");
  }

  private void checkPathOfURI(String value) {
    final URI uri = URI.create(value);
    collector.checkThat(Path.of(uri).toURI(), is(equalTo(uri)));
  }

  /*
   * Check that Path.of(value).toURI() == URI.create(expected)
   */
  @Test
  public void testPathOfString() {
    checkPathOfString("/foo/bar", "/foo/bar");
    checkPathOfString("/foo/bar/", "/foo/bar/");
    checkPathOfString("/foo/bar baz", "/foo/bar baz");
    checkPathOfString("//foo/bar", "//foo/bar");
    checkPathOfString("foo/bar", "foo/bar");
    checkPathOfString("hdfs:///foo/bar", "hdfs:/foo/bar");
    checkPathOfString("hdfs://foo/bar", "hdfs://foo/bar");
    checkPathOfString("file:/foo/bar baz", "file:/foo/bar baz");
    checkPathOfString("webhdfs://hostname.domain:12345/foo/bar baz", "webhdfs://hostname.domain:12345/foo/bar baz");
    // check normalization
    checkPathOfString("/foo/bar/..", "/foo/");
    checkPathOfString("/foo/../bar", "/bar");
    checkPathOfString("/foo/bar/.", "/foo/bar/");
    checkPathOfString("/foo/./bar", "/foo/bar");
    checkPathOfString("/foo/bar/../..", "/");
    checkPathOfString("/foo/../bar/..", "/");
    checkPathOfString("hdfs://hostname/foo//bar", "hdfs://hostname/foo/bar");
  }

  private void checkPathOfString(String value, String expected) {
    collector.checkThat(Path.of(value).toString(), is(equalTo(expected)));
  }

  /*
   * Check invalid paths
   */
  @Test
  public void testInvalidPathOfString() {
    checkInvalidPathOfString("file://");
    checkInvalidPathOfString("hdfs:");
    checkInvalidPathOfString(null);
    checkInvalidPathOfString("");
    checkInvalidPathOfString("foo:bar");
  }

  private void checkInvalidPathOfString(String value) {
    collector.checkSucceeds(() -> {
      try {
        Path.of(value);
        fail();
      } catch (NullPointerException | IllegalArgumentException e) {
        // Nothing
      }
      return null;
    });
  }

  /*
   * Check that value.getParent() == expected
   */
  @Test
  public void testParent() {
    checkParent(Path.of("/foo/bar"), Path.of("/foo"));
    checkParent(Path.of("/foo"), Path.of("/"));
    checkParent(Path.of("/"), null);
    checkParent(Path.of("foo/bar"), Path.of("foo"));
    checkParent(Path.of("foo"), Path.of("."));
  }

  private void checkParent(Path value, Path expected) {
    collector.checkThat(value.getParent(), is(equalTo(expected)));
  }

  /*
   * Check that base.resolve(value) == expected
   */
  @Test
  public void testResolveOfPath() {
    checkResolveOfPath(Path.of("hdfs://hostname/base"), Path.of("/foo/bar"), Path.of("/foo/bar"));
    checkResolveOfPath(Path.of("hdfs://hostname/base"), Path.of("foo/bar"), Path.of("hdfs://hostname/base/foo/bar"));
    checkResolveOfPath(Path.of("hdfs://hostname/base"), Path.of(URI.create("")), Path.of("hdfs://hostname/base"));
    checkResolveOfPath(Path.of("hdfs://hostname/base"), Path.of("file://baz"), Path.of("file://baz"));
    checkResolveOfPath(Path.of("."), Path.of("foo"), Path.of("foo"));
    checkResolveOfPath(Path.of("foo"), Path.of("."), Path.of("foo"));
    checkResolveOfPath(Path.of("/"), Path.of("."), Path.of("/"));
  }

  private void checkResolveOfPath(Path base, Path value, Path expected) {
    collector.checkThat(base.resolve(value), is(equalTo(expected)));
  }

  /*
   * Check that base.resolve(value) == expected
   */
  @Test
  public void testResolveOfString() {
    checkResolveOfString(Path.of("hdfs://hostname/base"), "/foo/bar", Path.of("/foo/bar"));
    checkResolveOfString(Path.of("hdfs://hostname/base"), "foo/bar", Path.of("hdfs://hostname/base/foo/bar"));
    checkResolveOfString(Path.of("hdfs://hostname/base"), "foo bar", Path.of("hdfs://hostname/base/foo bar"));
    checkResolveOfString(Path.of("hdfs://hostname/base"), "file://baz", Path.of("file://baz"));
    checkResolveOfString(Path.of("."), "foo", Path.of("foo"));
    checkResolveOfString(Path.of("foo"), ".", Path.of("foo"));
    checkResolveOfString(Path.of("/foo"), ".", Path.of("/foo"));
    checkResolveOfString(Path.of("/"), ".", Path.of("/"));
  }

  private void checkResolveOfString(Path base, String value, Path expected) {
    collector.checkThat(base.resolve(value), is(equalTo(expected)));
  }

  /*
   * Check that Path.mergePaths(base, value) == expected
   */
  @Test
  public void testMergePaths() {
    checkMergePaths(Path.of("hdfs://hostname/base"), Path.of("/foo/bar"), Path.of("hdfs://hostname/base/foo/bar"));
    checkMergePaths(Path.of("hdfs://hostname/base"), Path.of("foo/bar"), Path.of("hdfs://hostname/base/foo/bar"));
    checkMergePaths(Path.of("hdfs://hostname/base"), Path.of(URI.create("")), Path.of("hdfs://hostname/base"));
    checkMergePaths(Path.of("hdfs://hostname/base"), Path.of("file://baz"), Path.of("hdfs://hostname/base"));
    checkMergePaths(Path.of("."), Path.of("foo"), Path.of("foo"));
    checkMergePaths(Path.of("foo"), Path.of("."), Path.of("foo"));
    checkMergePaths(Path.of("/"), Path.of("."), Path.of("/"));
  }

  private void checkMergePaths(Path base, Path value, Path expected) {
    collector.checkThat(Path.mergePaths(base, value), is(equalTo(expected)));
  }

  /**
   * Check that value.getName() == expected
   */
  @Test
  public void testGetName() {
    checkGetName(Path.of("/foo/bar"), "bar");
    checkGetName(Path.of("/foo/bar baz"), "bar baz");
    checkGetName(Path.of("//foo/bar"), "bar");
    checkGetName(Path.of("foo/bar"), "bar");
    checkGetName(Path.of("hdfs:///foo/bar"), "bar");
    checkGetName(Path.of("hdfs://foo/bar"), "bar");
    checkGetName(Path.of("file:/foo/bar baz"), "bar baz");
    checkGetName(Path.of("webhdfs://hostname.domain:12345/foo/bar baz"), "bar baz");
  }

  private void checkGetName(Path value, String expected) {
    collector.checkThat(value.getName(), is(equalTo(expected)));
  }

  /**
   * Check that value.isAbsolute() == expected
   */
  @Test
  public void testIsAbsolute() {
    checkIsAbsolute(Path.of("/"), true);
    checkIsAbsolute(Path.of("/foo"), true);
    checkIsAbsolute(Path.of("/foo/bar"), true);
    checkIsAbsolute(Path.of("foo"), false);
    checkIsAbsolute(Path.of("foo/bar"), false);
    checkIsAbsolute(Path.of(URI.create("")), false);
    checkIsAbsolute(Path.of("."), false);
  }

  private void checkIsAbsolute(Path value, boolean expected) {
    collector.checkThat(value.isAbsolute(), is(equalTo(expected)));
  }
  /**
   * Check that value.depth() == expected
   */
  @Test
  public void testDepth() {
    checkDepth(Path.of("/"), 0);
    checkDepth(Path.of("/foo"), 1);
    checkDepth(Path.of("/foo/bar"), 2);
    checkDepth(Path.of("foo"), 0);
  }

  private void checkDepth(Path value, int expected) {
    collector.checkThat(value.depth(), is(equalTo(expected)));
  }

  @Test
  public void testgetContainerSpecificRelativePath() {
    String path = "wasbs://testdir@azurev1accountName.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041-1-bb5ecd1c-80fb-494f-9716-d2baa8f69eff.avro";
    Path p = Path.of(path);
    String modified = Path.getContainerSpecificRelativePath(p);
    assertEquals("/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041-1-bb5ecd1c-80fb-494f-9716-d2baa8f69eff.avro", modified);

    path = "s3a://unittest.dremio.com/icebergtables/t63/metadata/snap-2789552798039628309-1-8a1551a0-80cf-44fd-9a28-2e3334b9602f.avro";
    p = Path.of(path);
    modified = Path.getContainerSpecificRelativePath(p);
    assertEquals("/unittest.dremio.com/icebergtables/t63/metadata/snap-2789552798039628309-1-8a1551a0-80cf-44fd-9a28-2e3334b9602f.avro", modified);

    path = "adl://databrickstest.azuredatalakestore.net/Automation/regression/iceberg/init32_decimal_test/metadata/snap-5631102415330351524-1-0d989445-a0f5-47cf-bf19-a3d648ac1b31.avro";
    p = Path.of(path);
    modified = Path.getContainerSpecificRelativePath(p);
    assertEquals("/Automation/regression/iceberg/init32_decimal_test/metadata/snap-5631102415330351524-1-0d989445-a0f5-47cf-bf19-a3d648ac1b31.avro", modified);

    path = "file:///Automation/regression/iceberg/init32_decimal_test/metadata/snap-5631102415330351524-1-0d989445-a0f5-47cf-bf19-a3d648ac1b31.avro";
    p = Path.of(path);
    modified = Path.getContainerSpecificRelativePath(p);
    assertEquals("/Automation/regression/iceberg/init32_decimal_test/metadata/snap-5631102415330351524-1-0d989445-a0f5-47cf-bf19-a3d648ac1b31.avro", modified);
  }
}
