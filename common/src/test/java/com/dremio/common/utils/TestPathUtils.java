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
package com.dremio.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.google.common.collect.ImmutableList;

/**
 * Tests for conversions between filesystem paths to schema paths.
 */
public class TestPathUtils {

  @Test
  public void testPathComponents() throws Exception {
    assertEquals(ImmutableList.of("a", "b", "c"), PathUtils.toPathComponents(new Path("/a/b/c")));
    assertEquals(ImmutableList.of("a", "b", "c"), PathUtils.toPathComponents(new Path("a/b/c")));
    assertEquals(ImmutableList.of("a", "b", "c/"), PathUtils.toPathComponents(new Path("a/b/\"c/\"")));
  }

  @Test
  public void testFSPathToSchemaPath() throws Exception {
    assertEquals("a.b.c", PathUtils.toDottedPath(new Path("/a/b/c")));
    assertEquals("a.b.c", PathUtils.toDottedPath(new Path("a/b/c")));
    assertEquals("a.b.\"c.json\"", PathUtils.toDottedPath(new Path("/a/b/c.json")));
    assertEquals("\"c.json\"", PathUtils.toDottedPath(new Path("c.json")));
    assertEquals("\"c.json\"", PathUtils.toDottedPath(new Path("/c.json")));
    assertEquals("c", PathUtils.toDottedPath(new Path("/c")));
  }


  @Test
  public void testToFSPath() throws Exception {
    assertEquals(new Path("/a/b/c"), PathUtils.toFSPath("a.b.c"));
    assertEquals(new Path("/a/b"), PathUtils.toFSPath("a.b"));
    assertEquals(new Path("/c.txt"), PathUtils.toFSPath("\"c.txt\""));
    assertEquals(new Path("/a/b/c.txt"), PathUtils.toFSPath("a.b.\"c.txt\""));
  }

  @Test
  public void testHybridSchemaPathToFSPath() throws Exception {
    assertEquals(new Path("/dfs/tmp/a/b/c"), PathUtils.toFSPath("dfs.tmp.\"a/b/c"));
    assertEquals(new Path("/dfs/tmp/a/b/c.json"), PathUtils.toFSPath("dfs.tmp.\"a/b/c.json"));
    assertEquals(new Path("/dfs/tmp/a.txt"), PathUtils.toFSPath("dfs.tmp.\"a.txt"));
  }

  @Test
  public void testToFSPathSkipRoot() throws Exception {
    assertEquals(new Path("/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), "a"));
    assertEquals(new Path("/a/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), "z"));
    assertEquals(new Path("/a/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), null));
    assertEquals(new Path("/"), PathUtils.toFSPathSkipRoot(null, "a"));
    assertEquals(new Path("/"), PathUtils.toFSPathSkipRoot(Collections.<String>emptyList(), "a"));
  }

  @Test
  public void toDottedPathWithCommonPrefix() throws Exception {
    assertEquals("d", PathUtils.toDottedPath(new Path("/a/b/c"), new Path("/a/b/c/d")));
    assertEquals("b.c.d", PathUtils.toDottedPath(new Path("/a"), new Path("/a/b/c/d")));
    assertEquals("a.b.c.d", PathUtils.toDottedPath(new Path("/"), new Path("/a/b/c/d")));
    assertEquals("c.d.\"e.json\"", PathUtils.toDottedPath(new Path("/a/b/"), new Path("/a/b/c/d/e.json")));
  }

  @Test
  public void toDottedPathWithInvalidPrefix() throws Exception {
    try {
      PathUtils.toDottedPath(new Path("/p/q/"), new Path("/a/b/c/d/e.json"));
      fail("constructing relative path of child /a/b/c/d/e.json with parent /p/q should fail");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRelativePath() throws Exception {
    assertEquals("a", PathUtils.relativePath(new Path("/a"), new Path("/")));
    assertEquals("b", PathUtils.relativePath(new Path("/a/b"), new Path("/a")));
    assertEquals("b/c.json", PathUtils.relativePath(new Path("/a/b/c.json"), new Path("/a")));
    assertEquals("c/d/e", PathUtils.relativePath(new Path("/a/b/c/d/e"), new Path("/a/b")));
    assertEquals("/a/b", PathUtils.relativePath(new Path("/a/b"), new Path("/c/d"))); // no common prefix
  }

  @Test
  public void testRemoveLeadingSlash() {
    assertEquals("", PathUtils.removeLeadingSlash(""));
    assertEquals("", PathUtils.removeLeadingSlash("/"));
    assertEquals("aaaa", PathUtils.removeLeadingSlash("/aaaa"));
    assertEquals("aaaa/bbb", PathUtils.removeLeadingSlash("/aaaa/bbb"));
    assertEquals("aaaa/bbb", PathUtils.removeLeadingSlash("///aaaa/bbb"));
  }

  @Test
  public void testVerifyNoAccessOutsideBase() {
    PathUtils.verifyNoAccessOutsideBase(new Path("/"), new Path("/"));
    PathUtils.verifyNoAccessOutsideBase(new Path("/"), new Path("/a"));
    PathUtils.verifyNoAccessOutsideBase(new Path("/"), new Path("/a/b"));
    PathUtils.verifyNoAccessOutsideBase(new Path("/a"), new Path("/a/b"));
    PathUtils.verifyNoAccessOutsideBase(new Path("/a"), new Path("/a/b/c"));
    PathUtils.verifyNoAccessOutsideBase(new Path("/a"), new Path("/a/b/c"));
    try {
      PathUtils.verifyNoAccessOutsideBase(new Path("/a"), new Path("/a/../b/c"));
      fail();
    } catch (UserException ex) {
    }
    try {
      PathUtils.verifyNoAccessOutsideBase(new Path("/a"), new Path("/a/b/../../c"));
      fail();
    } catch (UserException ex) {
    }
  }
}
