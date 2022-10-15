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
package com.dremio.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.io.file.Path;
import com.google.common.collect.ImmutableList;

/**
 * Tests for conversions between filesystem paths to schema paths.
 */
public class TestPathUtils {

  @Test
  public void testPathComponents() throws Exception {
    assertEquals(ImmutableList.of("a", "b", "c"), PathUtils.toPathComponents(Path.of("/a/b/c")));
    assertEquals(ImmutableList.of("a", "b", "c"), PathUtils.toPathComponents(Path.of("a/b/c")));
    assertEquals(ImmutableList.of("a", "b", "c/"), PathUtils.toPathComponents(Path.of("a/b/\"c/\"")));
  }

  @Test
  public void testGetQuotedFileName() throws Exception {
    assertEquals("\"c\"", PathUtils.getQuotedFileName(Path.of("/a/b/c")));
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->PathUtils.getQuotedFileName(null));
    assertTrue(ex.getMessage().contains("Fail to get a valid path from"));
  }

  @Test
  public void testFSPathToSchemaPath() throws Exception {
    assertEquals("a.b.c", PathUtils.toDottedPath(Path.of("/a/b/c")));
    assertEquals("a.b.c", PathUtils.toDottedPath(Path.of("a/b/c")));
    assertEquals("a.b.\"c.json\"", PathUtils.toDottedPath(Path.of("/a/b/c.json")));
    assertEquals("\"c.json\"", PathUtils.toDottedPath(Path.of("c.json")));
    assertEquals("\"c.json\"", PathUtils.toDottedPath(Path.of("/c.json")));
    assertEquals("c", PathUtils.toDottedPath(Path.of("/c")));
  }


  @Test
  public void testToFSPath() throws Exception {
    assertEquals(Path.of("/a/b/c"), PathUtils.toFSPath("a.b.c"));
    assertEquals(Path.of("/a/b"), PathUtils.toFSPath("a.b"));
    assertEquals(Path.of("/c.txt"), PathUtils.toFSPath("\"c.txt\""));
    assertEquals(Path.of("/a/b/c.txt"), PathUtils.toFSPath("a.b.\"c.txt\""));
  }

  @Test
  public void testHybridSchemaPathToFSPath() throws Exception {
    assertEquals(Path.of("/dfs/tmp/a/b/c"), PathUtils.toFSPath("dfs.tmp.\"a/b/c"));
    assertEquals(Path.of("/dfs/tmp/a/b/c.json"), PathUtils.toFSPath("dfs.tmp.\"a/b/c.json"));
    assertEquals(Path.of("/dfs/tmp/a.txt"), PathUtils.toFSPath("dfs.tmp.\"a.txt"));
  }

  @Test
  public void testToFSPathSkipRoot() throws Exception {
    assertEquals(Path.of("/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), "a"));
    assertEquals(Path.of("/a/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), "z"));
    assertEquals(Path.of("/a/b/c"), PathUtils.toFSPathSkipRoot(Arrays.asList("a", "b", "c"), null));
    assertEquals(Path.of("/"), PathUtils.toFSPathSkipRoot(null, "a"));
    assertEquals(Path.of("/"), PathUtils.toFSPathSkipRoot(Collections.<String>emptyList(), "a"));
  }

  @Test
  public void toDottedPathWithCommonPrefix() throws Exception {
    assertEquals("d", PathUtils.toDottedPath(Path.of("/a/b/c"), Path.of("/a/b/c/d")));
    assertEquals("b.c.d", PathUtils.toDottedPath(Path.of("/a"), Path.of("/a/b/c/d")));
    assertEquals("a.b.c.d", PathUtils.toDottedPath(Path.of("/"), Path.of("/a/b/c/d")));
    assertEquals("c.d.\"e.json\"", PathUtils.toDottedPath(Path.of("/a/b/"), Path.of("/a/b/c/d/e.json")));
  }

  @Test
  public void toDottedPathWithInvalidPrefix() throws Exception {
    try {
      PathUtils.toDottedPath(Path.of("/p/q/"), Path.of("/a/b/c/d/e.json"));
      fail("constructing relative path of child /a/b/c/d/e.json with parent /p/q should fail");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRelativePath() throws Exception {
    assertEquals("a", PathUtils.relativePath(Path.of("/a"), Path.of("/")));
    assertEquals("b", PathUtils.relativePath(Path.of("/a/b"), Path.of("/a")));
    assertEquals("b/c.json", PathUtils.relativePath(Path.of("/a/b/c.json"), Path.of("/a")));
    assertEquals("c/d/e", PathUtils.relativePath(Path.of("/a/b/c/d/e"), Path.of("/a/b")));
    assertEquals("/a/b", PathUtils.relativePath(Path.of("/a/b"), Path.of("/c/d"))); // no common prefix
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
    PathUtils.verifyNoAccessOutsideBase(Path.of("/"), Path.of("/"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/"), Path.of("/a"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/"), Path.of("/a/b"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a/"), Path.of("/a"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a/"), Path.of("/a/"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/b"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/b/c"));
    PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/b/c"));
    try {
      PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/../b/c"));
      fail();
    } catch (UserException ex) {
    }
    try {
      PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/a/b/../../c"));
      fail();
    } catch (UserException ex) {
    }
    try {
      PathUtils.verifyNoAccessOutsideBase(Path.of("/a"), Path.of("/ab"));
      fail();
    } catch (UserException ex) {
    }
    try {
      PathUtils.verifyNoAccessOutsideBase(Path.of("/a/"), Path.of("/ab"));
      fail();
    } catch (UserException ex) {
    }
  }
}
