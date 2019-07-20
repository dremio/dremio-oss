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
package com.dremio.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.URL;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * Tests for FileRecursiveIterator
 */
public class TestFileRecursiveIterator {
  @Test
  public void testLazyness() throws Exception {
    final URL multilevel = Resources.getResource("multilevel/json");
    final Path rootPath = new Path(multilevel.getPath());
    final FileSystemWrapper wrapper = FileSystemWrapperCreator.get(rootPath, new Configuration());

    final FileRecursiveIterator iterator = new FileRecursiveIterator(wrapper, rootPath, true);
    final Iterator<FileStatus> iteratorWrapper = new RemoteIteratorWrapper<>(iterator);
    final Iterable<FileStatus> iterable = () -> iteratorWrapper;

    StreamSupport.stream(iterable.spliterator(), false)
      .limit(3)
      .collect(Collectors.toList());

    // stack should be the root, root/1994, root/1994/Q1
    assertEquals(3, iterator.getStackSize());

    StreamSupport.stream(iterable.spliterator(), false)
      .limit(24)
      .collect(Collectors.toList());

    // stack should be the root, root/1996, root/1994/Q3
    assertEquals(3, iterator.getStackSize());

    // the files should now exhausted
    assertFalse(iterator.hasNext());
    assertEquals(0, iterator.getStackSize());
  }
}
