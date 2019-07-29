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
package com.dremio.dac.model.folder;

import org.junit.Assert;
import org.junit.Test;

/**
 * DatasetPath tests
 */
public class TestFolderPath {

  @Test
  public void testCreateFromFullPath() {
    FolderPath folderPath = new FolderPath("a.b.c");
    Assert.assertEquals("a", folderPath.getRoot().getName());
    Assert.assertEquals("c", folderPath.getFolderName().getName());
    Assert.assertEquals(1, folderPath.getFolderPath().size());
    Assert.assertEquals("b", folderPath.getFolderPath().get(0).getName());
    Assert.assertEquals("a.b.c", folderPath.toPathString());

    Assert.assertEquals("a.b.c", folderPath.toPathString());
  }

  @Test
  public void testCreateFromUrlParams() {
    FolderPath folderPath = new FolderPath("a", "b/c");
    Assert.assertEquals("a", folderPath.getRoot().getName());
    Assert.assertEquals("c", folderPath.getFolderName().getName());
    Assert.assertEquals(1, folderPath.getFolderPath().size());
    Assert.assertEquals("b", folderPath.getFolderPath().get(0).getName());
    Assert.assertEquals("a.b.c", folderPath.toPathString());

    Assert.assertEquals("a.b.c", folderPath.toPathString());
  }

  @Test
  public void testParseUrlPath() {
    FolderPath folderPath = (FolderPath) Folder.parseUrlPath("/space/s1/folder/f1/f2");
    Assert.assertEquals("s1.f1.f2", folderPath.toPathString());
  }

  @Test
  public void testParseSourceUrlPath() {
    SourceFolderPath folderPath = (SourceFolderPath) Folder.parseUrlPath("/source/s1/folder/f1/f2");
    Assert.assertEquals("s1.f1.f2", folderPath.toPathString());
  }

  @Test
  public void testToUrlPath() {
    FolderPath folderPath = new FolderPath("a", "b/c");
    Assert.assertEquals("/space/a/folder/b/c", folderPath.toUrlPath());
  }
}
