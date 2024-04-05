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
package com.dremio.dac.model.namespace;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.space.proto.FolderConfig;
import org.junit.Test;

public class TestNamespaceTree {

  @Test
  public void testAddFolderCheckFileSystemFolder() throws Exception {
    NamespaceTree namespaceTree = new NamespaceTree();
    namespaceTree.setIsFileSystemSource(true);
    namespaceTree.addFolder(
        new SourceFolderPath("abc.cde"), new FolderConfig(), null, Type.SOURCE, false, false);
    assertTrue(
        "Folder should be file system folder if source is file system source",
        namespaceTree.getFolders().get(0).isFileSystemFolder());

    namespaceTree = new NamespaceTree();
    namespaceTree.setIsFileSystemSource(false);
    namespaceTree.addFolder(
        new SourceFolderPath("abc.bcd"), new FolderConfig(), null, Type.SOURCE, false, false);
    assertFalse(
        "Folder should not be file system folder if source is not file system source",
        namespaceTree.getFolders().get(0).isFileSystemFolder());
  }
}
