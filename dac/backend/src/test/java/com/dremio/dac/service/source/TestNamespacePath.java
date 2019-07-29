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
package com.dremio.dac.service.source;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.file.SourceFilePath;

/**
 * Tests NamespacePath
 */
public class TestNamespacePath {

  @Test
  public void testToUrlPathWithAction() {
    NamespacePath path;
    path = new SourceFilePath(Arrays.asList("mysource", "myfolder", "myfile"));
    assertEquals("/source/mysource/file/myfolder/myfile", path.toUrlPathWithAction("file"));

    path = new SourceFilePath(Arrays.asList("mysource", "myfile"));
    assertEquals("/source/mysource/file/myfile", path.toUrlPathWithAction("file"));

    path = new SpacePath("myspace");
    assertEquals("/space/myspace", path.toUrlPath());

    // test escaping of /
    path = new SourceFilePath(Arrays.asList("mysource", "myfolder", "my/file"));
    assertEquals("/source/mysource/file/myfolder/my%2Ffile", path.toUrlPathWithAction("file"));
  }

  @Test
  public void testGetQueryUrlPath() {
    NamespacePath path;
    path = new DatasetPath(Arrays.asList(new String[]{"@dremio", "my folder", "my-dataset"}));
    assertEquals("/home/%40dremio/%22my%20folder%22.%22my-dataset%22", path.getQueryUrlPath());

    path = new DatasetPath(Arrays.asList(new String[]{"myspace", "+@#$%^&;?/\\=()"}));
    assertEquals("/space/myspace/%22%2B%40%23%24%25%5E%26%3B%3F%2F%5C%3D()%22", path.getQueryUrlPath());
  }

  @Test
  public void testGetPreviewDataUrlPath() {
    NamespacePath path;
    path = new DatasetPath(Arrays.asList(new String[]{"myspace", "myfolder", "my-dataset"}));
    assertEquals("/dataset/myspace.myfolder.%22my-dataset%22/preview", path.getPreviewDataUrlPath());
  }

}
