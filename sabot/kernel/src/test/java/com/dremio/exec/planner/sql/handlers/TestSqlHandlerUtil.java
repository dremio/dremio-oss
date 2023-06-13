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
package com.dremio.exec.planner.sql.handlers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.ischema.InfoSchemaStoragePlugin;

public class TestSqlHandlerUtil {

  @Test
  public void testGetSourceType() throws Exception {
    Catalog catalog = mock(Catalog.class);
    StoragePlugin source = mock(InfoSchemaStoragePlugin.class);
    String sourceName = "info_schema";
    doReturn(source).when(catalog).getSource(sourceName);

    assertThat(SqlHandlerUtil.getSourceType(catalog, sourceName)).isEqualTo("InfoSchemaStoragePlugin");
  }

  @Test
  public void testGetUnknownSourceType() throws Exception {
    Catalog catalog = mock(Catalog.class);
    String sourceName = "unknown";
    doThrow(UserException.class).when(catalog).getSource(sourceName);

    assertThat(SqlHandlerUtil.getSourceType(catalog, sourceName)).isEqualTo("Unknown");
  }

  @Test
  public void testGetUnknownSourceTypeForEmpty() throws Exception {
    Catalog catalog = mock(Catalog.class);
    assertThat(SqlHandlerUtil.getSourceType(catalog, "")).isEqualTo("Unknown");
  }

  @Test
  public void testGetUnknownSourceTypeForNull() throws Exception {
    assertThat(SqlHandlerUtil.getSourceType(mock(Catalog.class), null)).isEqualTo("Unknown");
  }
}
