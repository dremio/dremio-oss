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
package com.dremio.exec.catalog;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.NessieReferenceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieForbiddenException;

@ExtendWith(MockitoExtension.class)
public class TestVersionContextResolver {

  @Mock private PluginRetriever pluginRetriever;

  @Test
  public void testNessieForbiddenException() {
    doThrow(new NessieForbiddenException(mock(NessieError.class)))
        .when(pluginRetriever)
        .getPlugin(anyString(), anyBoolean());
    assertThrows(
        NessieForbiddenException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext("mysource", mock(VersionContext.class)));
  }

  @Test
  public void testNessieReferenceException() {
    doThrow(NullPointerException.class).when(pluginRetriever).getPlugin(anyString(), anyBoolean());
    assertThrows(
        NessieReferenceException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext("mysource", mock(VersionContext.class)));
  }
}
