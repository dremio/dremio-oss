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

import static com.dremio.exec.catalog.CatalogFeatures.Feature.ARS;
import static com.dremio.exec.catalog.CatalogFeatures.Feature.DATA_GRAPH;
import static com.dremio.exec.catalog.CatalogFeatures.Feature.HOME;
import static com.dremio.exec.catalog.CatalogFeatures.Feature.SEARCH;
import static com.dremio.exec.catalog.CatalogFeatures.Feature.SPACE;
import static com.dremio.exec.catalog.CatalogFeatures.Feature.STARRING;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.dremio.options.OptionManager;

public class TestCatalogFeatures {
  @Test
  public void testCatalogARSFeatureEnabled() {
    OptionManager mockOptions = mock(OptionManager.class);
    when(mockOptions.getOption(CatalogOptions.CATALOG_ARS_ENABLED)).thenReturn(true);
    CatalogFeatures catalogFeatures = CatalogFeatures.get(mockOptions);
    assertEquals(true, catalogFeatures.isFeatureEnabled(ARS));
    assertEquals(false, catalogFeatures.isFeatureEnabled(DATA_GRAPH));
    assertEquals(false, catalogFeatures.isFeatureEnabled(HOME));
    assertEquals(false, catalogFeatures.isFeatureEnabled(SEARCH));
    assertEquals(false, catalogFeatures.isFeatureEnabled(SPACE));
    assertEquals(false, catalogFeatures.isFeatureEnabled(STARRING));
  }

  @Test
  public void testCatalogARSFeatureDisabled() {
    OptionManager mockOptions = mock(OptionManager.class);
    when(mockOptions.getOption(CatalogOptions.CATALOG_ARS_ENABLED)).thenReturn(false);
    CatalogFeatures catalogFeatures = CatalogFeatures.get(mockOptions);
    assertEquals(false, catalogFeatures.isFeatureEnabled(ARS));
    assertEquals(true, catalogFeatures.isFeatureEnabled(DATA_GRAPH));
    assertEquals(true, catalogFeatures.isFeatureEnabled(HOME));
    assertEquals(true, catalogFeatures.isFeatureEnabled(SEARCH));
    assertEquals(true, catalogFeatures.isFeatureEnabled(SPACE));
    assertEquals(true, catalogFeatures.isFeatureEnabled(STARRING));
  }
}
