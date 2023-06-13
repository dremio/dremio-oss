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

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.dremio.options.OptionManager;

/**
 * Immutable Catalog features class. The features are base on options.  Call static {@link CatalogFeatures#get(OptionManager) get()}
 * method to create an instance of this class.
 */
@Immutable
public final class CatalogFeatures {

  /**
   * Catalog features
   */
  public enum Feature {
    ARS,
    DATA_GRAPH,
    HOME,
    SEARCH,
    SPACE,
    STARRING,
  }

  private final Set<Feature> enabledFeatures;

  private CatalogFeatures(Feature... enabledFeatures) {
    this.enabledFeatures = new HashSet();
    for (Feature feature : enabledFeatures) {
      this.enabledFeatures.add(feature);
    }
  }

  public boolean isFeatureEnabled(Feature feature) {
    return enabledFeatures.contains(feature);
  }

  public static CatalogFeatures get(final OptionManager options) {
    boolean arsEnabled = options.getOption(CatalogOptions.CATALOG_ARS_ENABLED);

    if (arsEnabled) {
      return new CatalogFeatures(Feature.ARS);
    } else {
      return new CatalogFeatures(Feature.DATA_GRAPH, Feature.HOME, Feature.SEARCH, Feature.SPACE, Feature.STARRING);
    }
  }

}
