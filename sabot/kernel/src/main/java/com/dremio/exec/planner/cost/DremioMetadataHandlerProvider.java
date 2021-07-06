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
package com.dremio.exec.planner.cost;

import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataCache;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.janino.JaninoMetadataHandlerCreator;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.google.common.collect.ImmutableList;


public class DremioMetadataHandlerProvider implements MetadataHandlerProvider {

  private static final RelMetadataProvider DEFAULT_REL_METADATA_PROVIDER =
      ChainedRelMetadataProvider.of(ImmutableList.of(
          // Mostly relies on Calcite default with some adjustments...
          RelMdRowCount.SOURCE,
          RelMdDistinctRowCount.SOURCE,
          RelMdColumnOrigins.SOURCE,
          RelMdPredicates.SOURCE,
          RelMdCost.SOURCE,
          RelMdCollation.SOURCE,
          RelMdSelectivity.SOURCE,
          // Calcite catch-all
          org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE));
  static final DremioMetadataHandlerProvider INSTANCE =
      new DremioMetadataHandlerProvider(DEFAULT_REL_METADATA_PROVIDER);

  private final RelMetadataProvider relMetadataProvider;
  private DremioMetadataHandlerProvider(RelMetadataProvider relMetadataProvider) {
    this.relMetadataProvider = relMetadataProvider;
  }



  @Override
  public <H extends MetadataHandler<M>, M extends Metadata> H initialHandler(MetadataDef<M> def) {
    return (H) JaninoMetadataHandlerCreator.newInstance(def.handlerClass,
        relMetadataProvider.handlers(def));
  }

  @Override
  public <H extends MetadataHandler<M>, M extends Metadata> H revise(MetadataDef<M> def) {
    return (H) JaninoMetadataHandlerCreator.newInstance(def.handlerClass,
        relMetadataProvider.handlers(def));
  }

  @Override
  public MetadataCache buildCache() {
    return new DremioMetadataCache();
  }

  static DremioMetadataHandlerProvider createMetadataProviderWithStatics(StatisticsService statisticsService) {
    RelMetadataProvider relMetadataProvider = ChainedRelMetadataProvider.of(ImmutableList.of(
      // Mostly relies on Calcite default with some adjustments...
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new RelMdRowCount(statisticsService)),
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, new RelMdDistinctRowCount(statisticsService)),
    ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new RelMdSelectivity(statisticsService)),
      DEFAULT_REL_METADATA_PROVIDER));
    return new DremioMetadataHandlerProvider(relMetadataProvider);
  }

}
