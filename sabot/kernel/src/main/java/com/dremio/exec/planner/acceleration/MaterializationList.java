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
package com.dremio.exec.planner.acceleration;

import com.dremio.exec.planner.acceleration.substitution.MaterializationProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;

/**
 * MaterializationList builds a list of "considered" {@link DremioMaterialization} instances that
 * are used for the lifetime of a single query. A {@link DremioMaterialization} is usually retrieved
 * from cache and then deep-copied so that it can be mutated by the planner during target
 * normalization.
 *
 * <p>DremioMaterialization instances are lazily built during SqlToRel (for default raw reflections)
 * and logical planning phases from {@link MaterializationDescriptor} instances.
 */
public class MaterializationList implements MaterializationProvider {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaterializationList.class);

  private final Map<NamespaceKey, MaterializationDescriptor> materializationDescriptors =
      Maps.newHashMap();
  private List<DremioMaterialization> materializations = ImmutableList.of();

  private final MaterializationDescriptorProvider provider;
  private final SqlConverter converter;
  private final UserSession session;

  public MaterializationList(
      final SqlConverter converter,
      final UserSession session,
      final MaterializationDescriptorProvider provider) {
    this.provider = Preconditions.checkNotNull(provider, "provider is required");
    this.converter = Preconditions.checkNotNull(converter, "converter is required");
    this.session = Preconditions.checkNotNull(session, "session is required");
  }

  @Override
  public List<DremioMaterialization> getConsideredMaterializations() {
    return Preconditions.checkNotNull(materializations);
  }

  @Override
  public java.util.Optional<DremioMaterialization> getDefaultRawMaterialization(ViewTable table) {

    if (isNoReflections()) {
      return java.util.Optional.empty();
    }
    final Set<String> exclusions = getExclusions();
    final Set<String> inclusions = getInclusions();
    final boolean hasInclusions = !inclusions.isEmpty();
    final java.util.Optional<MaterializationDescriptor> opt =
        provider.getDefaultRawMaterialization(table);

    if (opt.isPresent()) {
      MaterializationDescriptor descriptor = opt.get();
      if ((hasInclusions && !inclusions.contains(descriptor.getLayoutId()))
          || exclusions.contains(descriptor.getLayoutId())) {
        return java.util.Optional.empty();
      } else {
        try {
          DremioMaterialization materialization = descriptor.getMaterializationFor(converter);
          materializationDescriptors.put(new NamespaceKey(descriptor.getPath()), descriptor);
          return java.util.Optional.of(materialization);
        } catch (Throwable e) {
          logger.warn("Failed to expand materialization {}", descriptor.getMaterializationId(), e);
        }
      }
    }
    return java.util.Optional.empty();
  }

  @Override
  public boolean isMaterializationCacheInitialized() {
    return provider.isMaterializationCacheInitialized();
  }

  public Optional<MaterializationDescriptor> getDescriptor(final List<String> path) {
    final MaterializationDescriptor descriptor =
        materializationDescriptors.get(new NamespaceKey(path));
    return Optional.ofNullable(descriptor);
  }

  @Override
  public List<DremioMaterialization> buildConsideredMaterializations(RelNode userQueryNode) {
    if (isNoReflections()) {
      return ImmutableList.of();
    }

    final Set<SubstitutionUtils.VersionedPath> queryTablesUsed =
        SubstitutionUtils.findTables(userQueryNode);
    final Set<SubstitutionUtils.VersionedPath> queryVdsUsed =
        SubstitutionUtils.findExpansionNodes(userQueryNode);
    final Set<SubstitutionUtils.ExternalQueryDescriptor> externalQueries =
        SubstitutionUtils.findExternalQueries(userQueryNode);

    final Set<String> exclusions = getExclusions();
    final Set<String> inclusions = getInclusions();
    final boolean hasInclusions = !inclusions.isEmpty();
    final List<DremioMaterialization> materializations = Lists.newArrayList();
    for (final MaterializationDescriptor descriptor : provider.get()) {

      if ((hasInclusions && !inclusions.contains(descriptor.getLayoutId()))
          || exclusions.contains(descriptor.getLayoutId())) {
        continue;
      }

      try {
        if (session.getSubstitutionSettings().isExcludeFileBasedIncremental()
            && (descriptor.getIncrementalUpdateSettings().isFileMtimeBasedUpdate()
                ||
                // TODO: support using snapshot based incremental reflection to accelerate other
                // incremental reflection refresh
                descriptor.getIncrementalUpdateSettings().isSnapshotBasedUpdate())) {
          continue;
        }
        // Prune the reflection early if the descriptor is already expanded
        if (!descriptor.isApplicable(queryTablesUsed, queryVdsUsed, externalQueries)) {
          continue;
        }
        final DremioMaterialization materialization = descriptor.getMaterializationFor(converter);
        if (materialization == null) {
          continue;
        }
        if (!SubstitutionUtils.usesTableOrVds(
            queryTablesUsed, queryVdsUsed, externalQueries, materialization.getQueryRel())) {
          continue;
        }
        final NamespaceKey path = new NamespaceKey(descriptor.getPath());
        materializationDescriptors.put(path, descriptor);
        materializations.add(materialization);

      } catch (Throwable e) {
        logger.warn("failed to expand materialization {}", descriptor.getMaterializationId(), e);
      }
    }
    this.materializations = materializations;
    return materializations;
  }

  public Set<String> getInclusions() {
    Set<String> inclusions = Sets.newHashSet(session.getSubstitutionSettings().getInclusions());
    inclusions.addAll(
        parseReflectionIds(
            converter
                .getFunctionContext()
                .getOptions()
                .getOption(PlannerSettings.CONSIDER_REFLECTIONS)));
    return inclusions;
  }

  public Set<String> getExclusions() {
    Set<String> exclusions = Sets.newHashSet(session.getSubstitutionSettings().getExclusions());
    exclusions.addAll(
        parseReflectionIds(
            converter
                .getFunctionContext()
                .getOptions()
                .getOption(PlannerSettings.EXCLUDE_REFLECTIONS)));
    return exclusions;
  }

  public boolean isNoReflections() {
    return converter.getFunctionContext().getOptions().getOption(PlannerSettings.NO_REFLECTIONS);
  }

  public static Set<String> parseReflectionIds(String value) {
    return Arrays.asList(value.split(",")).stream()
        .map(x -> x.trim())
        .filter(x -> !x.isEmpty())
        .collect(Collectors.toSet());
  }
}
