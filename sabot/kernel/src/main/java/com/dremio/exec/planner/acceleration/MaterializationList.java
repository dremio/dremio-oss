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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.acceleration.substitution.MaterializationProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionUtils;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * MaterializationList contains a deep copy of DremioMaterialization instances that are used for the lifetime
 * of a single command or query.  The actual list of DremioMaterialization instances are lazily built during logical planning phase.
 * When materialization cache is enabled, we can trim the DremioMaterialization instances to only those that
 * overlap between the user query and scan, views and external queries within the materializations.
 */
public class MaterializationList implements MaterializationProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializationList.class);

  private final Map<TablePath, MaterializationDescriptor> mapping = Maps.newHashMap();
  private List<DremioMaterialization> materializations = ImmutableList.of();

  private final MaterializationDescriptorProvider provider;
  private final SqlConverter converter;
  private final UserSession session;

  public MaterializationList(final SqlConverter converter, final UserSession session,
                             final MaterializationDescriptorProvider provider) {
    this.provider = Preconditions.checkNotNull(provider, "provider is required");
    this.converter = Preconditions.checkNotNull(converter, "converter is required");
    this.session = Preconditions.checkNotNull(session, "session is required");
  }

  /**
   * Returns list of applicable materializations.
   */
  @Override
  public List<DremioMaterialization> getApplicableMaterializations() {
    return Preconditions.checkNotNull(materializations);
  }

  @Override
  public java.util.Optional<DremioMaterialization> getDefaultRawMaterialization(NamespaceKey path, List<String> vdsFields) {
    return getDefaultRawMaterialization(provider, path, vdsFields);
  }

  public Optional<MaterializationDescriptor> getDescriptor(final List<String> path) {
    return getDescriptor(TablePath.of(path));
  }

  public Optional<MaterializationDescriptor> getDescriptor(final TablePath path) {
    final MaterializationDescriptor descriptor = mapping.get(path);
    return Optional.ofNullable(descriptor);
  }

  @Override
  public List<DremioMaterialization> buildApplicableMaterializations(RelNode userQueryNode) {
    final Set<List<String>> queryTablesUsed = SubstitutionUtils.findTables(userQueryNode);
    final Set<List<String>> queryVdsUsed = SubstitutionUtils.findExpansionNodes(userQueryNode);
    final Set<SubstitutionUtils.ExternalQueryDescriptor> externalQueries = SubstitutionUtils.findExternalQueries(userQueryNode);

    final Set<String> exclusions = Sets.newHashSet(session.getSubstitutionSettings().getExclusions());
    final Set<String> inclusions = Sets.newHashSet(session.getSubstitutionSettings().getInclusions());
    final boolean hasInclusions = !inclusions.isEmpty();
    final List<DremioMaterialization> materializations = Lists.newArrayList();
    for (final MaterializationDescriptor descriptor : provider.get()) {

      if(
          (hasInclusions && !inclusions.contains(descriptor.getLayoutId()))
          ||
          exclusions.contains(descriptor.getLayoutId())
         ) {
          continue;
      }

      try {
        if (session.getSubstitutionSettings().isExcludeFileBasedIncremental() && descriptor.getIncrementalUpdateSettings().isFileBasedUpdate()) {
          continue;
        }
        if (!descriptor.isApplicable(queryTablesUsed, queryVdsUsed, externalQueries)) {
          continue;
        }
        final DremioMaterialization materialization = descriptor.getMaterializationFor(converter);
        if (materialization == null) {
          continue;
        }

        mapping.put(TablePath.of(descriptor.getPath()), descriptor);
        materializations.add(materialization);
      } catch (Throwable e) {
        logger.warn("failed to expand materialization {}", descriptor.getMaterializationId(), e);
      }
    }
    this.materializations = materializations;
    return materializations;
  }

  /**
   * Returns available default raw materialization from the given provider and the path for the VDS/PDS
   *
   * @param provider materialization provider.
   * @param path     dataset path
   * @param vdsFields
   * @return materializations used by planner
   */
  @VisibleForTesting
  protected java.util.Optional<DremioMaterialization> getDefaultRawMaterialization(final MaterializationDescriptorProvider provider, NamespaceKey path, List<String> vdsFields) {
    final Set<String> exclusions = Sets.newHashSet(session.getSubstitutionSettings().getExclusions());
    final Set<String> inclusions = Sets.newHashSet(session.getSubstitutionSettings().getInclusions());
    final boolean hasInclusions = !inclusions.isEmpty();
    final java.util.Optional<MaterializationDescriptor> opt = provider.getDefaultRawMaterialization(path, vdsFields);

    if (opt.isPresent()) {
      MaterializationDescriptor descriptor = opt.get();
      if (
        !(
          (hasInclusions && !inclusions.contains(descriptor.getLayoutId()))
            ||
            exclusions.contains(descriptor.getLayoutId())
        )
      ) {
        try {
          return java.util.Optional.of(descriptor.getMaterializationFor(converter));
        } catch (Throwable e) {
          logger.warn("Failed to expand materialization {}", descriptor.getMaterializationId(), e);
        }
      }
    }
    return java.util.Optional.empty();
  }

  static final class TablePath {
    private final List<String> path;

    private TablePath(final List<String> path) {
      this.path = path;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TablePath tablePath = (TablePath) o;
      return Objects.equals(path, tablePath.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path);
    }

    public static TablePath of(final List<String> paths) {
      return new TablePath(ImmutableList.copyOf(Preconditions.checkNotNull(paths)));
    }

  }

}
