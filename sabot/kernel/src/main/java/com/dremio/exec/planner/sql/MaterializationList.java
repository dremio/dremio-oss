/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.sql;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.substitution.MaterializationProvider;

import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An abstraction used to maintain available materializations alongside a mapping from materialization handle to
 * {@link MaterializationDescriptor materialization} itself.
 */
public class MaterializationList implements MaterializationProvider<RelOptMaterialization> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializationList.class);

  private final Map<TablePath, MaterializationDescriptor> mapping = Maps.newHashMap();
  private final Supplier<List<RelOptMaterialization>> factory = Suppliers.memoize(new Supplier<List<RelOptMaterialization>>() {
    @Override
    public List<RelOptMaterialization> get() {
      return build(provider);
    }
  });

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
   * Returns list of available materializations.
   *
   * Note that {@link MaterializationDescriptor descriptors} are converted to {@link RelOptMaterialization materializations}
   * lazily and cached when this method is called the very first time.
   */
  @Override
  public List<RelOptMaterialization> getMaterializations() {
    return factory.get();
  }

  public Optional<MaterializationDescriptor> getDescriptor(final List<String> path) {
    return getDescriptor(TablePath.of(path));
  }

  public Optional<MaterializationDescriptor> getDescriptor(final TablePath path) {
    final MaterializationDescriptor descriptor = mapping.get(path);
    return Optional.fromNullable(descriptor);
  }

  /**
   * Builds materialization table from the given provider and returns list of available materializations.
   *
   * @param provider  materialization provider.
   * @return materializations used by planner
   */
  @VisibleForTesting
  protected List<RelOptMaterialization> build(final MaterializationDescriptorProvider provider) {
    final Set<String> exclusions = Sets.newHashSet(session.getMaterializationSettings().getExclusions());
    final List<RelOptMaterialization> materializations = Lists.newArrayList();
    for (final MaterializationDescriptor descriptor : provider.get(session.getMaterializationSettings().isIncludeIncompleteDatasets())) {
      // skip if materialization is excluded
      // exclusion list is passed by AcceleratorTask
      if (exclusions.contains(descriptor.getAccelerationId()) || exclusions.contains(descriptor.getLayoutId())) {
        continue;
      }

      try {
        final DremioRelOptMaterialization materialization = descriptor.getMaterializationFor(converter);
        if (materialization == null) {
          continue;
        }

        mapping.put(TablePath.of(descriptor.getPath()), descriptor);
        materializations.add(materialization);
      } catch (Throwable e) {
        logger.warn("failed to expand materialization {}", descriptor.getMaterializationId(), e);
      }
    }
    return materializations;
  }


  static class TablePath {
    public final List<String> path;

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
