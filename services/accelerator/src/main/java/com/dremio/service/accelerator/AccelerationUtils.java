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
package com.dremio.service.accelerator;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.logical.JdbcRel;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.MaterializationDescriptor.LayoutInfo;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContextDescriptor;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationMetrics;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializatonFailure;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.ParentDatasetDescriptor;
import com.dremio.service.accelerator.proto.VirtualDatasetDescriptor;
import com.dremio.service.accelerator.proto.pipeline.PipelineState;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.proto.TimePeriod;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Collection of helper methods for the accelerator
 *
 */
public final class AccelerationUtils {
   /**
    * Predicate to remove duplicate elements
    */
  public static class DuplicateRemover<T> implements Predicate<T> {

      private final Set<T> set = new HashSet<>();

      @Override
      public boolean apply(T input) {
          boolean flag = set.contains(input);
          if (!flag) {
              set.add(input);
          }
          return !flag;
      }
  }

  private static final Logger logger = LoggerFactory.getLogger(AccelerationUtils.class);
  private static final LayoutContainer EMPTY_CONTAINER = new LayoutContainer();

  private AccelerationUtils() {}

  public static long toMillis(final TimePeriod period) {
    final long duration = period.getDuration();
    final TimePeriod.TimeUnit unit = period.getUnit();
    switch (unit) {
      case SECONDS:
        return TimeUnit.SECONDS.toMillis(duration);
      case MINUTES:
        return TimeUnit.MINUTES.toMillis(duration);
      case HOURS:
        return TimeUnit.HOURS.toMillis(duration);
      case DAYS:
        return TimeUnit.DAYS.toMillis(duration);
      case WEEKS:
        return TimeUnit.DAYS.toMillis(7 * duration);
      case MONTHS:
        return TimeUnit.DAYS.toMillis(30 * duration);
      default:
        throw new UnsupportedOperationException(String.format("unsupported unit: %s", unit));
    }
  }

  public static List<Materialization> getAllMaterializations(final Optional<MaterializedLayout> materializedLayout) {
    return materializedLayout
        .transform(new Function<MaterializedLayout, List<Materialization>>() {
          @Nullable
          @Override
          public List<Materialization> apply(@Nullable final MaterializedLayout input) {
            return AccelerationUtils.selfOrEmpty(input.getMaterializationList());
          }
        })
        .or(ImmutableList.<Materialization>of());
  }

  /**
   * Normalizes given descriptor replacing null lists with empty ones
   */
  public static AccelerationDescriptor normalize(final AccelerationDescriptor descriptor) {
    final AccelerationContextDescriptor context = descriptor.getContext();
    final VirtualDatasetDescriptor virtual = context.getDataset().getVirtualDataset();
    if (virtual != null) {
      virtual.setParentList(Optional.fromNullable(virtual.getParentList()).or(ImmutableList.<ParentDatasetDescriptor>of()));
    }

    final LogicalAggregationDescriptor logical = context.getLogicalAggregation();
    logical
        .setDimensionList(Optional.fromNullable(logical.getDimensionList()).or(ImmutableList.<LayoutFieldDescriptor>of()))
        .setMeasureList(Optional.fromNullable(logical.getMeasureList()).or(ImmutableList.<LayoutFieldDescriptor>of()));

    final LayoutContainerDescriptor raw = descriptor.getRawLayouts();
    final LayoutContainerDescriptor agg = descriptor.getAggregationLayouts();

    raw.setLayoutList(Optional.fromNullable(raw.getLayoutList()).or(ImmutableList.<LayoutDescriptor>of()));
    agg.setLayoutList(Optional.fromNullable(agg.getLayoutList()).or(ImmutableList.<LayoutDescriptor>of()));

    for (final LayoutDescriptor layout : Iterables.concat(raw.getLayoutList(), agg.getLayoutList())) {
      final LayoutDetailsDescriptor details = layout.getDetails();
      details.setDimensionFieldList(Optional.fromNullable(details.getDimensionFieldList()).or(ImmutableList.<LayoutDimensionFieldDescriptor>of()));
      details.setDisplayFieldList(Optional.fromNullable(details.getDisplayFieldList()).or(ImmutableList.<LayoutFieldDescriptor>of()));
      details.setMeasureFieldList(Optional.fromNullable(details.getMeasureFieldList()).or(ImmutableList.<LayoutFieldDescriptor>of()));
      details.setPartitionFieldList(Optional.fromNullable(details.getPartitionFieldList()).or(ImmutableList.<LayoutFieldDescriptor>of()));
      details.setSortFieldList(Optional.fromNullable(details.getSortFieldList()).or(ImmutableList.<LayoutFieldDescriptor>of()));
      details.setDistributionFieldList(Optional.fromNullable(details.getDistributionFieldList()).or(ImmutableList.<LayoutFieldDescriptor>of()));

      // make sure dimensions are unique
      details.setDimensionFieldList(makeDistinctDimensions(details.getDimensionFieldList()));
    }
    return descriptor;
  }

  private static List<LayoutDimensionFieldDescriptor> makeDistinctDimensions(final Iterable<LayoutDimensionFieldDescriptor> descriptors) {
    final Set<String> seen = Sets.newHashSet();
    final List<LayoutDimensionFieldDescriptor> dimensions = Lists.newArrayList();
    for (final LayoutDimensionFieldDescriptor dimension : descriptors) {
      if (seen.contains(dimension.getName())) {
        continue;
      }

      dimensions.add(dimension);
      seen.add(dimension.getName());
    }

    return ImmutableList.copyOf(dimensions);
  }

  public static <T> List<T> selfOrEmptyCollection(final Collection<T> iterable) {
    if (iterable == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(iterable);
  }

  /**
   * Returns the iterable as list if not null or an empty list.
   */
  public static <T> List<T> selfOrEmpty(final List<T> iterable) {
    if (iterable == null) {
      return ImmutableList.of();
    }
    return iterable;
  }

  public static Iterable<Layout> getAllLayouts(final Acceleration acceleration) {
    final LayoutContainer aggContainer = Optional.fromNullable(acceleration.getAggregationLayouts()).or(EMPTY_CONTAINER);
    final LayoutContainer rawContainer = Optional.fromNullable(acceleration.getRawLayouts()).or(EMPTY_CONTAINER);

    return Iterables.concat(AccelerationUtils.selfOrEmpty(aggContainer.getLayoutList()),
        AccelerationUtils.selfOrEmpty(rawContainer.getLayoutList()));
  }

  public static boolean anyLayoutNeedPlanning(final Acceleration acceleration) {
    final Iterable<Layout> layouts = getAllLayouts(acceleration);
    for (final Layout layout : layouts) {
      if (layout.getLogicalPlan() == null || layout.getLayoutSchema() == null) {
        return true;
      }
      if (layout.getIncremental() && layout.getRefreshField() == null) {
        return true;
      }
    }
    return false;
  }

  public static Iterable<Layout> allLayouts(final Acceleration acceleration) {
    final LayoutContainer aggContainer = Optional.fromNullable(acceleration.getAggregationLayouts()).or(EMPTY_CONTAINER);
    final LayoutContainer rawContainer = Optional.fromNullable(acceleration.getRawLayouts()).or(EMPTY_CONTAINER);

    final Iterable<Layout> aggLayouts = AccelerationUtils.selfOrEmpty(aggContainer.getLayoutList());
    final Iterable<Layout> rawLayouts = AccelerationUtils.selfOrEmpty(rawContainer.getLayoutList());

    return Iterables.concat(aggLayouts, rawLayouts);
  }

  public static Iterable<Layout> allActiveLayouts(final Acceleration acceleration) {
    final LayoutContainer aggContainer = Optional.fromNullable(acceleration.getAggregationLayouts()).or(EMPTY_CONTAINER);
    final LayoutContainer rawContainer = Optional.fromNullable(acceleration.getRawLayouts()).or(EMPTY_CONTAINER);

    final Iterable<Layout> aggLayouts = AccelerationUtils.selfOrEmpty(aggContainer.getLayoutList());
    final Iterable<Layout> rawLayouts = AccelerationUtils.selfOrEmpty(rawContainer.getLayoutList());

    if (aggContainer.getEnabled() && rawContainer.getEnabled()) {
      return Iterables.concat(aggLayouts, rawLayouts);
    }

    if (aggContainer.getEnabled()) {
      return aggLayouts;
    }

    if (rawContainer.getEnabled()) {
      return rawLayouts;
    }

    return ImmutableList.of();
  }

  public static Iterable<Acceleration> getAllAccelerations(final AccelerationService service) {
    final IndexedStore.FindByCondition condition = new IndexedStore.FindByCondition()
        .setLimit(Integer.MAX_VALUE)
        .setCondition(SearchQueryUtils.newMatchAllQuery())
        .setOffset(0);
    return service.getAccelerations(condition);
  }

  public static <T> T getOrFailUnchecked(final Optional<T> entry, final String message) throws IllegalStateException {
    if (entry.isPresent()) {
      return entry.get();
    }

    throw new IllegalStateException(message);
  }

  public static boolean isPipelineDone(final Acceleration acceleration) {
    final PipelineState state = acceleration.getPipeline().getState();
    switch (state) {
      case DONE:
      case FAILED:
        return true;
      default:
        return false;
    }
  }

  public static String makePathString(final List<String> paths) {
    return new NamespaceKey(paths).getSchemaPath();
  }

  /**
   * Returns stack trace as a list of stack frames
   */
  public static String getStackTrace(final Throwable ex) {
    final StringWriter writer = new StringWriter();
    ex.printStackTrace(new PrintWriter(writer));
    return writer.toString();
  }

  /**
   * Materializes an exception for a layout
   * @param layoutId
   * @param ex
   * @param materializationStore
   */
  public static void saveException(final LayoutId layoutId, final Throwable ex, final MaterializationStore materializationStore) {
    try {
      //create materialization with the error message and stack trace
      final Materialization materialization = new Materialization()
          // unique materialization id
          .setId(new MaterializationId(UUID.randomUUID().toString()))
          .setState(MaterializationState.FAILED)
          // owning layout id
          .setLayoutId(layoutId)
          .setFailure(new MaterializatonFailure()
              .setMessage(ex.getMessage())
              .setStackTrace(AccelerationUtils.getStackTrace(ex)));

      //get the MaterializedLayout for the layout, creates one if not present
      final MaterializedLayout materializedLayout = materializationStore
          .get(layoutId)
          .or(new Supplier<MaterializedLayout>() {
            @Override
            public MaterializedLayout get() {
              final MaterializedLayout materializedLayout = new MaterializedLayout()
                  .setLayoutId(layoutId)
                  .setMaterializationList(ImmutableList.of(materialization));
              materializationStore.save(materializedLayout);
              return materializedLayout;
            }
          });
      //add the the new materialization to the list and save it
      final List<Materialization> materializations = Lists.newArrayList(AccelerationUtils.selfOrEmpty(materializedLayout.getMaterializationList()));
      materializations.add(materialization);
      materializedLayout.setMaterializationList(materializations);
      materializationStore.save(materializedLayout);
    } catch (Throwable e) {
      logger.warn("Exception while materializing Exception" , e);
    }
  }

  public static LayoutId newRandomId() {
    return new LayoutId(UUID.randomUUID().toString());
  }
  /**
   * Creates and returns MaterializationDescriptor
   * @param acceleration
   * @param layout
   * @param materialization
   * @return
   */
  static MaterializationDescriptor getMaterializationDescriptor(final Acceleration acceleration,
      final Layout layout, final Materialization materialization, final boolean complete) {
    final byte[] planBytes = layout.getLogicalPlan().toByteArray();
    final List<String> fullPath = ImmutableList.<String>builder()
        .addAll(materialization.getPathList())
        .build();
    return new MaterializationDescriptor(
        acceleration.getId().getId(),
        AccelerationUtils.toLayoutInfo(layout),
        materialization.getId().getId(),
        materialization.getUpdateId(),
        materialization.getExpiration(),
        planBytes,
        fullPath,
        Optional.fromNullable(materialization.getMetrics()).or(new MaterializationMetrics()).getOriginalCost(),
        new IncrementalUpdateSettings(Optional.fromNullable(layout.getIncremental()).or(false), layout.getRefreshField()),
        complete
        );
  }

  private static LayoutInfo toLayoutInfo(Layout layout){
    String id = layout.getId().getId();
    LayoutDetails details = layout.getDetails();
    return new LayoutInfo(id,
        layout.getName(),
        getNames(Optional.fromNullable(details.getSortFieldList()).or(Collections.<LayoutField>emptyList())),
        getNames(Optional.fromNullable(details.getPartitionFieldList()).or(Collections.<LayoutField>emptyList())),
        getNames(Optional.fromNullable(details.getDistributionFieldList()).or(Collections.<LayoutField>emptyList())),
        getDimensionNames(Optional.fromNullable(details.getDimensionFieldList()).or(Collections.<LayoutDimensionField>emptyList())),
        getNames(Optional.fromNullable(details.getMeasureFieldList()).or(Collections.<LayoutField>emptyList())),
        getNames(Optional.fromNullable(details.getDisplayFieldList()).or(Collections.<LayoutField>emptyList()))
    );
  }

  private static List<String> getNames(List<LayoutField> fields){
    return FluentIterable.from(fields).transform(new Function<LayoutField, String>(){
      @Override
      public String apply(LayoutField input) {
        return input.getName();
      }}).toList();
  }

  private static List<String> getDimensionNames(List<LayoutDimensionField> fields){
    return FluentIterable.from(fields).transform(new Function<LayoutDimensionField, String>(){
      @Nullable
      @Override
      public String apply(@Nullable LayoutDimensionField input) {
        return input.getName();
      }
    }).toList();
  }

  /**
   * Obtain the scans based on the plan
   * @param logicalPlan
   * @param acceleratorStorageName
   * @return
   */
  public static List<List<String>> getScans(RelNode logicalPlan, final String acceleratorStorageName) {
    final ImmutableList.Builder<List<String>> builder = ImmutableList.builder();
    logicalPlan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(final TableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        if (!qualifiedName.get(0).equals(acceleratorStorageName)) {
          builder.add(qualifiedName);
        }
        return super.visit(scan);
      }

      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof JdbcRel) {
          JdbcRel jdbcRel = (JdbcRel)other;
          visit(jdbcRel.getJdbcSubTree());
        }
        return super.visit(other);
      }
    });
    return builder.build();
  }

}
