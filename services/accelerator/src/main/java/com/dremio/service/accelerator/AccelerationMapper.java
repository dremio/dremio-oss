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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nullable;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationContextDescriptor;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.DatasetConfigDescriptor;
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
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.accelerator.proto.ParentDatasetDescriptor;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.accelerator.proto.VirtualDatasetDescriptor;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

/**
 * A utility for mapping acceleration instances to their API counter part.
 */
public final class AccelerationMapper {

  public static final Function<Layout, LayoutDescriptor> TO_LAYOUT_DESCRIPTOR = new Function<Layout, LayoutDescriptor>() {
    @Nullable
    @Override
    public LayoutDescriptor apply(@Nullable final Layout layout) {
      return toLayoutDescriptor(layout);
    }
  };

  private final Map<String, ViewFieldType> fields;

  private AccelerationMapper(final Map<String, ViewFieldType> fields) {
    this.fields = Preconditions.checkNotNull(fields, "fields is required");
  }

  // service messages to descriptors


  public static AccelerationDescriptor toAccelerationDescriptor(final Acceleration acceleration) {
    final AccelerationContext context = Preconditions.checkNotNull(acceleration.getContext(), "context is required");
    final DatasetConfig config = Preconditions.checkNotNull(context.getDataset(), "dataset config is required");

    final VirtualDatasetDescriptor virtualDataset = new VirtualDatasetDescriptor();
    final boolean isVirtual = config.getType() == DatasetType.VIRTUAL_DATASET;
    if (isVirtual) {
      virtualDataset
          .setSql(config.getVirtualDataset().getSql())
          .setParentList(
              FluentIterable
                  .from(AccelerationUtils.selfOrEmpty(config.getVirtualDataset().getParentsList()))
                  .transform(new Function<ParentDataset, ParentDatasetDescriptor>() {
                    @Nullable
                    @Override
                    public ParentDatasetDescriptor apply(@Nullable final ParentDataset parent) {
                      return new ParentDatasetDescriptor()
                          .setType(parent.getType())
                          .setPathList(parent.getDatasetPathList());
                    }
                  })
                  .toList()
          )
      ;
    }

    final AccelerationStateDescriptor state = toStateDescriptor(acceleration.getState());

    final LayoutContainer empty = new LayoutContainer();
    final LayoutContainer aggLayouts = Optional.fromNullable(acceleration.getAggregationLayouts()).or(empty);
    final LayoutContainer rawLayouts = Optional.fromNullable(acceleration.getRawLayouts()).or(empty);

    final LogicalAggregation aggregation = Optional.fromNullable(context.getLogicalAggregation()).or(new LogicalAggregation());

    final AccelerationDescriptor descriptor =  new AccelerationDescriptor()
        .setId(acceleration.getId())
        .setType(acceleration.getType())
        .setMode(acceleration.getMode())
        .setState(state)
        .setContext(
            new AccelerationContextDescriptor()
                .setDatasetSchema(context.getDatasetSchema())
                .setJobId(context.getJobId())
                .setDataset(
                    new DatasetConfigDescriptor()
                        .setType(config.getType())
                        .setVersion(config.getVersion())
                        .setCreatedAt(config.getCreatedAt())
                        .setVirtualDataset(isVirtual ? virtualDataset : null)
                )
                .setLogicalAggregation(toLogicalAggregationDescriptor(aggregation))
        )
        .setAggregationLayouts(
            new LayoutContainerDescriptor()
                .setType(LayoutType.AGGREGATION)
                .setEnabled(aggLayouts.getEnabled())
                .setLayoutList(toLayoutDescriptors(aggLayouts.getLayoutList()))
        )
        .setRawLayouts(
            new LayoutContainerDescriptor()
                .setType(LayoutType.RAW)
                .setEnabled(rawLayouts.getEnabled())
                .setLayoutList(toLayoutDescriptors(rawLayouts.getLayoutList()))
        )
        ;

    return AccelerationUtils.normalize(descriptor);
  }

  public static LogicalAggregationDescriptor toLogicalAggregationDescriptor(final LogicalAggregation aggregation) {
    return new LogicalAggregationDescriptor()
        .setDimensionList(toLayoutFieldDescriptors(aggregation.getDimensionList()))
        .setMeasureList(toLayoutFieldDescriptors(aggregation.getMeasureList()));
  }

  public static AccelerationStateDescriptor toStateDescriptor(final AccelerationState state) {
    switch (state) {
      case NEW_ADMIN:
      case NEW_SYSTEM:
      case NEW_USER:
        return AccelerationStateDescriptor.NEW;
      case REQUESTED:
        return AccelerationStateDescriptor.REQUESTED;
      case ENABLED:
        return AccelerationStateDescriptor.ENABLED;
      case DISABLED:
        return AccelerationStateDescriptor.DISABLED;
      case ERROR:
        return AccelerationStateDescriptor.ERROR;
      default:
        throw new IllegalArgumentException(String.format("unsupported acceleration state: %s", state.name()));
    }
  }

  public static List<LayoutDescriptor> toLayoutDescriptors(final List<Layout> layouts) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(layouts))
        .transform(TO_LAYOUT_DESCRIPTOR)
        .toList();
  }

  public static LayoutDescriptor toLayoutDescriptor(final Layout layout) {
    final LayoutDetails details = Preconditions.checkNotNull(layout.getDetails(), "layout details is required");

    return new LayoutDescriptor()
        .setId(layout.getId())
        .setName(layout.getName())
        .setDetails(
            new LayoutDetailsDescriptor()
                .setPartitionFieldList(toLayoutFieldDescriptors(details.getPartitionFieldList()))
                .setDimensionFieldList(toLayoutDimensionFieldDescriptors(details.getDimensionFieldList()))
                .setMeasureFieldList(toLayoutFieldDescriptors(details.getMeasureFieldList()))
                .setSortFieldList(toLayoutFieldDescriptors(details.getSortFieldList()))
                .setDisplayFieldList(toLayoutFieldDescriptors(details.getDisplayFieldList()))
                .setDistributionFieldList(toLayoutFieldDescriptors(details.getDistributionFieldList()))
                .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
        );

  }

  public static List<LayoutFieldDescriptor> toLayoutFieldDescriptors(final List<LayoutField> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutField, LayoutFieldDescriptor>() {
          @Nullable
          @Override
          public LayoutFieldDescriptor apply(@Nullable final LayoutField field) {
            return toLayoutFieldDescriptor(field);
          }
        })
        .toList();
  }

  public static List<LayoutDimensionFieldDescriptor> toLayoutDimensionFieldDescriptors(final List<LayoutDimensionField> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutDimensionField, LayoutDimensionFieldDescriptor>() {
          @Nullable
          @Override
          public LayoutDimensionFieldDescriptor apply(@Nullable final LayoutDimensionField input) {
            return new LayoutDimensionFieldDescriptor()
                .setName(input.getName())
                .setGranularity(input.getGranularity());
          }
        })
        .toList();
  }


  public static LayoutFieldDescriptor toLayoutFieldDescriptor(final LayoutField field) {
    return new LayoutFieldDescriptor().setName(field.getName());
  }

  // descriptor to service messages

  public LogicalAggregation toLogicalAggregation(final LogicalAggregationDescriptor descriptor) {
    return new LogicalAggregation()
        .setDimensionList(toLayoutFields(descriptor.getDimensionList()))
        .setMeasureList(toLayoutFields(descriptor.getMeasureList()));
  }


  public List<LayoutField> toLayoutFields(final List<LayoutFieldDescriptor> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutFieldDescriptor, LayoutField>() {
          @Nullable
          @Override
          public LayoutField apply(@Nullable final LayoutFieldDescriptor field) {
            return toLayoutField(field);
          }
        })
        .toList();
  }

  public List<LayoutDimensionField> toLayoutDimensionFields(final List<LayoutDimensionFieldDescriptor> fields) {
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutDimensionFieldDescriptor, LayoutDimensionField>() {
          @Nullable
          @Override
          public LayoutDimensionField apply(@Nullable final LayoutDimensionFieldDescriptor field) {
            final LayoutField newField = toLayoutField(new LayoutFieldDescriptor().setName(field.getName()));
            return new LayoutDimensionField()
                .setName(newField.getName())
                .setTypeFamily(newField.getTypeFamily())
                .setGranularity(field.getGranularity());
          }
        })
        .toList();
  }

  public LayoutField toLayoutField(final LayoutFieldDescriptor descriptor) {
    final ViewFieldType field = fields.get(descriptor.getName());
    if (field == null) {
      throw new IllegalArgumentException(String.format("unable to find field: %s. make sure it is in dataset schema",
          descriptor.getName()));
    }

    return new LayoutField()
        .setName(descriptor.getName())
        .setTypeFamily(field.getTypeFamily());
  }

  public static String newRandomId() {
    return UUID.randomUUID().toString();
  }

  public static AccelerationMapper create(final RowType schema) {
    final Map<String, ViewFieldType> fields = FluentIterable.from(AccelerationUtils.selfOrEmpty(schema.getFieldList()))
        .uniqueIndex(new Function<ViewFieldType, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final ViewFieldType input) {
            return input.getName();
          }
        });

    return new AccelerationMapper(fields);
  }
}
