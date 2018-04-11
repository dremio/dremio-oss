/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.dac.proto.model.acceleration.AccelerationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationContextApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationRequestApiDescriptor;
import com.dremio.dac.proto.model.acceleration.AccelerationStateApiDescriptor;
import com.dremio.dac.proto.model.acceleration.DatasetConfigApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutContainerApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDetailsApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDimensionFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LogicalAggregationApiDescriptor;
import com.dremio.dac.proto.model.acceleration.ParentDatasetApiDescriptor;
import com.dremio.dac.proto.model.acceleration.RowTypeApiDescriptor;
import com.dremio.dac.proto.model.acceleration.VirtualDatasetApiDescriptor;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.AccelerationContextDescriptor;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationRequest;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.DatasetConfigDescriptor;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.accelerator.proto.ParentDatasetDescriptor;
import com.dremio.service.accelerator.proto.PartitionDistributionStrategy;
import com.dremio.service.accelerator.proto.VirtualDatasetDescriptor;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

/**
 * Maps between api and user intent messages.
 */
public class ApiIntentMessageMapper {
  private static final Map<String, RowTypeApiDescriptor.FieldType> API_TYPES = buildTypeMapping();

  private static Map<String, RowTypeApiDescriptor.FieldType> buildTypeMapping() {
    final ImmutableMap.Builder<String, RowTypeApiDescriptor.FieldType> mappings = ImmutableMap.builder();
    for (final SqlTypeName booleanType : SqlTypeName.BOOLEAN_TYPES) {
      mappings.put(booleanType.getName(), RowTypeApiDescriptor.FieldType.BOOLEAN);
    }
    for (final SqlTypeName booleanType : SqlTypeName.BINARY_TYPES) {
      mappings.put(booleanType.getName(), RowTypeApiDescriptor.FieldType.BINARY);
    }
    for (final SqlTypeName booleanType : SqlTypeName.EXACT_TYPES) {
      mappings.put(booleanType.getName(), RowTypeApiDescriptor.FieldType.INTEGER);
    }
    for (final SqlTypeName booleanType : SqlTypeName.APPROX_TYPES) {
      mappings.put(booleanType.getName(), RowTypeApiDescriptor.FieldType.FLOAT);
    }
    for (final SqlTypeName booleanType : SqlTypeName.CHAR_TYPES) {
      mappings.put(booleanType.getName(), RowTypeApiDescriptor.FieldType.TEXT);
    }
    mappings.put(SqlTypeName.TIMESTAMP.getName(), RowTypeApiDescriptor.FieldType.DATETIME);
    mappings.put(SqlTypeName.DATE.getName(), RowTypeApiDescriptor.FieldType.DATE);
    mappings.put(SqlTypeName.TIME.getName(), RowTypeApiDescriptor.FieldType.TIME);
    mappings.put(SqlTypeName.MAP.getName(), RowTypeApiDescriptor.FieldType.MAP);
    mappings.put(SqlTypeName.ARRAY.getName(), RowTypeApiDescriptor.FieldType.LIST);
    mappings.put(SqlTypeName.ANY.getName(), RowTypeApiDescriptor.FieldType.ANY);
    mappings.put(SqlTypeName.OTHER.getName(), RowTypeApiDescriptor.FieldType.OTHER);

    return mappings.build();
  }

  public AccelerationApiDescriptor toApiMessage(final AccelerationDescriptor intent, List<String> datasetPath) {
    final AccelerationContextDescriptor context = Optional.fromNullable(intent.getContext()).or(new AccelerationContextDescriptor());
    final LogicalAggregationDescriptor logicalAggregation = Optional.fromNullable(context.getLogicalAggregation()).or(new LogicalAggregationDescriptor());
    final DatasetConfigDescriptor config = Optional.fromNullable(context.getDataset()).or(new DatasetConfigDescriptor());
    final VirtualDatasetDescriptor virtualDataset = config.getVirtualDataset();
    final LayoutContainerDescriptor rawLayouts = Optional.fromNullable(intent.getRawLayouts()).or(new LayoutContainerDescriptor());
    final LayoutContainerDescriptor aggLayouts = Optional.fromNullable(intent.getAggregationLayouts()).or(new LayoutContainerDescriptor());

    return new AccelerationApiDescriptor()
        .setId(intent.getId())
        .setType(intent.getType())
        .setState(toApiMessage(intent.getState()))
        .setContext(new AccelerationContextApiDescriptor()
            .setDatasetSchema(new RowTypeApiDescriptor()
              .setFieldList(toApiRowTypeFields(context.getDatasetSchema().getFieldList()))
            )
            .setJobId(context.getJobId())
            .setLogicalAggregation(new LogicalAggregationApiDescriptor()
                .setDimensionList(toApiFields(logicalAggregation.getDimensionList()))
                .setMeasureList(toApiFields(logicalAggregation.getMeasureList()))
            )
            .setDataset(new DatasetConfigApiDescriptor()
                .setCreatedAt(config.getCreatedAt())
                .setVersion(config.getVersion())
                .setType(config.getType())
                .setPathList(datasetPath)
                .setVirtualDataset(virtualDataset == null ? null : toApiVirtualDatasetMessage(virtualDataset))
            )
        )
        .setMode(intent.getMode())
        .setVersion(intent.getVersion())
        .setAggregationLayouts(toApiMessage(aggLayouts))
        .setRawLayouts(toApiMessage(rawLayouts))
        .setVersion(intent.getVersion())
        ;
  }

  private List<RowTypeApiDescriptor.Field> toApiRowTypeFields(final List<ViewFieldType> fieldList) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fieldList))
        .transform(new Function<ViewFieldType, RowTypeApiDescriptor.Field>() {
          @Nullable
          @Override
          public RowTypeApiDescriptor.Field apply(@Nullable final ViewFieldType input) {
            return new RowTypeApiDescriptor.Field()
                .setName(input.getName())
                .setType(Optional.fromNullable(API_TYPES.get(input.getType())).or(RowTypeApiDescriptor.FieldType.OTHER))
                .setTypeFamily(input.getTypeFamily());
          }
        })
        .toList();
  }

  private VirtualDatasetApiDescriptor toApiVirtualDatasetMessage(final VirtualDatasetDescriptor descriptor) {
    return new VirtualDatasetApiDescriptor()
        .setSql(descriptor.getSql())
        .setParentList(FluentIterable
            .from(AccelerationUtils.selfOrEmpty(descriptor.getParentList()))
            .transform(new Function<ParentDatasetDescriptor, ParentDatasetApiDescriptor>() {
              @Nullable
              @Override
              public ParentDatasetApiDescriptor apply(@Nullable final ParentDatasetDescriptor input) {
                return new ParentDatasetApiDescriptor()
                    .setType(input.getType())
                    .setPathList(input.getPathList());
              }
            })
            .toList()
        )
        ;
  }

  static LayoutType toOld(ReflectionType type) {
    return LayoutType.valueOf(type.number);
  }

  private static PartitionDistributionStrategy toOld(com.dremio.service.reflection.proto.PartitionDistributionStrategy strategy) {
    return PartitionDistributionStrategy.valueOf(strategy.number);
  }

  public static LayoutApiDescriptor toApiMessage(final ReflectionGoal goal) {
    if (goal == null) {
      return null;
    }

    final ReflectionDetails details = goal.getDetails();
    return new LayoutApiDescriptor()
      .setId(goal.getId().getId())
      .setName(goal.getName())
      .setType(toOld(goal.getType()))
      .setDetails(new LayoutDetailsApiDescriptor()
        .setDisplayFieldList(toApiFields(details.getDisplayFieldList()))
        .setDimensionFieldList(toApiDimensionFields(details.getDimensionFieldList()))
        .setMeasureFieldList(toApiFields(details.getMeasureFieldList()))
        .setPartitionFieldList(toApiFields(details.getPartitionFieldList()))
        .setSortFieldList(toApiFields(details.getSortFieldList()))
        .setDistributionFieldList(toApiFields(details.getDistributionFieldList()))
        .setPartitionDistributionStrategy(toOld(details.getPartitionDistributionStrategy()))
      );
  }

  public static LayoutApiDescriptor toApiMessage(final LayoutDescriptor descriptor, final LayoutType type) {
    if (descriptor == null) {
      return null;
    }

    final LayoutDetailsDescriptor details = descriptor.getDetails();
    return new LayoutApiDescriptor()
      .setId(descriptor.getId().getId())
      .setName(descriptor.getName())
      .setType(type)
      .setDetails(new LayoutDetailsApiDescriptor()
        .setDisplayFieldList(toApiFields(details.getDisplayFieldList()))
        .setDimensionFieldList(toApiDimensionFields(details.getDimensionFieldList()))
        .setMeasureFieldList(toApiFields(details.getMeasureFieldList()))
        .setPartitionFieldList(toApiFields(details.getPartitionFieldList()))
        .setSortFieldList(toApiFields(details.getSortFieldList()))
        .setDistributionFieldList(toApiFields(details.getDistributionFieldList()))
        .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
      );
  }

  private LayoutContainerApiDescriptor toApiMessage(final LayoutContainerDescriptor container) {
    return new LayoutContainerApiDescriptor()
        .setType(container.getType())
        .setEnabled(container.getEnabled())
        .setLayoutList(FluentIterable
            .from(AccelerationUtils.selfOrEmpty(container.getLayoutList()))
            .transform(new Function<LayoutDescriptor, LayoutApiDescriptor>() {
              @Override
              public LayoutApiDescriptor apply(final LayoutDescriptor input) {
                return toApiMessage(input, container.getType());
              }
            })
            .toList()
        );
  }

  private static List<LayoutDimensionFieldApiDescriptor> toApiDimensionFields(final List<LayoutDimensionFieldDescriptor> fields) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutDimensionFieldDescriptor, LayoutDimensionFieldApiDescriptor>() {
          @Nullable
          @Override
          public LayoutDimensionFieldApiDescriptor apply(@Nullable final LayoutDimensionFieldDescriptor input) {
            return new LayoutDimensionFieldApiDescriptor()
                .setName(input.getName())
                .setGranularity(input.getGranularity());
          }
        })
        .toList();
  }

  private static DimensionGranularity toOld(com.dremio.service.reflection.proto.DimensionGranularity granularity) {
    return DimensionGranularity.valueOf(granularity.number);
  }

  private static List<LayoutDimensionFieldApiDescriptor> toApiDimensionFields(final Iterable<ReflectionDimensionField> fields) {
    if (fields == null) {
      return Collections.emptyList();
    }
    return FluentIterable
      .from(fields)
      .transform(new Function<ReflectionDimensionField, LayoutDimensionFieldApiDescriptor>() {
        @Override
        public LayoutDimensionFieldApiDescriptor apply(final ReflectionDimensionField input) {
          return new LayoutDimensionFieldApiDescriptor()
            .setName(input.getName())
            .setGranularity(toOld(input.getGranularity()));
        }
      })
      .toList();
  }

  private static List<LayoutFieldApiDescriptor> toApiFields(final List<LayoutFieldDescriptor> fields) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutFieldDescriptor, LayoutFieldApiDescriptor>() {
          @Nullable
          @Override
          public LayoutFieldApiDescriptor apply(@Nullable final LayoutFieldDescriptor input) {
            return new LayoutFieldApiDescriptor().setName(input.getName());
          }
        })
        .toList();
  }

  private static List<LayoutFieldApiDescriptor> toApiFields(final Iterable<ReflectionField> fields) {
    if (fields == null) {
      return Collections.emptyList();
    }
    return FluentIterable.from(fields)
      .transform(new Function<ReflectionField, LayoutFieldApiDescriptor>() {
        @Override
        public LayoutFieldApiDescriptor apply(final ReflectionField input) {
          return new LayoutFieldApiDescriptor().setName(input.getName());
        }
      })
      .toList();
  }

  public AccelerationStateApiDescriptor toApiMessage(final AccelerationStateDescriptor intent) {
    switch (intent) {
      case DISABLED:
        return AccelerationStateApiDescriptor.DISABLED;
      case ENABLED:
        return AccelerationStateApiDescriptor.ENABLED;
      case ENABLED_SYSTEM:
        return AccelerationStateApiDescriptor.ENABLED_SYSTEM;
      case ERROR:
        return AccelerationStateApiDescriptor.ERROR;
      case NEW:
        return AccelerationStateApiDescriptor.NEW;
      case REQUESTED:
        return AccelerationStateApiDescriptor.REQUESTED;
      case OUT_OF_DATE:
        return AccelerationStateApiDescriptor.OUT_OF_DATE;
      default:
        throw new IllegalArgumentException(String.format("unknown state: %s", intent));
    }
  }

  public AccelerationRequest toIntentMessage(final AccelerationRequestApiDescriptor intent) {
    final AccelerationRequestApiDescriptor.AccelerationRequestContext context = Optional
        .fromNullable(intent.getContext())
        .or(new AccelerationRequestApiDescriptor.AccelerationRequestContext());

    return new AccelerationRequest()
        .setUsername(intent.getUsername())
        .setCreatedAt(intent.getCreatedAt())
        .setType(intent.getType())
        .setContext(new AccelerationRequest.AccelerationRequestContext()
            .setDatasetPathList(context.getDatasetPathList())
            .setJobId(context.getJobId())
        );
  }

  public AccelerationDescriptor toIntentMessage(final AccelerationApiDescriptor descriptor) {
    final AccelerationContextApiDescriptor context = Optional.fromNullable(descriptor.getContext()).or(new AccelerationContextApiDescriptor());
    final LayoutContainerApiDescriptor rawLayouts = Optional.fromNullable(descriptor.getRawLayouts()).or(new LayoutContainerApiDescriptor());
    final LayoutContainerApiDescriptor aggLayouts = Optional.fromNullable(descriptor.getAggregationLayouts()).or(new LayoutContainerApiDescriptor());

    return new AccelerationDescriptor()
        .setId(descriptor.getId())
        .setType(descriptor.getType())
        .setState(toIntentMessage(descriptor.getState()))
        .setMode(descriptor.getMode())
        .setAggregationLayouts(toIntentMessage(aggLayouts))
        .setRawLayouts(toIntentMessage(rawLayouts))
        .setVersion(descriptor.getVersion())
        .setContext(new AccelerationContextDescriptor()
            .setLogicalAggregation(new LogicalAggregationDescriptor()
                .setDimensionList(toIntentFields(context.getLogicalAggregation().getDimensionList()))
                .setMeasureList(toIntentFields(context.getLogicalAggregation().getMeasureList()))
            )
        )
        ;
  }

  private AccelerationStateDescriptor toIntentMessage(final AccelerationStateApiDescriptor state) {
    switch (state) {
      case DISABLED:
        return AccelerationStateDescriptor.DISABLED;
      case ENABLED:
        return AccelerationStateDescriptor.ENABLED;
      case ENABLED_SYSTEM:
        return AccelerationStateDescriptor.ENABLED_SYSTEM;
      case ERROR:
        return AccelerationStateDescriptor.ERROR;
      case NEW:
        return AccelerationStateDescriptor.NEW;
      case REQUESTED:
        return AccelerationStateDescriptor.REQUESTED;
      default:
        throw new IllegalArgumentException(String.format("unknown state: %s", state));
    }
  }

  private LayoutContainerDescriptor toIntentMessage(final LayoutContainerApiDescriptor descriptor) {
    return new LayoutContainerDescriptor()
        .setEnabled(descriptor.getEnabled())
        .setType(descriptor.getType())
        .setLayoutList(FluentIterable
            .from(descriptor.getLayoutList())
            .transform(new Function<LayoutApiDescriptor, LayoutDescriptor>() {
              @Nullable
              @Override
              public LayoutDescriptor apply(@Nullable final LayoutApiDescriptor input) {
                final LayoutDetailsApiDescriptor details = input.getDetails();
                return new LayoutDescriptor()
                    .setId(toLayoutId(input.getId()))
                    .setName(input.getName())
                    .setDetails(new LayoutDetailsDescriptor()
                        .setDisplayFieldList(toIntentFields(details.getDisplayFieldList()))
                        .setMeasureFieldList(toIntentFields(details.getMeasureFieldList()))
                        .setDimensionFieldList(toIntentDimensionFields(details.getDimensionFieldList()))
                        .setPartitionFieldList(toIntentFields(details.getPartitionFieldList()))
                        .setSortFieldList(toIntentFields(details.getSortFieldList()))
                        .setDistributionFieldList(toIntentFields(details.getDistributionFieldList()))
                        .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
                    );
              }
            })
            .toList()
        );
  }

  public static LayoutId toLayoutId(String id) {
    if (id == null) {
      return null;
    }
    return new LayoutId(id);
  }

  private List<LayoutFieldDescriptor> toIntentFields(final List<LayoutFieldApiDescriptor> fields) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutFieldApiDescriptor, LayoutFieldDescriptor>() {
          @Nullable
          @Override
          public LayoutFieldDescriptor apply(@Nullable final LayoutFieldApiDescriptor input) {
            return new LayoutFieldDescriptor()
                .setName(input.getName());
          }
        })
        .toList();
  }

  private List<LayoutDimensionFieldDescriptor> toIntentDimensionFields(final List<LayoutDimensionFieldApiDescriptor> fields) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutDimensionFieldApiDescriptor, LayoutDimensionFieldDescriptor>() {
          @Nullable
          @Override
          public LayoutDimensionFieldDescriptor apply(@Nullable final LayoutDimensionFieldApiDescriptor input) {
            return new LayoutDimensionFieldDescriptor()
                .setName(input.getName())
                .setGranularity(input.getGranularity());
          }
        })
        .toList();
  }

}
