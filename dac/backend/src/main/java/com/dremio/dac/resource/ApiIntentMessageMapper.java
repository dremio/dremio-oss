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
package com.dremio.dac.resource;

import java.util.List;

import javax.annotation.Nullable;

import com.dremio.dac.proto.model.acceleration.LayoutApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDetailsApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutDimensionFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutFieldApiDescriptor;
import com.dremio.dac.proto.model.acceleration.LayoutMeasureFieldApiDescriptor;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutMeasureFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutType;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * Maps between api and user intent messages.
 */
public class ApiIntentMessageMapper {

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
        .setMeasureFieldList(toApiMeasureFields(details.getMeasureFieldList()))
        .setPartitionFieldList(toApiFields(details.getPartitionFieldList()))
        .setSortFieldList(toApiFields(details.getSortFieldList()))
        .setDistributionFieldList(toApiFields(details.getDistributionFieldList()))
        .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
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

  private static List<LayoutMeasureFieldApiDescriptor> toApiMeasureFields(final List<LayoutMeasureFieldDescriptor> fields) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fields))
        .transform(new Function<LayoutMeasureFieldDescriptor, LayoutMeasureFieldApiDescriptor>() {
          @Nullable
          @Override
          public LayoutMeasureFieldApiDescriptor apply(@Nullable final LayoutMeasureFieldDescriptor input) {
            return new LayoutMeasureFieldApiDescriptor()
                .setName(input.getName())
                .setMeasureTypeList(input.getMeasureTypeList());
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

  public static LayoutId toLayoutId(String id) {
    if (id == null) {
      return null;
    }
    return new LayoutId(id);
  }



}
