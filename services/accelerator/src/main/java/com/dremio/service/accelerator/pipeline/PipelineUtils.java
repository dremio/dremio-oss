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
package com.dremio.service.accelerator.pipeline;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.dremio.exec.planner.logical.serialization.serializers.ImmutableCollectionSerializers;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutId;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Utility for pipeline package.
 */
public final class PipelineUtils {
  private static final Kryo kryo;

  static {
    kryo = new Kryo();
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    ImmutableCollectionSerializers.register(kryo);
  }

  /**
   * Clones given instance.
   *
   * The instance class does not need to have a visible default constructor.
   */
  public static <T> T clone(final T instance) {
    // DX-5193: kryo is not thread safe so we should synchronize here.
    // we need to switch using a kryo pool possibly as part of DX-4034. Note for the lucky person who will work on
    // DX-4034, kryo already has a pool implementation.
    synchronized (kryo) {
      return kryo.copy(instance);
    }
  }

  /**
   * LayoutDetails that uses the layout version when checking equality
   */
  public static class VersionedLayoutId {
    private final int version;
    private final String layoutId;

    VersionedLayoutId(Layout layout) {
      this.version = Preconditions.checkNotNull(layout.getVersion());
      this.layoutId = Preconditions.checkNotNull(layout.getId().getId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(version, layoutId);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof VersionedLayoutId)) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      final VersionedLayoutId vld = (VersionedLayoutId) obj;
      return version == vld.version && layoutId.equals(vld.layoutId);
    }
  }

  /**
   * Given an acceleration, this method generates an inverse layout mapping from layout fields to layouts.
   */
  public static Map<VersionedLayoutId, Layout> generateVersionedLayoutMapping(final Acceleration acceleration) {
    normalizeLayouts(acceleration);

    return FluentIterable
      .from(AccelerationUtils.getAllLayouts(acceleration))
      .uniqueIndex(new Function<Layout, VersionedLayoutId>() {
        @Override
        public VersionedLayoutId apply(final Layout layout) {
          return new VersionedLayoutId(layout);
        }
      });
  }

  private static void normalizeLayouts(final Acceleration acceleration) {
    // normalize layouts for mapping
    final ImmutableList<LayoutField> empty = ImmutableList.of();
    for (final Layout layout : AccelerationUtils.getAllLayouts(acceleration)) {
      final LayoutDetails details = layout.getDetails();
      details.setDimensionFieldList(Optional.fromNullable(details.getDimensionFieldList()).or(ImmutableList.<LayoutDimensionField>of()));
      details.setDisplayFieldList(Optional.fromNullable(details.getDisplayFieldList()).or(empty));
      details.setMeasureFieldList(Optional.fromNullable(details.getMeasureFieldList()).or(empty));
      details.setPartitionFieldList(Optional.fromNullable(details.getPartitionFieldList()).or(empty));
      details.setSortFieldList(Optional.fromNullable(details.getSortFieldList()).or(empty));
      details.setDistributionFieldList(Optional.fromNullable(details.getDistributionFieldList()).or(empty));
    }
  }

  /**
   * Given an acceleration, this method generates a mapping from layout id to layouts.
   */
  public static Map<LayoutId, Layout> generateLayoutIdMapping(final Acceleration acceleration) {
    normalizeLayouts(acceleration);
    return FluentIterable
        .from(AccelerationUtils.getAllLayouts(acceleration))
        .uniqueIndex(new Function<Layout, LayoutId>() {
          @Nullable
          @Override
          public LayoutId apply(@Nullable final Layout layout) {
            return layout.getId();
          }
        });
  }

}
