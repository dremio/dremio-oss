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
package com.dremio.service.accelerator;


import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.proto.TimePeriod;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Collection of helper methods for the accelerator
 *
 */
public final class AccelerationUtils {

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

  public static <T> List<T> selfOrEmptyCollection(final Collection<T> iterable) {
    if (iterable == null) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(iterable);
  }

  public static <T> Stream<T> selfOrEmpty(final Stream<T> stream) {
    if (stream == null) {
      return Stream.of();
    }
    return stream;
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

  public static String makePathString(final List<String> paths) {
    return new NamespaceKey(paths).getSchemaPath();
  }

}
