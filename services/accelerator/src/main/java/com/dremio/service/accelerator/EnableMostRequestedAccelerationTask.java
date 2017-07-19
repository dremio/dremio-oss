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

import static com.dremio.service.accelerator.store.AccelerationIndexKeys.ACCELERATION_STATE;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.SystemSettings;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

/**
 * A scheduled task that enables most requested accelerations.
 */
public class EnableMostRequestedAccelerationTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(EnableMostRequestedAccelerationTask.class);

  private static final Function<Acceleration, AccelerationId> INDEXER = new Function<Acceleration, AccelerationId>() {
    @Nullable
    @Override
    public AccelerationId apply(@Nullable final Acceleration acceleration) {
      return acceleration.getId();
    }
  };

  private final AccelerationServiceImpl accelerationService;

  public EnableMostRequestedAccelerationTask(final AccelerationServiceImpl accelerationService) {
    this.accelerationService = accelerationService;
  }

  @Override
  public void run() {
    // read acceleration settings
    final SystemSettings settings = accelerationService.getSettings();
    final int limit = settings.getLimit();
    final boolean enableAggregation = settings.getAccelerateAggregation();
    final boolean enableRaw = settings.getAccelerateRaw();


    // find existing system accelerations
    final IndexedStore.FindByCondition findExisting = new IndexedStore.FindByCondition()
        .setCondition(SearchQueryUtils.newTermQuery(ACCELERATION_STATE, AccelerationState.ENABLED_SYSTEM.toString()))
        .setLimit(Integer.MAX_VALUE)
        .setPageSize(Integer.MAX_VALUE);

    final Map<AccelerationId, Acceleration> current = FluentIterable
        .from(accelerationService.getAccelerations(findExisting))
        .uniqueIndex(INDEXER);

    // find candidate accelerations to enable such that acceleration must not be already ENABLED
    final SearchQuery candidateQuery = SearchQueryUtils.not(SearchQueryUtils.newTermQuery(AccelerationIndexKeys.ACCELERATION_STATE, AccelerationState.ENABLED.toString()));
    final IndexedStore.FindByCondition findCandidates = new IndexedStore.FindByCondition()
        .setCondition(candidateQuery)
        .addSorting(AccelerationIndexKeys.TOTAL_REQUESTS.toSortField(SortOrder.DESCENDING))
        .setLimit(limit)
        .setPageSize(limit);

    final Map<AccelerationId, Acceleration> candidates =  FluentIterable
        .from(accelerationService.getAccelerations(findCandidates))
        .uniqueIndex(INDEXER);

    // find accelerations whose materializations will be dropped from top list
    final Set<AccelerationId> drop = Sets.difference(current.keySet(), candidates.keySet());
    // find accelerations whose materializations will be added to top list
    final Set<AccelerationId> add = Sets.difference(candidates.keySet(), current.keySet());

    for (final AccelerationId id : drop) {
      accelerationService.remove(id);
    }

    for (final AccelerationId id : add) {
      final String msg = String.format("acceleration[id:%s] not found", id.getId());
      final AccelerationEntry entry = AccelerationUtils.getOrFailUnchecked(accelerationService.getAccelerationEntryById(id), msg);
      final AccelerationDescriptor descriptor = entry.getDescriptor();
      descriptor.setState(AccelerationStateDescriptor.ENABLED_SYSTEM);
      descriptor.getAggregationLayouts().setEnabled(enableAggregation);
      descriptor.getRawLayouts().setEnabled(enableRaw);
      accelerationService.update(entry);
    }
  }
}
