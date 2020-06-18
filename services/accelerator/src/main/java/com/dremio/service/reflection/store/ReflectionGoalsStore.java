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
package com.dremio.service.reflection.store;

import static com.dremio.datastore.SearchQueryUtils.and;
import static com.dremio.datastore.SearchQueryUtils.newRangeLong;
import static com.dremio.datastore.SearchQueryUtils.newTermQuery;
import static com.dremio.datastore.SearchQueryUtils.or;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.CREATED_AT;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.DATASET_ID;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.REFLECTION_GOAL_MODIFIED_AT;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.REFLECTION_GOAL_STATE;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.REFLECTION_ID;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.REFLECTION_NAME;
import static com.google.common.base.Predicates.notNull;

import java.util.Map;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.util.VisibleForTesting;

import com.dremio.datastore.KVUtil;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;

/**
 * stores the reflection goals
 */
public class ReflectionGoalsStore {
  public static final String TABLE_NAME = "reflection_goals";

  private static final Function<Map.Entry<ReflectionId, ReflectionGoal>, ReflectionGoal> GET_VALUE =
    new Function<Map.Entry<ReflectionId, ReflectionGoal>, ReflectionGoal>() {
      @Override
      public ReflectionGoal apply(@Nullable Map.Entry<ReflectionId, ReflectionGoal> entry) {
        return entry == null ? null : entry.getValue();
      }
    };

  private final Supplier<LegacyIndexedStore<ReflectionId, ReflectionGoal>> store;

  public ReflectionGoalsStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider cannot be null");
    this.store = Suppliers.memoize(new Supplier<LegacyIndexedStore<ReflectionId, ReflectionGoal>>() {
      @Override
      public LegacyIndexedStore<ReflectionId, ReflectionGoal> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
  }

  public void save(ReflectionGoal goal) {
    final long currentTime = System.currentTimeMillis();
    if (goal.getCreatedAt() == null) {
      goal.setCreatedAt(currentTime);
    }
    goal.setModifiedAt(currentTime);
    // Invalidate the old tag, which might have been carried over from a version of Dremio
    // before the KVStore interface revisions (pre 4.2.0).
    goal.setVersion(null);
    store.get().put(goal.getId(), goal);
  }

  public Iterable<ReflectionGoal> getAll() {
    return KVUtil.values(store.get().find());
  }

  public Iterable<ReflectionGoal> getAllNotDeleted() {
    final LegacyFindByCondition condition = new LegacyFindByCondition().setCondition(
      or(
        newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.ENABLED.name()),
        newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.DISABLED.name())
      )).addSortings(ReflectionIndexKeys.DEFAULT_SORT);
    return FluentIterable.from(store.get().find(condition))
      .transform(GET_VALUE)
      .filter(notNull());
  }

  public Iterable<ReflectionGoal> getModifiedOrCreatedSince(final long time) {
    final LegacyFindByCondition condition = new LegacyFindByCondition()
      .setCondition(or(
        newRangeLong(REFLECTION_GOAL_MODIFIED_AT.getIndexFieldName(), time, Long.MAX_VALUE, true, false),
        newRangeLong(CREATED_AT.getIndexFieldName(), time, Long.MAX_VALUE, true, false)
      ));
    return FluentIterable.from(store.get().find(condition))
      .transform(GET_VALUE)
      .filter(notNull());
  }

  public Iterable<ReflectionGoal> getDeletedBefore(final long time) {
    final LegacyFindByCondition condition = new LegacyFindByCondition().setCondition(
      and(
        newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.DELETED.name()),
        newRangeLong(REFLECTION_GOAL_MODIFIED_AT.getIndexFieldName(), Long.MIN_VALUE, time, false, true)));
    return FluentIterable.from(store.get().find(condition))
      .transform(GET_VALUE)
      .filter(notNull());
  }

  public Iterable<ReflectionGoal> getByDatasetId(final String datasetId) {
    final LegacyFindByCondition condition = new LegacyFindByCondition().setCondition(
      and(
        or(
          newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.ENABLED.name()),
          newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.DISABLED.name())
        ),
        newTermQuery(DATASET_ID.getIndexFieldName(), datasetId)));
    return FluentIterable.from(store.get().find(condition))
      .transform(GET_VALUE)
      .filter(notNull());
  }

  public int getEnabledByDatasetId(final String datasetId) {
    return store.get().getCounts(SearchQueryUtils.and(
      newTermQuery(REFLECTION_GOAL_STATE, ReflectionGoalState.ENABLED.name()),
      newTermQuery(DATASET_ID.getIndexFieldName(), datasetId)
    )).get(0);
  }

  public ReflectionGoal get(ReflectionId id) {
    return store.get().get(id);
  }

  public void delete(ReflectionId id) {
    store.get().delete(id);
  }

  @VisibleForTesting
  static final class ReflectionGoalVersionExtractor implements VersionExtractor<ReflectionGoal> {
    @Override
    public String getTag(ReflectionGoal value) {
      return value.getTag();
    }

    @Override
    public void setTag(ReflectionGoal value, String tag) {
      // Back up the existing tag, which might be a number if the reflection goal
      // was upgraded from a build pre-dating the revised KVStore API (pre-4.2.0).
      try {
        // Old tags are long values. Verify that it's convertible to a long before writing it.
        value.setVersion(Long.valueOf(value.getTag()));
      } catch (NumberFormatException ex) {
        // This is normal. It means that we aren't holding a legacy tag in value anymore, so
        // we shouldn't use this.
        value.setVersion(null);
      }

      value.setTag(tag);
    }
  }

  // Verify the given ReflectionGoal matches the given goal version from a separate KV Store.
  // The goal version might have a copy of the tag from a pre-4.2.0 build of Dremio (prior to the
  // KVStore interface changes that separated out the tag from the value).
  public static boolean checkGoalVersion(ReflectionGoal goal, String expectedGoalVersion) {
    Preconditions.checkNotNull(expectedGoalVersion);
    return expectedGoalVersion.equals(goal.getTag()) || expectedGoalVersion.equals(String.valueOf(goal.getVersion()));
  }

  private static final class StoreConverter implements DocumentConverter<ReflectionId, ReflectionGoal> {
    @Override
    public void convert(DocumentWriter writer, ReflectionId key, ReflectionGoal record) {
      writer.write(REFLECTION_ID, key.getId());
      writer.write(DATASET_ID, record.getDatasetId());
      writer.write(CREATED_AT, record.getCreatedAt());
      writer.write(REFLECTION_GOAL_MODIFIED_AT, record.getModifiedAt());
      writer.write(REFLECTION_GOAL_STATE, record.getState().name());
      writer.write(REFLECTION_NAME, record.getName());
    }
  }

  /**
   * Reflection user store creator
   */
  public static class StoreCreator implements LegacyIndexedStoreCreationFunction<ReflectionId, ReflectionGoal> {

    @Override
    public LegacyIndexedStore<ReflectionId, ReflectionGoal> build(LegacyStoreBuildingFactory factory) {
      return factory.<ReflectionId, ReflectionGoal>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.ofProtostuff(ReflectionId.class))
        .valueFormat(Format.ofProtostuff(ReflectionGoal.class))
        .versionExtractor(ReflectionGoalVersionExtractor.class)
        .buildIndexed(new StoreConverter());
    }
  }

}
