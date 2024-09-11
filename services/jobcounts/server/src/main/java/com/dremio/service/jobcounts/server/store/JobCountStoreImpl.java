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
package com.dremio.service.jobcounts.server.store;

import com.dremio.common.util.Retryer;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.format.Format;
import com.dremio.service.jobcounts.DailyJobCount;
import com.dremio.service.jobcounts.JobCountInfo;
import com.dremio.service.jobcounts.JobCountType;
import com.dremio.service.jobcounts.JobCountUpdate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Provider;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jobs count store Implementation. */
public class JobCountStoreImpl implements JobCountStore {
  private static final Logger logger = LoggerFactory.getLogger(JobCountStoreImpl.class);
  private final Provider<KVStoreProvider> storeProvider;
  private Supplier<IndexedStore<String, JobCountInfo>> store;
  private static final int RETRY_LIMIT = 5;
  public static final int COUNTS_SIZE = 30; // max age of job counts (in days)
  private static final long COUNTS_SIZE_DAYS_IN_MILLIS = TimeUnit.DAYS.toMillis(COUNTS_SIZE);
  private final Retryer retryer;

  public JobCountStoreImpl(Provider<KVStoreProvider> storeProvider) {
    this.storeProvider = storeProvider;
    this.retryer =
        Retryer.newBuilder()
            .retryIfExceptionOfType(ConcurrentModificationException.class)
            .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 2500)
            .setMaxRetries(RETRY_LIMIT)
            .build();
  }

  @Override
  public void start() throws Exception {
    store = Suppliers.memoize(() -> storeProvider.get().getStore(JobCountStoreCreator.class));
  }

  /**
   * Please note: This is overridden (customized) in derived class. Do not use store directly,
   * instead use this method.
   *
   * @return
   */
  protected IndexedStore<String, JobCountInfo> getStore() {
    return store.get();
  }

  @Override
  public int getCount(String id, JobCountType type, int jobCountAgeInDays) {
    Document<String, JobCountInfo> doc = getStore().get(id);
    if (doc != null) {
      return calculateCount(
          doc.getValue(),
          type,
          System.currentTimeMillis() - TimeUnit.DAYS.toMillis(jobCountAgeInDays));
    }
    return 0;
  }

  @Override
  public List<Integer> getCounts(List<String> ids, JobCountType type, int jobCountAgeInDays) {
    Iterable<Document<String, JobCountInfo>> docs = getStore().get(ids);

    List<Integer> res = new ArrayList<>(ids.size());
    for (Document<String, JobCountInfo> doc : docs) {
      if (doc != null) {
        res.add(
            calculateCount(
                doc.getValue(),
                type,
                System.currentTimeMillis() - TimeUnit.DAYS.toMillis(jobCountAgeInDays)));
      } else {
        res.add(0);
      }
    }
    return res;
  }

  @Override
  public void updateCount(String id, JobCountType type, long currTimeMillis) {
    updateCountWithRetry(id, type, currTimeMillis);
  }

  @Override
  public void bulkUpdateCount(List<JobCountUpdate> countUpdates) {
    long currTimeMillis = System.currentTimeMillis();
    for (JobCountUpdate jc : countUpdates) {
      updateCount(jc.getId(), jc.getType(), currTimeMillis);
    }
  }

  @Override
  public void deleteCount(String id) {
    store.get().delete(id);
  }

  @Override
  public void bulkDeleteCount(List<String> countDeleteIds) {
    for (String id : countDeleteIds) {
      deleteCount(id);
    }
  }

  private void updateCountWithRetry(String id, JobCountType type, long currTimeMillis) {
    retryer.call(
        () -> {
          calculateAndUpdateCount(id, type, currTimeMillis);
          return null;
        });
  }

  // returns true if current update is on same day as the last update of the entry
  public boolean isUpdateOnSameDay(JobCountInfo info) {
    Calendar entryDate = Calendar.getInstance();
    entryDate.setTimeInMillis(info.getDailyCount(COUNTS_SIZE - 1).getTimeStamp());
    Calendar currDate = Calendar.getInstance();

    // if currDate is older than entryDate, always update count on entryDate, otherwise it'll cause
    // issues when updates
    // are in order like Jan 1 11:59PM, Jan 2 00:01AM, Jan 1 11:59PM
    // Otherwise
    // 2nd condition checks if last entry & current entry timestamp difference is <1Day, if it's
    // false they are guaranteed to be different days
    // 3rd check ensures that 11:59PM and 00:01AM end up on different days
    return currDate.getTimeInMillis() <= entryDate.getTimeInMillis()
        || (currDate.getTimeInMillis() - entryDate.getTimeInMillis() < TimeUnit.DAYS.toMillis(1)
            && currDate.get(Calendar.DAY_OF_MONTH) == entryDate.get(Calendar.DAY_OF_MONTH));
  }

  private void calculateAndUpdateCount(String id, JobCountType type, long currTimeMillis) {
    Document<String, JobCountInfo> doc = getStore().get(id);
    JobCountInfo info;
    if (doc == null) {
      // new entry
      List<DailyJobCount> list = new ArrayList<>(COUNTS_SIZE);
      long oldTs = currTimeMillis - COUNTS_SIZE_DAYS_IN_MILLIS;
      for (int i = 0; i < COUNTS_SIZE - 1; i++) {
        list.add(DailyJobCount.newBuilder().setTimeStamp(oldTs).build());
      }
      list.add(createDailyJobCount(type, currTimeMillis));
      info =
          JobCountInfo.newBuilder()
              .setId(id)
              .addAllDailyCount(list)
              .setLastModifiedAt(currTimeMillis)
              .build();
      getStore().put(id, info, KVStore.PutOption.CREATE);
    } else {
      final ImmutableVersionOption versionOption =
          new ImmutableVersionOption.Builder().setTag(doc.getTag()).build();
      info = doc.getValue();
      if (isUpdateOnSameDay(info)) {
        // existing entry with update on the same day
        DailyJobCount dailyJobCount = info.getDailyCount(COUNTS_SIZE - 1);
        DailyJobCount.JobCountWithType typeCount = dailyJobCount.getTypeCount(type.getNumber());
        typeCount = typeCount.toBuilder().setCount(typeCount.getCount() + 1).build();
        dailyJobCount = dailyJobCount.toBuilder().setTypeCount(type.getNumber(), typeCount).build();
        info =
            info.toBuilder()
                .removeDailyCount(COUNTS_SIZE - 1)
                .addDailyCount(COUNTS_SIZE - 1, dailyJobCount)
                .build();
      } else {
        // existing entry with update on different day
        CircularFifoQueue<DailyJobCount> cirQ = new CircularFifoQueue<>(info.getDailyCountList());
        // removes 0th entry & adds entry for the current day at the end
        cirQ.add(createDailyJobCount(type, currTimeMillis));

        info = info.toBuilder().clearDailyCount().addAllDailyCount(cirQ).build();
      }
      info = info.toBuilder().setLastModifiedAt(currTimeMillis).build();
      getStore().put(id, info, versionOption);
    }
  }

  private int calculateCount(JobCountInfo info, JobCountType type, long cutOffTsInMillis) {
    int res = 0;

    for (int i = COUNTS_SIZE - 1; i >= 0; i--) {
      DailyJobCount dailyJobCount = info.getDailyCount(i);
      if (dailyJobCount.getTimeStamp() > cutOffTsInMillis) {
        res += dailyJobCount.getTypeCount(type.getNumber()).getCount();
      } else {
        break;
      }
    }
    return res;
  }

  private DailyJobCount createDailyJobCount(JobCountType type, long currTimeMillis) {
    DailyJobCount.Builder djcBuilder = DailyJobCount.newBuilder();
    djcBuilder.setTimeStamp(currTimeMillis);
    for (JobCountType jct : JobCountType.values()) {
      if (jct != JobCountType.UNRECOGNIZED) {
        int count = (jct == type) ? 1 : 0;
        djcBuilder.addTypeCount(
            DailyJobCount.JobCountWithType.newBuilder().setType(jct).setCount(count).build());
      }
    }
    return djcBuilder.build();
  }

  @Override
  public void close() throws Exception {}

  /** Creator for JobCountInfo store. */
  public static class JobCountStoreCreator
      implements IndexedStoreCreationFunction<String, JobCountInfo> {
    public static final String NAME = "jobcounts";

    @SuppressWarnings("unchecked")
    @Override
    public IndexedStore<String, JobCountInfo> build(StoreBuildingFactory factory) {
      return factory
          .<String, JobCountInfo>newStore()
          .name(NAME)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtobuf(JobCountInfo.class))
          .buildIndexed(new JobCountConverter());
    }

    private static final class JobCountConverter
        implements DocumentConverter<String, JobCountInfo> {
      private Integer version = 0;

      @Override
      public void convert(DocumentWriter writer, String key, JobCountInfo document) {}

      @Override
      public Integer getVersion() {
        return version;
      }
    }
  }
}
