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
package com.dremio.service.jobcounts.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.jobcounts.JobCountType;
import com.dremio.service.jobcounts.JobCountUpdate;
import com.dremio.service.jobcounts.server.store.JobCountStore;
import com.dremio.service.jobcounts.server.store.JobCountStoreImpl;

/**
 * Test Jobs count store.
 */
public class TestJobCountStore {
  public static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  public static final ScanResult CLASSPATH_SCAN_RESULT =
    ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private static LocalKVStoreProvider kvStoreProvider;
  private static JobCountStore store;

  @BeforeClass
  public static void setup() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    store = new JobCountStoreImpl(() -> kvStoreProvider);
    store.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testNewEntryUpdate() {
    String id = UUID.randomUUID().toString();
    store.updateCount(id, JobCountType.CATALOG, System.currentTimeMillis());
    Assert.assertEquals(1, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testUpdateSameDay() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    store.updateCount(id, JobCountType.CATALOG, currTs);
    store.updateCount(id, JobCountType.CATALOG, currTs);
    store.updateCount(id, JobCountType.CATALOG);
    Assert.assertEquals(3, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testUpdateDifferentDay() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    long currTs2 = currTs - TimeUnit.DAYS.toMillis(2);
    store.updateCount(id, JobCountType.CATALOG, currTs2);
    store.updateCount(id, JobCountType.CATALOG, currTs);
    Assert.assertEquals(2, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testUpdateRotation() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    for (int i=0; i<=29; i++) {
      store.updateCount(id, JobCountType.CATALOG, currTs - TimeUnit.DAYS.toMillis(JobCountStoreImpl.COUNTS_SIZE - i));
    }
    // first update won't be counted as it's timestamp is older than 30days
    Assert.assertEquals(29, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));

    // this update will cause the above first entry to be deleted (as a circular queue is used) and new entry will get added.
    store.updateCount(id, JobCountType.CATALOG, currTs);
    Assert.assertEquals(30, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testOlderUpdate() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    store.updateCount(id, JobCountType.CATALOG, currTs);
    long currTs2 = currTs - TimeUnit.SECONDS.toMillis(10);
    // this update's timestamp is older than previous, so count will be added to previous entry
    store.updateCount(id, JobCountType.CATALOG, currTs2);
    Assert.assertEquals(2, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testGetCount() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    // update older than 30 days (COUNTS_SIZE) won't be counted
    store.updateCount(id, JobCountType.CATALOG, currTs - TimeUnit.DAYS.toMillis(JobCountStoreImpl.COUNTS_SIZE + 1));
    store.updateCount(id, JobCountType.CATALOG, currTs - TimeUnit.DAYS.toMillis(JobCountStoreImpl.COUNTS_SIZE - 5));
    store.updateCount(id, JobCountType.CATALOG, currTs);
    Assert.assertEquals(2, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount("random", JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(id, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testGetCounts() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    store.updateCount(id, JobCountType.CATALOG, currTs);
    List<Integer> counts = store.getCounts(Arrays.asList(id, "random"), JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE);
    Assert.assertEquals(2, counts.size());
    Assert.assertEquals(1, counts.get(0).intValue());
    Assert.assertEquals(0, counts.get(1).intValue());
  }

  @Test
  public void testGetCountsOnAge() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    // update a job count on each day for last 30 days
    for (int i=0; i<=29; i++) {
      store.updateCount(id, JobCountType.CATALOG, currTs - TimeUnit.DAYS.toMillis(JobCountStoreImpl.COUNTS_SIZE - i - 1));
    }

    // verify get job counts based on jobCountsAgeInDays
    Assert.assertEquals(30, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(29, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE-1));
    Assert.assertEquals(28, store.getCount(id, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE-2));
    Assert.assertEquals(2, store.getCount(id, JobCountType.CATALOG, 2));
    Assert.assertEquals(1, store.getCount(id, JobCountType.CATALOG, 1));
  }

  @Test
  public void testReflectionUpdate() {
    String id = UUID.randomUUID().toString();
    store.updateCount(id, JobCountType.MATCHED, System.currentTimeMillis());
    Assert.assertEquals(1, store.getCount(id, JobCountType.MATCHED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(id, JobCountType.CHOSEN, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(id, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testReflectionTypesUpdate() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    store.updateCount(id, JobCountType.MATCHED, currTs);
    store.updateCount(id, JobCountType.CONSIDERED, currTs);
    store.updateCount(id, JobCountType.CONSIDERED, currTs);
    store.updateCount(id, JobCountType.CHOSEN, currTs);
    store.updateCount(id, JobCountType.CHOSEN, currTs);
    store.updateCount(id, JobCountType.CHOSEN, currTs);
    Assert.assertEquals(1, store.getCount(id, JobCountType.MATCHED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(2, store.getCount(id, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(3, store.getCount(id, JobCountType.CHOSEN, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testReflectionTypesUpdate2() {
    String id = UUID.randomUUID().toString();
    long currTs = System.currentTimeMillis();
    store.updateCount(id, JobCountType.MATCHED, currTs);
    store.updateCount(id, JobCountType.CONSIDERED, currTs - 1);
    store.updateCount(id, JobCountType.CONSIDERED, currTs);
    store.updateCount(id, JobCountType.CHOSEN, currTs - TimeUnit.DAYS.toMillis(2));
    store.updateCount(id, JobCountType.CHOSEN, currTs - TimeUnit.DAYS.toMillis(1));
    store.updateCount(id, JobCountType.CHOSEN, currTs);
    Assert.assertEquals(1, store.getCount(id, JobCountType.MATCHED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(2, store.getCount(id, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(3, store.getCount(id, JobCountType.CHOSEN, JobCountStoreImpl.COUNTS_SIZE));
  }

  @Test
  public void testBulkUpdatesAndDeletes() {
    String catalogId = UUID.randomUUID().toString();
    String reflectionId = UUID.randomUUID().toString();
    List<JobCountUpdate> updateList = new ArrayList<>();
    updateList.add(JobCountUpdate.newBuilder().setId(catalogId).setType(JobCountType.CATALOG).build());
    updateList.add(JobCountUpdate.newBuilder().setId(reflectionId).setType(JobCountType.MATCHED).build());
    updateList.add(JobCountUpdate.newBuilder().setId(reflectionId).setType(JobCountType.CONSIDERED).build());
    updateList.add(JobCountUpdate.newBuilder().setId(reflectionId).setType(JobCountType.CONSIDERED).build());
    updateList.add(JobCountUpdate.newBuilder().setId(reflectionId).setType(JobCountType.CHOSEN).build());

    store.bulkUpdateCount(updateList);
    Assert.assertEquals(1, store.getCount(catalogId, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(catalogId, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(1, store.getCount(reflectionId, JobCountType.MATCHED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(2, store.getCount(reflectionId, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(1, store.getCount(reflectionId, JobCountType.CHOSEN, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(reflectionId, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));

    List<String> deleteList = Arrays.asList(catalogId, reflectionId);
    store.bulkDeleteCount(deleteList);
    Assert.assertEquals(0, store.getCount(catalogId, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(catalogId, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(reflectionId, JobCountType.MATCHED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(reflectionId, JobCountType.CONSIDERED, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(reflectionId, JobCountType.CHOSEN, JobCountStoreImpl.COUNTS_SIZE));
    Assert.assertEquals(0, store.getCount(reflectionId, JobCountType.CATALOG, JobCountStoreImpl.COUNTS_SIZE));
  }
}
