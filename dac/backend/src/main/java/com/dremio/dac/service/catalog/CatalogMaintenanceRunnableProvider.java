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
package com.dremio.dac.service.catalog;

import com.dremio.catalog.CatalogMaintenanceRunnable;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.datasets.DatasetVersionTrimmer;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceOptions;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.scheduler.Schedule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.LocalTime;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.inject.Provider;

/** Provides maintenance runnables to run on a schedule. */
public class CatalogMaintenanceRunnableProvider {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CatalogMaintenanceRunnableProvider.class);

  private static final String LOCAL_TASK_LEADER_NAME = "catalog_maintenance";

  private final Provider<OptionManager> optionManagerProvider;
  private final KVStoreProvider storeProvider;
  private final Provider<NamespaceService> namespaceServiceProvider;

  public CatalogMaintenanceRunnableProvider(
      Provider<OptionManager> optionManagerProvider,
      KVStoreProvider storeProvider,
      Provider<NamespaceService> namespaceServiceProvider) {
    this.optionManagerProvider = optionManagerProvider;
    this.storeProvider = storeProvider;
    this.namespaceServiceProvider = namespaceServiceProvider;
  }

  public ImmutableList<CatalogMaintenanceRunnable> get(long randomSeed) {
    Random random = new Random(randomSeed);
    LocalTime deleteOrphansTime = fromDayFraction(random.nextDouble());
    LocalTime trimVersionsTime = fromDayFraction(random.nextDouble());
    logger.info(
        "Scheduled time: delete orphans = {}, trim versions = {}",
        deleteOrphansTime,
        trimVersionsTime);
    OptionManager optionManager = optionManagerProvider.get();
    // DatasetVersion.getTimestamp is used for trimming, as of 03/07/24 it records
    // offsets from time in default JVM's timezone, which could vary by installation.
    int minAgeInDays = 1 + (int) optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS);
    return ImmutableList.of(
        CatalogMaintenanceRunnable.builder()
            .setName("DeleteDatasetOrphans")
            .setSchedule(makeDailySchedule(deleteOrphansTime))
            .setRunnable(
                () ->
                    DatasetVersionMutator.deleteOrphans(
                        optionManagerProvider,
                        storeProvider.getStore(DatasetVersionMutator.VersionStoreCreator.class),
                        (int) optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS)))
            .build(),
        CatalogMaintenanceRunnable.builder()
            .setName("TrimVersions")
            .setSchedule(makeDailySchedule(trimVersionsTime))
            .setRunnable(
                () ->
                    DatasetVersionTrimmer.trimHistory(
                        Clock.systemUTC(),
                        storeProvider.getStore(DatasetVersionMutator.VersionStoreCreator.class),
                        namespaceServiceProvider.get(),
                        (int) optionManager.getOption(NamespaceOptions.DATASET_VERSIONS_LIMIT),
                        minAgeInDays))
            .build());
  }

  private static Schedule makeDailySchedule(LocalTime time) {
    return Schedule.Builder.everyDays(1, time)
        .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
        .releaseOwnershipAfter(1, TimeUnit.DAYS)
        .build();
  }

  private static LocalTime fromDayFraction(double fraction) {
    Preconditions.checkArgument(
        0 <= fraction && fraction < 1, "Time fraction must be between [0, 1).");
    double time = fraction * 24;
    int hour = (int) time;
    double minuteTime = 60 * (time - hour);
    int minute = (int) minuteTime;
    int second = (int) (60 * (minuteTime - minute));
    return LocalTime.of(hour, minute, second);
  }
}
