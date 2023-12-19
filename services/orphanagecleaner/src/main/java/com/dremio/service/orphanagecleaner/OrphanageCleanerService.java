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
package com.dremio.service.orphanagecleaner;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.datastore.api.Document;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.proto.OrphanEntry;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * This is implementation of OrphanageService
 */
public class OrphanageCleanerService implements Service {

    private static final String LOCAL_TASK_LEADER_NAME = "orphanagecleanup";
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrphanageCleanerService.class);
    private final Provider<SchedulerService> schedulerService;
    private final Provider<OptionManager> optionManager;
    private final boolean isDistributedMaster;
    private final Provider<Orphanage.Factory> orphanageFactoryProvider;
    private final Provider<NamespaceService.Factory> namespaceServiceProvider;
    private final Provider<SabotContext> sabotContextProvider;

    private Orphanage orphanage;
    private OrphanageManager orphanEntryManager;

    public OrphanageCleanerService(final Provider<SchedulerService> schedulerService,
                                   final Provider<OptionManager> optionManagerProvider,
                                   final Provider<Orphanage.Factory> orphanageFactoryProvider,
                                   final Provider<NamespaceService.Factory> namespaceServiceProvider,
                                   final Provider<SabotContext> sabotContextProvider,
                                   boolean isDistributedMaster) {
        this.schedulerService = schedulerService;
        this.optionManager = optionManagerProvider;
        this.isDistributedMaster = isDistributedMaster;
        this.orphanageFactoryProvider = orphanageFactoryProvider;
        this.namespaceServiceProvider = namespaceServiceProvider;
        this.sabotContextProvider = sabotContextProvider;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Orphanage Clean up Service");
        orphanage = orphanageFactoryProvider.get().get();
        NamespaceService namespaceService = namespaceServiceProvider.get().get(SYSTEM_USERNAME);
        SabotContext sabotContext = sabotContextProvider.get();
        orphanEntryManager = new OrphanageManager(orphanage, namespaceService, sabotContext,
                optionManager.get());
        schedulerService.get().schedule(Schedule.Builder
            .singleShotChain()
            .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + getCleanUpPeriodInMillis()))
            .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
            .build(),
                new Runnable() {
                    @Override
                    public void run() {
                      cleanup();
                      schedulerService.get().schedule(Schedule.Builder
                          .singleShotChain()
                          .startingAt(Instant.ofEpochMilli(System.currentTimeMillis() + getCleanUpPeriodInMillis()))
                          .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                          .build(), this);
                    }
                }
        );
    }

    public int cleanup() {
        try {
            if (isDistributedMaster) {
                Iterator<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphanEntryIterator = orphanage.getAllOrphans().iterator();
                if (!orphanEntryIterator.hasNext()) {
                    logger.debug("No orphan dataset entry to clean up");
                }
                List<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphanEntries = Lists.newArrayList(
                        Iterators.limit(orphanEntryIterator, orphanEntryManager.maxAllowedEntries()));
                if (orphanEntries.size() > 0) {
                    orphanEntryManager.processOrphans(orphanEntries);
                    logger.info("Sent " + orphanEntries.size() + " entries to orphanHandlerThread");
                } else {
                    logger.debug("No orphan dataset entry to clean up");
                }
                return orphanEntries.size();
            }
        } catch (Throwable ex) {
            logger.warn(ex.getMessage(), ex);
        }
        return 0;
    }

    private long getCleanUpPeriodInMillis() {
        return TimeUnit.MINUTES.toMillis(optionManager.get().getOption(ExecConstants.ORPHANAGE_ENTRY_CLEAN_PERIOD_MINUTES));
    }

    @Override
    public void close() throws Exception {
      if (orphanEntryManager != null) {
        orphanEntryManager.close();
      }
    }
}
