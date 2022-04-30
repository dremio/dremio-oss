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

import static com.dremio.service.orphanage.proto.OrphanEntry.OrphanType.ICEBERG_METADATA;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.datastore.api.Document;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageEntryHandler;
import com.dremio.service.orphanage.proto.OrphanEntry;

/**
 * Orphanage Manager processes orphan entries and submits job to respective orphanage entry handler.
 * Based on the handler response, updates the orphan entry in the orphanage.
 */
public class OrphanageManager implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(OrphanageManager.class);
  private static final int ORPHAN_ENTRY_MAX_RETRY_COUNT = 10;
  private static final int MAX_ALLOWED_BLOCKING_QUEUE_SIZE = 1000;

  private final Orphanage orphanage;
  private final Map<OrphanEntry.OrphanType, OrphanageEntryHandler> orphanTypeToHandlerMap = new HashMap<>();
  private final ThreadPoolExecutor executorService;
  private final BlockingQueue<Runnable> blockingQueue;
  private final OptionManager optionManager;
  private int orphanageThreads;

  public OrphanageManager(Orphanage orphanage, NamespaceService namespaceService, SabotContext sabotContext,
                          OptionManager optionManager) {
    this.orphanage = orphanage;
    this.optionManager = optionManager;
    orphanTypeToHandlerMap.put(ICEBERG_METADATA, new IcebergMetadataHandler(namespaceService, sabotContext));
    this.blockingQueue = new LinkedBlockingDeque<>(MAX_ALLOWED_BLOCKING_QUEUE_SIZE);
    this.orphanageThreads = (int)getOrphanProcessingThreadCount();
    this.executorService = new ThreadPoolExecutor(this.orphanageThreads, this.orphanageThreads,
                                        0L, TimeUnit.MILLISECONDS, blockingQueue);
  }

  private long getOrphanProcessingThreadCount() {
    return optionManager.getOption(ExecConstants.ORPHANAGE_PROCESSING_THREAD_COUNT);
  }

  private void resizeThreadPoolIfNeeded() {
    int orphanageThreadCountValue = (int)getOrphanProcessingThreadCount();
    if (this.orphanageThreads == orphanageThreadCountValue) {
      return;
    }
    this.orphanageThreads = orphanageThreadCountValue;
    this.executorService.setCorePoolSize(this.orphanageThreads);
    this.executorService.setMaximumPoolSize(this.orphanageThreads);
  }

  public void processOrphans(List<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> orphans) {
    resizeThreadPoolIfNeeded();
    for (Document<OrphanEntry.OrphanId, OrphanEntry.Orphan> orphanEntry : orphans) {
      Runnable cleanerTask = () -> {
        long curTime = System.currentTimeMillis();
        OrphanEntry.Orphan orphan = orphanEntry.getValue();
        if (orphan.getScheduledAt() <= curTime) {
          if (orphanTypeToHandlerMap.containsKey(orphan.getOrphanType())) {
            OrphanageEntryHandler handler = orphanTypeToHandlerMap.get(orphan.getOrphanType());
            OrphanEntry.OrphanId orphanId = orphanEntry.getKey();
            boolean isSuccess = handler.process(orphanId, orphan);
            if (isSuccess) {
              orphanage.deleteOrphan(orphanId);
            } else {
              if (orphanEntry.getValue().getRetriesCount() < ORPHAN_ENTRY_MAX_RETRY_COUNT) {
                OrphanEntry.Orphan updatedOrphan = OrphanEntry.Orphan.newBuilder(orphanEntry.getValue())
                  .setRetriesCount(orphanEntry.getValue().getRetriesCount() + 1)
                  .build();
                orphanage.addOrUpdateOrphan(orphanId, updatedOrphan);
              } else {
                logger.debug("Deleting orphan {} from the orphanage store as all retries have exhausted.", orphanId);
                orphanage.deleteOrphan(orphanId);
              }
            }
          } else {
            logger.warn("Orphan Type {} not registered with the Orphan Manager, ignoring it.", orphan.getOrphanType());
          }
        }
      };
      try {
        executorService.execute(cleanerTask);
      } catch (RejectedExecutionException ex) {
        logger.debug("The task to clean orphan {} in not accepted for execution {}", orphanEntry.getKey(), ex);
      }
    }
  }

  public int maxAllowedEntries() {
    return Math.max(0, MAX_ALLOWED_BLOCKING_QUEUE_SIZE - blockingQueue.size());
  }

  @Override
  public void close() throws Exception {
    CloseableSchedulerThreadPool.close(executorService, logger);
  }
}
