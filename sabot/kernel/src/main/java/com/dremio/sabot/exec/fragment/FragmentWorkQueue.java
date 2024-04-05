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
package com.dremio.sabot.exec.fragment;

import com.dremio.common.AutoCloseables;
import com.dremio.common.collections.Tuple;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the pipeline the ability to submit work that will be executed when fragment is scheduled
 * again for execution. Relies on a {@link SharedResourceGroup} to allow the fragment to 'wake up'
 * when work is available.
 */
public class FragmentWorkQueue {
  private static final Logger logger = LoggerFactory.getLogger(FragmentWorkQueue.class);
  // multiple threads can add work to the queue, but only the fragment executor thread can poll from
  // the queue
  private final ConcurrentLinkedQueue<Tuple<Runnable, AutoCloseable>> workQueue =
      new ConcurrentLinkedQueue<>();
  private final SharedResource resource;
  private volatile boolean isRetired = false;

  FragmentWorkQueue(SharedResourceGroup resourceGroup) {
    resource =
        resourceGroup.createResource("fragment-work-queue", SharedResourceType.FRAGMENT_WORK_QUEUE);
    resource.markBlocked();
  }

  public void put(Runnable work, AutoCloseable closeableOnRetire) {
    synchronized (resource) {
      if (isRetired) {
        // Skip because work queue is already retired
        suppressingClose(closeableOnRetire);
        return;
      }
      workQueue.add(Tuple.of(work, closeableOnRetire));
      resource.markAvailable(); // this is no-op when resource is already available
    }
  }

  public void put(Runnable work) {
    put(work, () -> {});
  }

  public Runnable poll() {
    final Runnable work = Optional.ofNullable(workQueue.poll()).map(t -> t.first).orElse(null);
    synchronized (resource) {
      if (workQueue.isEmpty()) {
        resource.markBlocked();
      }
    }
    return work;
  }

  public void retire() {
    synchronized (resource) {
      isRetired = true;
    }
    while (!workQueue.isEmpty()) {
      suppressingClose(workQueue.poll().second);
    }
  }

  private void suppressingClose(AutoCloseable closeable) {
    try {
      AutoCloseables.close(closeable);
    } catch (Exception e) {
      logger.error("Error while closing work queue item", e);
    }
  }
}
