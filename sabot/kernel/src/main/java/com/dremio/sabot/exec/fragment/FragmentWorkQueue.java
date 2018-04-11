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
package com.dremio.sabot.exec.fragment;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;

/**
 * Provides the pipeline the ability to submit work that will be executed when fragment is scheduled again for execution.
 * Relies on a {@link SharedResourceGroup} to allow the fragment to 'wake up' when work is available.
 */
public class FragmentWorkQueue {
  // multiple threads can add work to the queue, but only the fragment executor thread can poll from the queue
  private final ConcurrentLinkedQueue<Runnable> workQueue = new ConcurrentLinkedQueue<>();

  private final SharedResource resource;

  FragmentWorkQueue(SharedResourceGroup resourceGroup) {
    resource = resourceGroup.createResource("fragment-work-queue");
    resource.markBlocked();
  }

  public void put(Runnable work) {
    synchronized (resource) {
      workQueue.add(work);
      resource.markAvailable(); // this is no-op when resource is already available
    }
  }

  public Runnable poll() {
    final Runnable work = workQueue.poll();
    synchronized (resource) {
      if (workQueue.isEmpty()) {
        resource.markBlocked();
      }
    }

    return work;
  }


}
