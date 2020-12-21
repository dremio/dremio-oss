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
package com.dremio.sabot.task.single;

import com.dremio.common.perf.StatsCollectionEligibilityRegistrar;
import com.dremio.exec.rpc.ResettableBarrier;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.BlockRun;
import com.dremio.sabot.task.TaskManager;
import com.dremio.sabot.task.TaskManager.TaskHandle;

public class DedicatedFragmentRunnable implements Runnable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DedicatedFragmentRunnable.class);

  private final AsyncTaskWrapper task;
  private final ResettableBarrier barrier = new ResettableBarrier();

  private volatile Thread currentThread = null;

  public DedicatedFragmentRunnable(AsyncTaskWrapper task) {
    super();
    this.task = task;
  }

  @Override
  public void run() {
    StatsCollectionEligibilityRegistrar.addSelf();
    currentThread = Thread.currentThread();

    while(true){

      // put try inside the run loop so we don't lose threads with uncaught exceptions.
      try {
        barrier.closeBarrier();
        task.run();

        switch(task.getState()){
        case BLOCKED_ON_DOWNSTREAM:
        case BLOCKED_ON_UPSTREAM:
        case BLOCKED_ON_SHARED_RESOURCE:
          task.setAvailabilityCallback(new BlockRun(toTaskHandle()));
          barrier.await();
          break;
        case DONE:
          task.getCleaner().close();
          return;
        case RUNNABLE:
        default:
          // noop
          break;

        }

      } catch(InterruptedException i){
        logger.info("Thread interrupted, exiting.");
        return;
      } catch(Throwable t) {
        logger.error("Unhandled Exception in Fragment Thread.", t);
        return;
      }
    }
  }

  public TaskManager.TaskHandle<AsyncTaskWrapper> toTaskHandle() {
    return new TaskHandle<AsyncTaskWrapper>() {

      @Override
      public void reEnqueue() {
        barrier.openBarrier();
      }

      @Override
      public int getThread() {
        return currentThread != null ? (int) currentThread.getId() : -1;
      }

      @Override
      public AsyncTaskWrapper getTask() {
        return task;
      }

      @Override
      public String toString() {
        return String.format("%s", task.getState());
      }
    };
  }
}
