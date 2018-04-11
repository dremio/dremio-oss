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
package com.dremio.sabot.threads;

import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;

/**
 * Account for whether all messages sent have been completed. Necessary before finishing a task so we don't think
 * buffers are hanging when they will be released.
 *
 * TODO: Need to update to use long for number of pending messages.
 */
public class SendingAccountor {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SendingAccountor.class);

  private final AtomicInteger pendingMessages = new AtomicInteger(0);
  private volatile SharedResource sendComplete;

  public void increment() {
    pendingMessages.incrementAndGet();
  }

  public void decrement() {
    synchronized(this) { // protect against setting of send complete.
      if(pendingMessages.decrementAndGet() == 0) {
        if(sendComplete != null){
          synchronized(sendComplete){
            sendComplete.markAvailable();
          }
          sendComplete = null;
        }
      }
    }
  }

  public boolean isFlushed(){
    return pendingMessages.get() == 0;
  }

  /**
   * If there are outstanding messages, add waiting for them the provided resource manager.
   * @param manager SharedResourceManager to enhance.
   * @return True if we have no further outstanding messages. False if we are still waiting for some messages.
   */
  public boolean markBlockingWhenMessagesOutstanding(SharedResourceGroup manager) {
    synchronized(this) {
      if (sendComplete != null && !sendComplete.isAvailable()) {
        // we are already blocked waiting for pending messages
        return false;
      }

      final int pending = pendingMessages.get();
      if(pending == 0) {
        // do nothing with manager.
        return true;
      } else {
        SharedResource resource = manager.createResource("outgoing-messages-ack");
        resource.markBlocked();
        sendComplete = resource;
        return false;
      }
    }
  }

}
