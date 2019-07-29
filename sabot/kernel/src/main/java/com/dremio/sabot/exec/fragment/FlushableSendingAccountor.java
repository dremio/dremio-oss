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

import com.dremio.sabot.exec.context.Flushable;
import com.dremio.sabot.threads.SendingAccountor;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;

/**
 * Makes sending accountor non-blocking flushable via the shared resource manager.
 */
public class FlushableSendingAccountor implements Flushable {
  private final SendingAccountor sendingAccountor = new SendingAccountor();

  private final SharedResourceGroup resourceGroup;

  public FlushableSendingAccountor(SharedResourceGroup resourceGroup) {
    super();
    this.resourceGroup = resourceGroup;
  }

  public SendingAccountor getAccountor(){
    return sendingAccountor;
  }

  public boolean flushMessages() {
    return sendingAccountor.markBlockingWhenMessagesOutstanding(resourceGroup);
  }
}
