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
package com.dremio.sabot.threads.sharedres;

import com.dremio.exec.proto.UserBitShared.SharedResourceCategory;

public enum SharedResourceType {
  UNKNOWN(SharedResourceCategory.OTHER),
  FRAGMENT_WORK_QUEUE(SharedResourceCategory.OTHER),
  SEND_MSG_COORDINATOR(SharedResourceCategory.DOWNSTREAM),
  SEND_MSG_DATA(SharedResourceCategory.DOWNSTREAM),
  UNORDERED_RECV_MEM_BUFFER(SharedResourceCategory.UPSTREAM),
  UNORDERED_RECV_SPOOL_BUFFER(SharedResourceCategory.UPSTREAM),
  NWAY_RECV_MEM_BUFFER(SharedResourceCategory.UPSTREAM),
  NWAY_RECV_SPOOL_BUFFER(SharedResourceCategory.UPSTREAM),
  OUTGOING_MSG_ACK(SharedResourceCategory.DOWNSTREAM),
  WAIT_FOR_MEMORY(SharedResourceCategory.MEMORY),
  FRAGMENT_ACTIVATE_SIGNAL(SharedResourceCategory.OTHER),
  TEST(SharedResourceCategory.OTHER);

  SharedResourceType(SharedResourceCategory category) {
    this.category = category;
  }

  public SharedResourceCategory getCategory() {
    return category;
  }

  private final SharedResourceCategory category;
}
