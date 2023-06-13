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
package com.dremio.resource;

import com.dremio.exec.proto.UserBitShared;

/**
 * Interface to be able to cancel running query
 *
 * Note that this interface is not used when query cancellation is triggered by APIs
 */
public interface QueryCancelTool {

  /**
   * To cancel running query
   * @param id - query ID
   * @param reason - reason for cancellation
   * @return
   */
  boolean cancel(UserBitShared.ExternalId id, String reason, boolean runTimeExceeded);

  QueryCancelTool NO_OP = new QueryCancelTool() {
    @Override
    public boolean cancel(UserBitShared.ExternalId id, String reason, boolean runTimeExceeded) {
      return false;
    }
  };
}
