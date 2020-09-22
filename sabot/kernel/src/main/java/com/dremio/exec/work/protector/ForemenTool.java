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
package com.dremio.exec.work.protector;

import java.util.Optional;

import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;

/**
 * Tool for interacting with the foremen manager.
 */
public interface ForemenTool {
  boolean cancel(ExternalId id, String reason);
  Optional<QueryProfile> getProfile(ExternalId id);

  ForemenTool NO_OP = new ForemenTool(){

    @Override
    public boolean cancel(ExternalId id, String reason) {
      return false;
    }

    @Override
    public Optional<QueryProfile> getProfile(ExternalId id) {
      return Optional.empty();
    }

  };


}
