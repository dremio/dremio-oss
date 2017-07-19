/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.jobs;

import java.util.UUID;

import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.work.AttemptId;
import com.dremio.exec.work.ExternalIdHelper;

/**
 * Helper class to convert from/to {@code com.dremio.exec.work.AttemptId}
 */
public final class AttemptIdUtils {

  private AttemptIdUtils() {
  }

  public static String toString(AttemptId attemptId) {
    return QueryIdHelper.getQueryId(attemptId.toQueryId());
  }

  public static AttemptId fromString(final String attemptIdStr) {
    final UUID uuid = UUID.fromString(attemptIdStr);
    final long part1 = uuid.getMostSignificantBits();
    final long part2 = uuid.getLeastSignificantBits();
    final ExternalId externalId = ExternalId.newBuilder()
        .setPart1(part1)
        .setPart2(part2 & ExternalIdHelper.MASK)
        .build();

    final int attempt = (int) part2 & 0xFF;
    return new AttemptId(externalId, attempt);
  }
}
