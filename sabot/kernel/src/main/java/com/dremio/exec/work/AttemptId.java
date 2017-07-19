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
package com.dremio.exec.work;

import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryId;

/**
 * represents a single attempt ID. It's a combination of an external QueryId + an attempt number
 */
public class AttemptId {

  // internal internalId
  private final ExternalId externalId;
  private final int attempt;

  public AttemptId() {
    this(ExternalIdHelper.generateExternalId(), 0);
  }

  /**
   * AttemptId = externalId + attempt #
   */
  public AttemptId(final ExternalId externalId, int attempt) {
    assert ExternalIdHelper.isValid(externalId) : "not a valid extenalId";
    assert attempt >= 0 && attempt < 256 : "invalid attempt# " + attempt;
    this.externalId = externalId;
    this.attempt = attempt;
  }

  /**
   * @return internal QueryId corresponding to this attempt id
   */
  public QueryId toQueryId() {
    final long part1 = externalId.getPart1();
    final long part2 = externalId.getPart2();

    return QueryId.newBuilder()
            .setPart1(part1)
            .setPart2(part2 + ((byte) attempt & 0xFF))
            .build();
  }

  public ExternalId getExternalId() {
    return externalId;
  }

  @Override
  public String toString() {
    return ExternalIdHelper.toString(externalId) + "/" + attempt;
  }

  public int getAttemptNum() {
    return attempt;
  }

  public static AttemptId of(final ExternalId externalId) {
    return new AttemptId(externalId, 0);
  }

  public AttemptId nextAttempt() {
    return new AttemptId(externalId, attempt + 1);
  }
}
