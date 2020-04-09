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
package com.dremio.common.utils.protos;

import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryId;

/**
 * represents a single attempt ID. It's a combination of an external QueryId + an attempt number
 */
public class AttemptId {

  private static final int MASK = 0xFF;
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
    assert ExternalIdHelper.isValid(externalId) : "not a valid externalId";
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
            .setPart2(part2 + ((byte) attempt & MASK))
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

  /**
   * Convert an external id to an attempt id, assuming this is its first attempt
   * @param externalId
   * @return
   */
  public static AttemptId of(final ExternalId externalId) {
    return new AttemptId(externalId, 0);
  }

  /**
   * Convert a query id to an attempt id
   * @param queryId
   * @return
   */
  public static AttemptId of(final QueryId queryId) {
    ExternalId externalId = ExternalIdHelper.toExternal(queryId);
    int attempt = (int) (queryId.getPart2() & MASK);

    return new AttemptId(externalId, attempt);
  }

  public AttemptId nextAttempt() {
    return new AttemptId(externalId, attempt + 1);
  }
}
