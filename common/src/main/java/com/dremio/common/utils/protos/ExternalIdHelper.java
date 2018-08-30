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
package com.dremio.common.utils.protos;

import java.util.concurrent.ThreadLocalRandom;


import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult;

/**
 * utility methods for ExternalId
 */
public final class ExternalIdHelper {

  public static final long MASK = 0xFFFFFFFFFFFFFF00L;

  public static QueryWritableBatch replaceQueryId(QueryWritableBatch batch, ExternalId externalId) {
    final QueryData header =
            QueryData.newBuilder(batch.getHeader())
            .setQueryId(toQueryId(externalId))
            .build();

    return new QueryWritableBatch(header, batch.getBuffers());
  }

  public static QueryResult replaceQueryId(QueryResult queryResult, final ExternalId externalId) {
    return QueryResult.newBuilder(queryResult).setQueryId(toQueryId(externalId)).build();
  }

  /**
   * Helper method to generate an "external" QueryId.
   *
   * This is the only method that can generate external query Id.
   *
   * @return generated QueryId
   */
  public static ExternalId generateExternalId() {
    ThreadLocalRandom r = ThreadLocalRandom.current();

    // create a new internalId where the first four bytes are a growing time (each new value comes earlier in sequence).
    // Last 11 bytes are random.
    // last byte is set to 0
    final long time = (int) (System.currentTimeMillis()/1000);
    final long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();

    // generate a long from 7 random bytes + 1 zero byte
    // I could also just generate a random long then mask it's last bit
    // but not sure how this will affect it's randomness
    final byte[] bytes = new byte[7];
    r.nextBytes(bytes);
    long p2 = 0;
    for (int i = 0; i < 7; i++) {
      p2 += (bytes[i] & 0xFFL) << (8 * i);
    }
    p2 = p2 << 8;

    return ExternalId.newBuilder().setPart1(p1).setPart2(p2).build();
  }

  public static QueryId toQueryId(final ExternalId externalId) {
    return QueryId.newBuilder()
            .setPart1(externalId.getPart1())
            .setPart2(externalId.getPart2())
            .build();
  }

  @Deprecated // Dremio still uses QueryId internally, once we switch to using AttemptId we can get rid of this
  public static ExternalId toExternal(final QueryId queryId) {
    return ExternalId.newBuilder()
            .setPart1(queryId.getPart1())
            .setPart2(queryId.getPart2() & MASK)
            .build();
  }

  public static boolean isValid(final ExternalId externalId) {
    return (externalId.getPart2() & ~MASK) == 0;
  }

  public static String toString(ExternalId externalId) {
    return QueryIdHelper.getQueryId(toQueryId(externalId));
  }
}
