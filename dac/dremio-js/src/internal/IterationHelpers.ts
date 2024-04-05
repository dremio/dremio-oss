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

export const withOffsetAsyncIter =
  <
    Params extends { offset?: number; limit: number; batch_size?: number },
    DataType extends any[],
  >(
    fn: (params: Params) => Promise<{ data: DataType; hasNextPage: boolean }>,
  ) =>
  (params: Params) => {
    const { limit, offset = 0, batch_size = 1000, ...rest } = params;
    return {
      async *data() {
        let currentOffset = offset;
        let recordsLoaded = 0;
        let result = await fn({
          limit: Math.min(batch_size, limit),
          offset,
          ...rest,
        } as any);
        yield* result.data;
        recordsLoaded += result.data.length;
        while (result.hasNextPage && recordsLoaded < limit) {
          result = await fn({
            ...rest,
            offset: (currentOffset += result.data.length),
            limit: Math.min(batch_size, limit - recordsLoaded),
          } as any);
          yield* result.data;
          recordsLoaded += result.data.length;
        }
      },
    };
  };
