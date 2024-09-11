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

/**
 * @deprecated
 */
export const withOffsetAsyncIter =
  <
    Params extends { offset?: number; limit?: number; batch_size?: number },
    DataType extends unknown[],
  >(
    fn: (params: Params) => Promise<{ data: DataType; hasNextPage: boolean }>,
  ) =>
  (params: Params) => {
    const { batch_size = 1000, limit = Infinity, offset = 0, ...rest } = params;

    const pageSize = Math.min(batch_size, limit);

    return {
      async *data() {
        let currentOffset = offset;
        let recordsLoaded = 0;
        let result = await fn({
          limit: pageSize,
          offset,
          ...rest,
        } as any);
        yield* result.data;
        recordsLoaded += result.data.length;
        while (recordsLoaded < limit && result.hasNextPage) {
          result = await fn({
            ...rest,
            limit: pageSize,
            offset: (currentOffset += result.data.length),
          } as any);
          yield* result.data;
          recordsLoaded += result.data.length;
        }
      },
    };
  };
