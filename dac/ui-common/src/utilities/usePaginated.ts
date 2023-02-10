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

import { useState } from "react";

type PageToken = string;
type PageResult<T> =
  | {
      status: "SUCCESS";
      data: T;
    }
  | {
      status: "REFRESHING";
      data: T;
    }
  | {
      status: "PENDING";
    }
  | {
      status: "ERROR";
      error: unknown;
    };

/**
 * Merges multiple API calls into a single in-order stream
 * @deprecated this does not yet cover enough edge cases to be used widely in the app
 */
export const usePaginated = <T>(fetcher: (...args: any[]) => Promise<T>) => {
  const [pageTokens, setPageTokens] = useState<PageToken[]>([]);
  const [pageResults, setPageResults] = useState<Map<PageToken, PageResult<T>>>(
    new Map()
  );

  const reset = (): void => {
    setPageTokens([]);
    setPageResults(new Map());
  };

  return {
    reset,
    fetchPage: async (
      pageToken: PageToken = "initial",
      ...args: any[]
    ): Promise<void> => {
      const shouldReset = pageToken === "initial";

      if (shouldReset) {
        setPageTokens([pageToken]);
      } else if (!pageTokens.includes(pageToken)) {
        setPageTokens((prev) => [...prev, pageToken]);
      }

      // Create a new page state object with the state set to pending
      setPageResults((prev) => {
        const next = shouldReset ? new Map() : new Map(prev);
        next.set(pageToken, {
          status: "PENDING",
        });
        return next;
      });

      // Wait for the data from the promise to come back and then update the page state object
      try {
        const data = await fetcher(...args);
        setPageResults((prev) => {
          const next = new Map(prev);
          next.set(pageToken, {
            status: "SUCCESS",
            data,
          });
          return next;
        });
      } catch (e) {
        setPageResults((prev) => {
          const next = new Map(prev);
          next.set(pageToken, {
            status: "ERROR",
            error: e,
          });
          return next;
        });
      }
    },
    pages: pageTokens.map((pageToken) => {
      return pageResults.get(pageToken)!;
    }),
  };
};
