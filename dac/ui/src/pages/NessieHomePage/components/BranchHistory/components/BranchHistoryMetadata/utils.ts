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

import { LogEntryV1 as LogEntry } from "@app/services/nessie/client";

export const countUniqueAuthors = (
  logEntries: LogEntry[] | undefined
): number => {
  const authorSet = new Set<string>();

  if (logEntries) {
    for (const logEntry of logEntries) {
      if (logEntry.commitMeta && logEntry.commitMeta.author) {
        authorSet.add(logEntry.commitMeta.author);
      }
    }
  }

  return authorSet.size;
};
