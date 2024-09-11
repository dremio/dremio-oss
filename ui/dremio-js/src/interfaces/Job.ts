import type { Table } from "apache-arrow";
import type { Result } from "ts-results-es";
// import type { Observable } from "rxjs";

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
export type JobProperties = {
  readonly state:
    | "NOT_SUBMITTED"
    | "STARTING"
    | "RUNNING"
    | "COMPLETED"
    | "CANCELED"
    | "FAILED"
    | "CANCELLATION_REQUESTED"
    | "PLANNING"
    | "PENDING"
    | "METADATA_RETRIEVAL"
    | "QUEUED"
    | "ENGINE_START"
    | "EXECUTION_PLANNING"
    | "INVALID_STATE";
  readonly rowCount: number | null;
  readonly errorMessage: string | null;
  readonly startedAt: Date | null;
  readonly endedAt: Date | null;
  readonly queryType: string;
  readonly queueName: string;
  readonly queueId: string;
  readonly resourceSchedulingStartedAt: Date | null;
  readonly resourceSchedulingEndedAt: Date | null;
  readonly cancellationReason: string | null;
  readonly id: string;
};

type JobMethods = {
  // get observable(): Observable<Job>;
  get observable(): any;
  get settled(): boolean;

  get results(): {
    jsonBatches(): AsyncGenerator<{
      readonly schema: {
        fields: {
          name: string;
          type: {
            name: string;
          };
        }[];
      };
      get columns(): Map<string, unknown[]>;
      readonly rows: Record<string, unknown>[];
    }>;
    recordBatches(): AsyncGenerator<Table>;
  };

  cancel(): Promise<Result<void, unknown>>;
};

export type Job = JobProperties & JobMethods;
