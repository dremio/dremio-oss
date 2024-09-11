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

import type { SonarV3Config } from "../../_internal/types/Config.js";
import { Observable, filter, lastValueFrom, share } from "rxjs";
import { tableFromArrays } from "apache-arrow";
import { createRowTypeMapper, jobEntityToProperties } from "./utils.js";
import type {
  Job as JobInterface,
  JobProperties,
} from "../../interfaces/Job.js";
import { Err, Ok } from "ts-results-es";

export class Job implements JobInterface {
  readonly state: JobProperties["state"];
  readonly rowCount: JobProperties["rowCount"];
  readonly errorMessage: JobProperties["errorMessage"];
  readonly startedAt: JobProperties["startedAt"];
  readonly endedAt: JobProperties["endedAt"];
  readonly queryType: JobProperties["queryType"];
  readonly queueName: JobProperties["queueName"];
  readonly queueId: JobProperties["queueId"];
  readonly resourceSchedulingStartedAt: JobProperties["resourceSchedulingStartedAt"];
  readonly resourceSchedulingEndedAt: JobProperties["resourceSchedulingEndedAt"];
  readonly cancellationReason: JobProperties["cancellationReason"];
  readonly id: JobProperties["id"];

  #config: SonarV3Config;
  #observable = new Observable<JobInterface>((subscriber) => {
    if (this.settled) {
      subscriber.next(this);
      subscriber.complete();
      return;
    }
    let currentState = this.state;
    const update = async () => {
      const updatedJob = await this.#config
        .sonarV3Request(`job/${this.id}`)
        .then((res) => res.json())
        .then(
          (properties) =>
            new Job(
              jobEntityToProperties({ ...properties, id: this.id }),
              this.#config,
            ),
        );

      if (updatedJob.state !== currentState) {
        currentState = updatedJob.state;
        subscriber.next(updatedJob);
      }

      if (updatedJob.settled) {
        subscriber.complete();
      } else {
        setTimeout(() => {
          update();
        }, 2000);
      }
    };
    update();
  }).pipe(share());

  constructor(properties: JobProperties, config: SonarV3Config) {
    this.state = properties.state;
    this.rowCount = properties.rowCount;
    this.errorMessage = properties.errorMessage;
    this.startedAt = properties.startedAt;
    this.endedAt = properties.endedAt;
    this.queryType = properties.queryType;
    this.queueName = properties.queueName;
    this.queueId = properties.queueId;
    this.resourceSchedulingStartedAt = properties.resourceSchedulingStartedAt;
    this.resourceSchedulingEndedAt = properties.resourceSchedulingEndedAt;
    this.cancellationReason = properties.cancellationReason;
    this.id = properties.id;
    this.#config = config;
  }

  get settled() {
    return (
      this.state === "COMPLETED" ||
      this.state === "FAILED" ||
      this.state === "INVALID_STATE" ||
      this.state === "CANCELED"
    );
  }

  get results() {
    return {
      jsonBatches: this.#jsonBatches.bind(this),
      recordBatches: this.#recordBatches.bind(this),
    };
  }

  get observable() {
    return this.#observable;
  }

  async *#jsonBatches() {
    if (!this.settled) {
      try {
        await lastValueFrom(
          this.observable.pipe(filter((job) => job.state === "COMPLETED")),
        );
      } catch (e) {
        throw new Error("Job failed");
      }
    }

    const batch_size = 500;
    let hasMore = true;
    let offset = 0;
    while (hasMore) {
      const batch = await this.#config
        .sonarV3Request(
          `job/${this.id}/results?offset=${offset}&limit=${batch_size}`,
        )
        .then((res) => res.json());

      offset += batch.rows.length;
      hasMore = batch.rows.length === batch_size;

      if (batch.rows.length) {
        const schema = { fields: batch.schema } as {
          fields: { name: string; type: { name: string } }[];
        };
        const rowTypeMapper = createRowTypeMapper(schema);
        const rows = [...batch.rows];
        for (const row of rows) {
          rowTypeMapper(row);
        }
        yield {
          get columns() {
            const colMap: any = new Map<string, unknown>();
            for (const field of batch.schema) {
              switch (field.type.name) {
                case "BIGINT":
                  colMap.set(field.name, new BigInt64Array(rows.length));
                  break;
                case "DOUBLE":
                  colMap.set(field.name, new Float64Array(rows.length));
                  break;
                default:
                  colMap.set(field.name, Array.from({ length: rows.length }));
                  break;
              }
            }
            rows.forEach((row: any, i: number) => {
              for (const [field, value] of Object.entries(row)) {
                colMap.get(field)[i] = value;
              }
            });
            return colMap;
          },
          rows,
          schema,
        };
      }
    }
  }

  async *#recordBatches() {
    for await (const batch of this.#jsonBatches()) {
      yield tableFromArrays(Object.fromEntries(batch.columns));
    }
  }

  async cancel() {
    return this.#config
      .sonarV3Request(`job/${this.id}/cancel`, {
        keepalive: true,
        method: "POST",
      })
      .then(() => Ok(undefined))
      .catch((e) => Err(e));
  }
}
