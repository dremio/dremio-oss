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
import parseMilliseconds from "parse-ms";
import type {
  EngineConfiguration,
  EngineProperties,
} from "../../interfaces/Engine.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const engineEntityToProperties = (properties: any): EngineProperties => {
  return {
    activeReplicas: properties.activeReplicas,
    additionalEngineStateInfo: properties.additionalEngineStateInfo,
    configuration: {
      autoStopDelay: Temporal.Duration.from(
        parseMilliseconds(properties.autoStopDelaySeconds * 1000),
      ),
      cloudTags: properties.cloudTags,
      description: properties.description,
      drainTimeLimit: Temporal.Duration.from(
        parseMilliseconds(properties.drainTimeLimitSeconds * 1000),
      ),
      maxConcurrency: properties.maxConcurrency,
      maxReplicas: properties.maxReplicas,
      minReplicas: properties.minReplicas,
      queueTimeLimit: Temporal.Duration.from(
        parseMilliseconds(properties.queueTimeLimitSeconds * 1000),
      ),
      runTimeLimit: Temporal.Duration.from(
        parseMilliseconds(properties.runtimeLimitSeconds * 1000),
      ),
      size: properties.size,
    },
    id: properties.id,
    instanceFamily: properties.instanceFamily,
    name: properties.name,
    queriedAt: (() => {
      const d = new Date(properties.queriedAt);
      if (d.getTime() === 0) {
        return null;
      }
      return d;
    })(),
    state: properties.state,
    statusChangedAt: new Date(properties.statusChangedAt),
  };
};

export const enginePropertiesToEntity = (
  configuration: Partial<EngineConfiguration>,
) => {
  const {
    autoStopDelay,
    description,
    drainTimeLimit,
    queueTimeLimit,
    runTimeLimit,
    ...rest
  } = configuration;
  return {
    ...rest,
    autoStopDelaySeconds: autoStopDelay?.total("seconds"),
    description: description || "",
    drainTimeLimitSeconds: drainTimeLimit?.total("seconds"),
    queueTimeLimitSeconds: queueTimeLimit?.total("seconds"),
    runtimeLimitSeconds: runTimeLimit?.total("seconds"),
  };
};
