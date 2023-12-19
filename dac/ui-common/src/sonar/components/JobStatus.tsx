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

import { getIntlContext } from "../../contexts/IntlContext";
import { Tooltip } from "dremio-ui-lib/components";

export enum JobStates {
  NOT_SUBMITTED = "NOT_SUBMITTED",
  STARTING = "STARTING",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  CANCELED = "CANCELED",
  FAILED = "FAILED",
  CANCELLATION_REQUESTED = "CANCELLATION_REQUESTED",
  ENQUEUED = "ENQUEUED",
  PLANNING = "PLANNING",
  PENDING = "PENDING",
  METADATA_RETRIEVAL = "METADATA_RETRIEVAL",
  QUEUED = "QUEUED",
  ENGINE_START = "ENGINE_START",
  EXECUTION_PLANNING = "EXECUTION_PLANNING",
}

export const JobStatusLabels = {
  [JobStates.NOT_SUBMITTED]: "Sonar.Job.Status.NotSubmitted",
  [JobStates.STARTING]: "Sonar.Job.Status.Starting",
  [JobStates.RUNNING]: "Sonar.Job.Status.Running",
  [JobStates.COMPLETED]: "Sonar.Job.Status.Completed",
  [JobStates.CANCELED]: "Sonar.Job.Status.Canceled",
  // [JobStates.ENQUEUED]: "Sonar.Job.Status.Failed",
  [JobStates.CANCELLATION_REQUESTED]: "Sonar.Job.Status.CancellationRequested",
  [JobStates.ENQUEUED]: "Sonar.Job.Status.Enqueued",
  [JobStates.PENDING]: "Sonar.Job.Status.Pending",
  [JobStates.METADATA_RETRIEVAL]: "Sonar.Job.Status.MetadataRetrieval",
  [JobStates.PLANNING]: "Sonar.Job.Status.Planning",
  [JobStates.ENGINE_START]: "Sonar.Job.Status.EngineStart",
  [JobStates.QUEUED]: "Sonar.Job.Status.Queued",
  [JobStates.EXECUTION_PLANNING]: "Sonar.Job.Status.ExecutionPlanning",
};

export const JobStatusIcons = {
  [JobStates.NOT_SUBMITTED]: "ellipsis",
  [JobStates.STARTING]: "starting",
  [JobStates.RUNNING]: { src: "running", className: "spinner" },
  [JobStates.COMPLETED]: "job-completed",
  [JobStates.CANCELED]: "canceled",
  [JobStates.FAILED]: "error-solid",
  [JobStates.CANCELLATION_REQUESTED]: "cancelled-gray",
  [JobStates.ENQUEUED]: "ellipsis",
  [JobStates.PLANNING]: "planning",
  [JobStates.PENDING]: "setup",
  [JobStates.METADATA_RETRIEVAL]: "planning",
  [JobStates.QUEUED]: "queued",
  [JobStates.ENGINE_START]: "engine-start",
  [JobStates.EXECUTION_PLANNING]: "starting",
};

type JobStatusType = string | { src: string; className: string };

export const JobStatus = ({
  state,
}: {
  state: keyof typeof JobStatusIcons;
}) => {
  const { t } = getIntlContext();
  let src: JobStatusType = "ellipsis";
  let className = "";
  const curState: JobStatusType = JobStatusIcons[state];

  if (curState) {
    if (typeof curState === "string") {
      src = curState;
    } else {
      src = curState.src;
      className = curState.className;
    }
  }

  const dimension = src === "planning" ? 22 : 24;

  return (
    <Tooltip portal content={t("Sonar.Job.Status." + state)}>
      {/* @ts-ignore */}
      <dremio-icon
        name={`job-state/${src}`}
        alt={t("Sonar.Job.Status." + state)}
        class={className}
        style={{
          height: dimension,
          width: dimension,
          verticalAlign: "unset",
        }}
      />
    </Tooltip>
  );
};
