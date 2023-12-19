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

const PhasesDetailMap: any = {
  PENDING: "pendingTime",
  METADATA_RETRIEVAL: "metadataRetrievalTime",
  PLANNING: "planningTime",
  QUEUED: "queuedTime",
  EXECUTION_PLANNING: "executionPlanningTime",
  STARTING: "startingTime",
  RUNNING: "runningTime",
};

const PhasesArray = Object.keys(PhasesDetailMap);

export const transformFromReflectionDetails = (payload: any) => {
  const attemptDetails = payload.attemptDetails?.[0];
  return {
    ...payload,
    ...(payload.stats || {}),
    id: payload.jobId?.id,
    jobStatus: payload.state,
    durationDetails: PhasesArray.map((phase) => ({
      phaseName: phase,
      phaseDuration: `${attemptDetails?.[PhasesDetailMap[phase]] || 0}`,
    })),
    cpuUsed:
      payload.topOperations?.reduce(
        (acc: number, cur: any) => acc + cur.timeConsumed,
        0
      ) || 0,
    queryUser: payload.user,
    duration: attemptDetails?.totalTime,
    totalMemory: payload.peakMemory,
    engine: payload.resourceScheduling?.engineName,
    queryText: payload.description,
    wlmQueue: payload.resourceScheduling?.queueName,
    queriedDatasets: [
      {
        datasetName:
          payload.datasetPathList?.[payload.datasetPathList?.length - 1],
        datasetPath: payload.datasetPathList?.join("."),
        datasetType: payload.datasetType,
        datasetPathsList: payload.datasetPathList,
      },
    ],
    reflections: [
      {
        ...(payload.materializationFor || {}),
        reflectionDatasetPath: payload.datasetPathList?.join("."),
      },
    ],
  };
};
