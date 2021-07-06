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

export const JOB_STATUS = {
  notSubmitted: 'NOT_SUBMITTED',
  starting: 'STARTING',
  running: 'RUNNING',
  completed: 'COMPLETED',
  canceled: 'CANCELED',
  failed: 'FAILED',
  cancellationRequested: 'CANCELLATION_REQUESTED',
  enqueued: 'ENQUEUED',
  pending: 'PENDING',
  planning: 'PLANNING',
  metadataRetrieval: 'METADATA_RETRIEVAL',
  engineStart: 'ENGINE_START',
  queued: 'QUEUED',
  executionPlanning: 'EXECUTION_PLANNING'
};
