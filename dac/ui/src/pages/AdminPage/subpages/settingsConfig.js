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
import ByteField from "components/Fields/ByteField";
import DurationField from "components/Fields/DurationField";
import { formatMessage } from "#oss/utils/locale";

// todo: loc
export const SECTIONS = new Map([
  [
    "Query Queue Control",
    new Map([
      ["exec.queue.enable", formatMessage("QueueControl.exec.queue.enable")], // todo: ax
      ["exec.queue.large", formatMessage("QueueControl.exec.queue.large")],
      ["exec.queue.small", formatMessage("QueueControl.exec.queue.small")],
      [
        "exec.queue.timeout_millis",
        formatMessage("QueueControl.exec.queue.timeout_millis"),
      ],
    ]),
  ],
  [
    "Reflection Queue Control",
    new Map([
      [
        "reflection.queue.enable",
        formatMessage("QueueControl.reflection.queue.enable"),
      ],
      [
        "reflection.queue.large",
        formatMessage("QueueControl.reflection.queue.large"),
      ],
      [
        "reflection.queue.small",
        formatMessage("QueueControl.reflection.queue.small"),
      ],
      [
        "reflection.queue.timeout_millis",
        formatMessage("QueueControl.reflection.queue.timeout_millis"),
      ],
    ]),
  ],
  [
    "Query Memory Control",
    new Map([
      [
        "exec.queue.memory.enable",
        formatMessage("QueueControl.exec.queue.memory.enable"),
      ], // todo: ax
      [
        "exec.queue.memory.large",
        formatMessage("QueueControl.exec.queue.memory.large"),
      ],
      [
        "exec.queue.memory.small",
        formatMessage("QueueControl.exec.queue.memory.small"),
      ],
    ]),
  ],
  [
    "Query Thresholds",
    new Map([
      [
        "exec.queue.threshold",
        formatMessage("QueueControl.exec.queue.threshold"),
      ],
    ]),
  ],
]);

const labels = {};
for (const items of SECTIONS.values()) {
  for (const [key, label] of items.entries()) {
    labels[key] = label;
  }
}

export const LABELS_IN_SECTIONS = labels;

export const LABELS = {
  // handled by Support.js subpage
  "support.email.addr": formatMessage("Support.email.addr"),
  "support.email.jobs.subject": formatMessage("Support.email.jobs.subject"),

  ...labels,
};

export const FIELD_OVERRIDES = {
  "exec.queue.memory.large": ByteField,
  "exec.queue.memory.small": ByteField,
  "exec.queue.timeout_millis": DurationField,
  "reflection.queue.timeout_millis": DurationField,

  "dremio.exec.operator_batch_bytes": ByteField,
};

export const SETTINGS_TOOL_ID = {
  powerbi: "client.tools.powerbi",
  tableau: "client.tools.tableau",
  qlik: "client.tools.qlik",
  qlikEnabled: "support.dac.qlik",
};
