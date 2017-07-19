/*
 * Copyright (C) 2017 Dremio Corporation
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
import ByteField from 'components/Fields/ByteField';
import DurationField from 'components/Fields/DurationField';

// todo: loc
export const SECTIONS = new Map([
  ['Query Queue Control', new Map([
    ['exec.queue.enable', ''], // todo: ax
    ['exec.queue.large', 'Large queue size'],
    ['exec.queue.small', 'Small queue size'],
    ['exec.queue.timeout_millis', 'Queue timeout']
  ])],
  ['Query Memory Control', new Map([
    ['exec.queue.memory.enable', ''], // todo: ax
    ['exec.queue.memory.large', 'Maximum memory (large queries)'],
    ['exec.queue.memory.small', 'Maximum memory (small queries)']
  ])],
  ['Query Thresholds', new Map([
    ['exec.queue.threshold', 'Large query threshold (plan cost)']
  ])]
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
  'support.email.addr': 'Email addresses (comma separated)',
  'support.email.jobs.subject': 'Email subject',

  ...labels
};

export const FIELD_OVERRIDES = {
  'exec.queue.memory.large': ByteField,
  'exec.queue.memory.small': ByteField,
  'exec.queue.timeout_millis': DurationField,

  'dremio.exec.operator_batch_bytes': ByteField
};
