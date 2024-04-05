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
const UNITS = [
  { label: "B", divider: 1 },
  { label: "KB", divider: 1024 },
  { label: "MB", divider: 1048576 },
  { label: "GB", divider: 1073741824 },
  { label: "TB", divider: 1099511627776 },
  { label: "PB", divider: 1125899906842624 },
  { label: "EB", divider: 1.152921504606846976e18 },
  { label: "ZB", divider: 1.1805916207174113e21 },
  { label: "YB", divider: 1.2089258196146292e24 },
] as const;

const PRECISION = 2;

type SizeString = `${string} ${(typeof UNITS)[number]["label"]}`;

export const formatBytes = (bytes: number): SizeString => {
  for (let i = 1; i <= UNITS.length - 1; i++) {
    if (
      bytes >= UNITS[i].divider &&
      (i === UNITS.length - 1 || bytes < UNITS[i + 1].divider)
    ) {
      return `${(bytes / UNITS[i].divider).toFixed(PRECISION)} ${
        UNITS[i].label
      }`;
    }
  }

  return `${bytes.toFixed(PRECISION)} ${UNITS[0].label}`;
};
