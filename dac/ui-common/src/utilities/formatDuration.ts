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
  // { label: "d", divider: 86400000 },
  { label: "", divider: 3600000 },
  { label: "", divider: 60000 },
  { label: "", divider: 1000 },
] as const;

const MS_PRECISION = 3;

export const formatDuration = (remainder: number, unitIndex = 0): string => {
  // Seconds.ms format
  // if (unitIndex === UNITS.length - 1) {
  //   return `${(remainder / UNITS[unitIndex].divider).toFixed(MS_PRECISION)}${
  //     UNITS[unitIndex].label
  //   }`;
  // }

  const result = ~~(remainder / UNITS[unitIndex].divider);

  const formattedString = `${result.toLocaleString("default", {
    minimumIntegerDigits: 2,
  })}${UNITS[unitIndex].label}`;

  if (unitIndex === UNITS.length - 1) {
    return formattedString;
  }

  const nextRemainder = remainder % UNITS[unitIndex].divider;
  const nextString = formatDuration(nextRemainder, unitIndex + 1);

  // ignore values that are 0
  // if (result === 0) {
  //   return nextString;
  // }

  return `${formattedString}:${nextString}`;
};
