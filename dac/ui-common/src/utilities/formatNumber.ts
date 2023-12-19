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
  { value: 1, symbol: "" },
  { value: 1e3, symbol: "K" },
  { value: 1e6, symbol: "M" },
  { value: 1e9, symbol: "B" },
  { value: 1e12, symbol: "T" },
  { value: 1e15, symbol: "P" },
  { value: 1e18, symbol: "E" },
];

export const formatNumber = (val: number) => {
  if (!val) return;

  const filteredUnit = UNITS.slice()
    .reverse()
    .find((unit) => val >= unit.value);

  if (filteredUnit) {
    if (filteredUnit.symbol === UNITS[0].symbol) {
      return val / filteredUnit.value + filteredUnit.symbol;
    } else {
      return (val / filteredUnit.value).toFixed(1) + filteredUnit.symbol;
    }
  } else {
    return "0";
  }
};
