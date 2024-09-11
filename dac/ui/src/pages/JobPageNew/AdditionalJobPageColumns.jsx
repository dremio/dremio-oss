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

import ColumnCell from "./components/ColumnCell";

export const additionalColumns = [
  {
    key: "qn",
    id: "qn",
    label: "Queue",
    disableSort: false,
    isSelected: true,
    width: 95,
    height: 40,
    flexGrow: "0",
    flexShrink: "0",
    isFixedWidth: false,
    isDraggable: true,
    headerClassName: "",
  },
];

const renderColumn = (data, isNumeric, className) => {
  return <ColumnCell data={data} isNumeric={isNumeric} className={className} />;
};

export const additionalColumnName = (job) => [
  {
    qn: {
      node: () => renderColumn(job.get("wlmQueue"), false, "fullHeight"),
      value: job.get("wlmQueue"),
    },
  },
];
