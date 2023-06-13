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

import { ReflectionSummary } from "../reflections/ReflectionSummary.type";

const ICON_BASE = "/static/icons/dremio";

type ReflectionTypeProps = {
  type: ReflectionSummary.ReflectionTypeEnum;
};

const getConfigForReflectionType = (
  type: ReflectionSummary.ReflectionTypeEnum
) => {
  const icon = (() => {
    switch (type) {
      case ReflectionSummary.ReflectionTypeEnum.RAW:
        return {
          iconName: "interface/reflection-raw-mode",
          // iconTooltip: "Raw Reflection",
          label: "Raw",
        };
      case ReflectionSummary.ReflectionTypeEnum.AGGREGATION:
        return {
          iconName: "interface/reflection-aggregate",
          // iconTooltip: "Aggregation Reflection",
          label: "Aggregation",
        };
      default:
        return {
          iconName: "interface/reflection-raw-mode",
          // iconTooltip: "Raw Reflection",
          label: "Raw",
        };
    }
  })();

  return {
    ...icon,
    path: `${ICON_BASE}/${icon.iconName}.svg`,
  };
};

export const ReflectionType = (props: ReflectionTypeProps) => {
  const { label, path } = getConfigForReflectionType(props.type);

  return (
    <div className="dremio-icon-label">
      <img src={path} alt=""></img> {label}
    </div>
  );
};
