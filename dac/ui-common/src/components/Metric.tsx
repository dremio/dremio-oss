/* eslint-disable react/prop-types */
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

/* eslint-disable react/prop-types */
import clsx from "clsx";

type MetricProps = {
  name: string;
  duration: string;
  percentage: string;
  metricWidth: number;
  className: string;
};

export const Metric = (props: MetricProps) => {
  const { name, duration, percentage, className = "", metricWidth } = props;
  return (
    <div className={clsx("metric", className)}>
      <div className="metric__label">{name}</div>
      <div className="metric__value">
        {duration}
        {` (${percentage}%)`}
      </div>
      <div
        className="metric__bar"
        style={{
          width: metricWidth,
        }}
      />
    </div>
  );
};
