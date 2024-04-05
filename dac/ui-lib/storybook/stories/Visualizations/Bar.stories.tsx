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

import { Meta } from "@storybook/react";

import { Bar, Legend } from "../../../components";

export default {
  title: "Visualizations/Bar",
  component: Bar,
} as Meta<typeof Bar>;

const tooltipSegments =
  ({ label, segments }) =>
  () => (
    <div>
      <div className="dremio-typography-less-important mb-1">{label}</div>
      <dl className="viz-segment-list">
        {segments.map((segment) => (
          <>
            <dt style={{ display: "flex", gap: "6px" }}>
              <div
                className={segment.legendClass}
                style={{
                  width: "12px",
                  height: "12px",
                  borderRadius: "99999px",
                }}
              />
              {segment.label}
            </dt>
            <dd>{segment.value}</dd>
          </>
        ))}
      </dl>
    </div>
  );

const barData = [
  [5, 10, 30, 40, 35, 15, 20, 30, 90, 25, 10],
  [20, 3, 0, 0, 3, 10, 3, 1, 0, 2, 0],
];

const xAxis = {
  data: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
};

type DataSeries = number[] | number[][];

// const getTotalFromSeries = (series: DataSeries) =>
//   series.flat(1).reduce((total, val) => total + val, 0);

const getSliceFromSeries = (series: DataSeries) => (index: number) => {
  if (typeof series[0] === "number") {
    return [series[index]];
  }
  return series.map((segment) => segment[index]);
};

const getMaximumFromSeries = (series: DataSeries) => {
  if (typeof series[0] === "number") {
    return Math.max(series as unknown as number);
  }
  let max;
  for (let i = 0; i < series[0].length; i++) {
    const total = series.reduce((combined, segment) => {
      if (!combined) {
        return segment[i];
      }
      return combined + segment[i];
    }, undefined);
    if (!max || total > max) {
      max = total;
    }
  }
  return max;
};

const renderBars = ({ xAxis, series, segmentMode }) => {
  const seriesMax = getMaximumFromSeries(series);
  const bars = [];
  const getSlice = getSliceFromSeries(series);
  for (let i = 0; i < series[0].length; i++) {
    const seriesSlice = getSlice(i);
    bars.push(
      <Bar
        label={xAxis.data[i]}
        segmentMode={segmentMode}
        segments={[
          { height: `${(seriesSlice[0] / seriesMax) * 100}%` },
          {
            height: `${(seriesSlice[1] / seriesMax) * 100}%`,
            class: "bg-danger-bold",
          },
        ]}
        tooltip={tooltipSegments({
          label: xAxis.data[i],
          segments: [
            {
              label: "Successful",
              value: seriesSlice[0],
              legendClass: "bg-cyan-200",
            },
            {
              label: "Errors",
              value: seriesSlice[1],
              legendClass: "bg-danger-bold",
            },
          ],
        })}
      />,
    );
  }

  return bars;
};

export const Default = () => {
  return (
    <div>
      <div className="bar-group" style={{ height: "200px", width: "400px" }}>
        {renderBars({ xAxis, series: barData, segmentMode: "stacked" })}
      </div>
      <Legend
        series={[
          { class: "bg-cyan-200", label: "Success" },
          { class: "bg-danger-bold", label: "Failed" },
        ]}
      />
    </div>
  );
};

Default.storyName = "Bar";

export const GroupedBar = () => {
  return (
    <div>
      <div className="bar-group" style={{ height: "200px", width: "400px" }}>
        {renderBars({ xAxis, series: barData, segmentMode: "grouped" })}
      </div>
    </div>
  );
};
