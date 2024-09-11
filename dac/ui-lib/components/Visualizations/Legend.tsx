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
import * as React from "react";
import clsx from "clsx";

export const Legend = (props: {
  series: { class?: string; label: string }[];
  onSeriesClick?: (seriesIndex: number) => void;
  disabledSeries?: Set<number>;
}) => {
  return (
    <div className="visualization-legend">
      {props.series.map((segment, i) => {
        const isDisabled = props.disabledSeries?.has(i);
        return (
          <div
            className="visualization-legend__segment"
            key={i}
            onClick={(e) => {
              e.stopPropagation();
              props.onSeriesClick?.(i);
            }}
            style={{ cursor: props.onSeriesClick ? "pointer" : "default" }}
          >
            <div
              className={clsx(
                "visualization-legend__segment__chip",
                segment.class && {
                  [segment.class]: !isDisabled,
                },
              )}
              style={isDisabled ? { background: "var(--fill--disabled)" } : {}}
            />
            <div>
              {isDisabled ? (
                <span style={{ color: "var(--text--disabled)" }}>
                  {segment.label}
                </span>
              ) : (
                segment.label
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
};
