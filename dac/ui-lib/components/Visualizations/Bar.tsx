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
import { forwardRef } from "react";
import clsx from "clsx";
import { Popover } from "../Popover";

type HeightString = `${number}%`;

type Segment = {
  id?: string | number;
  height: HeightString;
  class?: string;
};

type BarProps = {
  label?: string;
  segments: Segment[];
  segmentMode: "stacked" | "grouped";
  tooltip?: () => JSX.Element;
};

const withBarTooltip =
  (tooltip?: BarProps["tooltip"]) => (target: JSX.Element) => {
    if (!tooltip) {
      return target;
    }

    return (
      <Popover
        content={() => (
          <div
            className="drop-shadow-lg bg-primary"
            style={{ padding: "8px", borderRadius: "4px" }}
          >
            {tooltip()}
          </div>
        )}
        mode="hover"
        placement="right"
        role="tooltip"
      >
        {target}
      </Popover>
    );
  };

export const Bar = forwardRef<HTMLDivElement, BarProps>((props, ref) => {
  const wrapTooltip = withBarTooltip(props.tooltip);
  return (
    <div ref={ref} className="bar">
      {wrapTooltip(
        <div
          className={`bar__container hover-alt bar__container--${props.segmentMode}`}
        >
          {props.segments.map((segment, i) => (
            <div
              key={segment.id || i}
              className={clsx(
                "bar__segment",
                segment.class || "bar__segment--bg-default",
              )}
              style={{ height: segment.height }}
            />
          ))}
        </div>,
      )}
      {props.label && <div className="bar__label">{props.label}</div>}
    </div>
  );
});
