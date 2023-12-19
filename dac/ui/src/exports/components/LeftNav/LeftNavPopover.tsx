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
import { type ReactElement } from "react";
import { IconButton, Popover } from "dremio-ui-lib/components";
import * as classes from "./LeftNavPopover.less";
import clsx from "clsx";

type LeftNavPopoverProps = {
  tooltip: string;
  content: Parameters<typeof Popover>[0]["content"];
  icon: ReactElement;
  mode: Parameters<typeof Popover>[0]["mode"];
  role: Parameters<typeof Popover>[0]["role"];
};

export const LeftNavPopover = (props: LeftNavPopoverProps) => {
  return (
    <div className="sideNav-item item__hover" style={{ color: "white" }}>
      <Popover
        mode={props.mode}
        role={props.role}
        content={props.content}
        placement="right-end"
        portal
      >
        <IconButton
          data-qa="Tutorials"
          tooltip={props.tooltip}
          tooltipPlacement="right"
          className={clsx(
            "w-8 h-8 position-relative",
            classes["left-nav-popover"]
          )}
        >
          {props.icon}
          <dremio-icon
            name="interface/caret-down-right"
            alt=""
            style={{
              width: "12px",
              height: "12px",
              right: "7px",
              bottom: "7px",
              position: "absolute",
            }}
          ></dremio-icon>
        </IconButton>
      </Popover>
    </div>
  );
};
