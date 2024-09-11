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

import { IconButton, Popover } from "dremio-ui-lib/components";
import clsx from "clsx";

import * as classes from "./SettingsPopover.module.less";

type SettingsPopoverProps = {
  content: Parameters<typeof Popover>[0]["content"];
  children: any;
  tooltip?: string;
  dataQa?: string;
  forceTooltipOnHover?: boolean;
  disabled?: boolean;
  hideArrowIcon?: boolean;
  hasDropdown?: boolean;
  onOpen?: () => void;
  onClose?: () => void;
};

const SettingsPopover = (props: SettingsPopoverProps) => {
  return (
    <div>
      <Popover
        mode="click"
        role="menu"
        content={props.content}
        placement="left-start"
        portal={false}
        className={clsx(classes["settings-popover"], "drop-shadow-lg")}
        dismissable
        onOpen={props.onOpen}
        onClose={props.onClose}
      >
        <IconButton
          className="settings-button"
          data-qa={props.dataQa}
          tooltip={props.tooltip ?? "More"}
          tooltipPortal
        >
          {props.children}
          {props.hasDropdown && !props.disabled && !props.hideArrowIcon && (
            <dremio-icon
              style={{
                blockSize: 20,
                inlineSize: 20,
              }}
              name="interface/caretDown"
            ></dremio-icon>
          )}
        </IconButton>
      </Popover>
    </div>
  );
};

export default SettingsPopover;
