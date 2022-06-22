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
import * as React from "react";
import { useCollapsed } from "./useCollapsed";
import { IconButton } from "../IconButton";

export const CollapsibleStacktrace = (props) => {
  const [collapsed, toggleCollapsed] = useCollapsed(true);

  return (
    <React.Fragment>
      <p>
        <IconButton
          aria-expanded={!collapsed}
          onClick={toggleCollapsed}
          tooltip={collapsed ? "Show" : "Hide"}
        >
          <dremio-icon
            name={collapsed ? "interface/caret-right" : "interface/caret-down"}
          ></dremio-icon>
          {props.title}
        </IconButton>
      </p>
      <pre
        aria-hidden={collapsed}
        hidden={collapsed}
        className="dremio-error-display__codeblock"
      >
        {props.contents}
      </pre>
    </React.Fragment>
  );
};
