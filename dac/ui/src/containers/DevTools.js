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
/*eslint no-unused-vars: [2, {"args": "after-used", "varsIgnorePattern": "React"}]*/

import React from "react";

// Exported from redux-devtools
import { createDevTools } from "@redux-devtools/core";

// Monitors are separate packages, and you can make a custom one
import LogMonitor from "@redux-devtools/log-monitor";
import DockMonitor from "@redux-devtools/dock-monitor";

window.React = React;

// createDevTools takes a monitor and produces a DevTools component
const DevTools = createDevTools(
  // Monitors are individually adjustable with props.
  // Consult their repositories to learn about those props.
  // Here, we put LogMonitor inside a DockMonitor.
  // Note: DockMonitor is visible by default.
  <DockMonitor
    toggleVisibilityKey="ctrl-h"
    changePositionKey="ctrl-q"
    defaultIsVisible
  >
    <LogMonitor theme="tomorrow" />
  </DockMonitor>
);

export default DevTools;
