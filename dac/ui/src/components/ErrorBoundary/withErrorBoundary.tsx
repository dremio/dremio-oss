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
import { ErrorBoundary } from "./ErrorBoundary";

type WithErrorBoundaryConfig = {
  title: string | (() => string);
};

export const withErrorBoundary =
  (config: WithErrorBoundaryConfig) =>
  <T extends React.ComponentType>(Component: T) =>
  (props: any) => {
    return (
      <ErrorBoundary
        title={
          typeof config.title === "function" ? config.title() : config.title
        }
      >
        <Component {...props} />
      </ErrorBoundary>
    );
  };
