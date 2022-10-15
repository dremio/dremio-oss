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
import type { ErrorDisplayProps } from "./ErrorDisplay.type";
import { renderDevInfo } from "./renderDevInfo";
import { renderProdInfo } from "./renderProdInfo";

const narwhalErrorIcon = (
  //@ts-ignore
  <dremio-icon
    name="narwhal/error"
    class="dremio-error-display__title-icon"
    alt=""
    //@ts-ignore
  ></dremio-icon>
);

/**
 * An error overlay component providing support information (in production mode)
 * or stack traces (in development mode). Can be used inside of an ErrorBoundary
 * or rendered directly as a standalone component.
 */
export const ErrorDisplay: React.FC<ErrorDisplayProps> = (props) => {
  return (
    <div className="dremio-error-display" role="alert">
      <div className="dremio-error-display__wrapper">
        <header>
          {narwhalErrorIcon}
          <div className="dremio-error-display__title-text">{props.title}</div>
        </header>
        {props.production
          ? renderProdInfo({
              supportMessage: props.supportMessage,
              renderSupportInfo: props.renderSupportInfo,
            })
          : renderDevInfo({ error: props.error, errorInfo: props.errorInfo })}
      </div>
    </div>
  );
};
