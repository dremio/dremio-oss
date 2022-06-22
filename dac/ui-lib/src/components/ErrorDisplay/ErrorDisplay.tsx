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
import { CollapsibleStacktrace } from "./CollapsibleStacktrace";

const DEFAULT_SUPPORT_MESSAGE =
  "Something went wrong when we tried to render this content.";

const errorIcon = (
  //@ts-ignore
  <dremio-icon
    name="job-state/failed"
    class="dremio-error-display__title-icon"
    style={{ margin: "-7px" }}
    //@ts-ignore
  ></dremio-icon>
);

type ErrorDisplayProps = {
  details: React.ReactNode;
  error: Error;
  errorInfo?: { componentStack: string };
  production?: boolean;
  supportInfo?: string;

  // A customized support message depending on the product edition
  supportMessage?: string;
  title: string;
};

const renderDevInfo = ({
  error,
  errorInfo,
}: Pick<ErrorDisplayProps, "error" | "errorInfo">): React.ReactElement => {
  return (
    <React.Fragment>
      <div className="dremio-error-display__codeblock">{error.message}</div>
      {errorInfo?.componentStack && (
        <CollapsibleStacktrace
          title="Component Tree"
          contents={errorInfo.componentStack}
        />
      )}
    </React.Fragment>
  );
};

const renderProdInfo = ({
  supportInfo,
  supportMessage,
}: Pick<
  ErrorDisplayProps,
  "supportInfo" | "supportMessage"
>): React.ReactElement => {
  return (
    <React.Fragment>
      <p
        aria-details="dremio-error-display__supportInfo"
        className="dremio-error-display__support-message"
      >
        {supportMessage}
      </p>
      {supportInfo && (
        <pre
          id="dremio-error-display__supportInfo"
          className="dremio-error-display__codeblock"
        >
          {supportInfo}
        </pre>
      )}
    </React.Fragment>
  );
};

export const ErrorDisplay: React.FC<ErrorDisplayProps> = (props) => {
  return (
    <div className="dremio-error-display" role="alert">
      <div className="dremio-error-display__wrapper">
        <header>
          <p className="dremio-error-display__title">
            {errorIcon}
            {props.title}
          </p>
        </header>
        {props.production
          ? renderProdInfo({
              supportMessage: props.supportMessage || DEFAULT_SUPPORT_MESSAGE,
              supportInfo: props.supportInfo,
            })
          : renderDevInfo({ error: props.error, errorInfo: props.errorInfo })}
      </div>
    </div>
  );
};
