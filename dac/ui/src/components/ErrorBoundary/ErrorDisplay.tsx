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

import { type ErrorInfo } from "react";
//@ts-ignore
import { ErrorDisplay as BaseErrorDisplay } from "dremio-ui-lib";
import { SupportInfo } from "./SupportInfo";
import { intl } from "@app/utils/intl";

type Props = {
  className?: string;
  title: string | (() => string);
  error: Error;
  errorInfo?: ErrorInfo | null;
};

export const ErrorDisplay = (props: Props): JSX.Element => {
  // Just in case it's used from a non-TS file
  if (process.env.NODE_ENV === "development" && !props.title) {
    throw new Error(
      "ErrorBoundary: a title prop customized and translated for the specific context it's wrapping should be provided."
    );
  }

  return (
    <BaseErrorDisplay
      error={props.error}
      errorInfo={props.errorInfo}
      production={process.env.NODE_ENV !== "development"}
      renderSupportInfo={() => <SupportInfo />}
      supportMessage={intl.formatMessage({ id: "Support.contact" })}
      title={
        props.title
          ? typeof props.title === "function"
            ? props.title()
            : props.title
          : "An unexpected error occurred"
      }
    />
  );
};
