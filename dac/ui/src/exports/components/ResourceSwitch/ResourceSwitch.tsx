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

import { useEffect } from "react";
import { SmartResource } from "smart-resource";
import { ErrorDisplay } from "../../../components/ErrorBoundary/ErrorDisplay";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";

type ErrorMessageProps = {
  renderError?: undefined;
  errorMessage: string;
};

type ManualErrorProps = {
  renderError: (error: any) => JSX.Element;
  errorMessage?: undefined;
};

type Props<T> = {
  resource: SmartResource<T>;
  renderSuccess: (value: T) => JSX.Element;
  renderPending: (value: T | null, error?: any) => JSX.Element;
  refreshOnMount?: boolean;
} & (ErrorMessageProps | ManualErrorProps);

const renderErrorMessage = (error: any, title: string): JSX.Element => {
  return <ErrorDisplay error={error} title={title} />;
};

export const ResourceSwitch = <T extends any>(props: Props<T>) => {
  const {
    renderSuccess,
    renderPending,
    renderError,
    errorMessage,
    refreshOnMount = true,
    resource,
  } = props;
  const [resourceValue, resourceError] = useResourceSnapshot(resource);
  const resourceStatus = useResourceStatus(resource);

  useEffect(() => {
    if (refreshOnMount || !resource.value) {
      resource.fetch();
    }
  }, [resource, refreshOnMount]);

  if (resourceStatus === "initial" || resourceStatus === "pending") {
    return renderPending(resourceValue, resourceError);
  } else if (resourceStatus === "success") {
    return renderSuccess(resourceValue);
  } else {
    if (renderError) {
      return renderError(resourceError);
    }
    return renderErrorMessage(resourceError, errorMessage);
  }
};
