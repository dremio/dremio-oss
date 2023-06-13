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

import { Component, ReactNode, ErrorInfo } from "react";
//@ts-ignore
import { ErrorDisplay } from "./ErrorDisplay";
import sentryUtil from "@app/utils/sentryUtil";

type ErrorBoundaryProps = {
  className?: string;
  children: ReactNode;
  title: string | (() => string);
};
type ErrorBoundaryState =
  | {
      hasError: false;
      error: null;
      errorInfo: null;
    }
  | {
      hasError: true;
      error: Error;
      errorInfo: ErrorInfo | null;
    };

/**
 * Shows an error overlay over any part of its children that have thrown an unhandled exception during rendering
 * and stops the error from propagating.
 */
export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: any) {
    super(props);

    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
    };
  }

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return {
      hasError: true,
      error,
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    this.setState({
      errorInfo,
    });
    sentryUtil.logException(error);
  }

  render() {
    if (this.state.hasError) {
      return (
        <ErrorDisplay
          error={this.state.error}
          errorInfo={this.state.errorInfo}
          title={this.props.title}
        />
      );
    }

    return this.props.children;
  }
}
