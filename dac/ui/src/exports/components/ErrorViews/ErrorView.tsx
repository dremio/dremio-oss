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

import clsx from "clsx";
import * as classes from "./ErrorView.less";

type Props = {
  action?: JSX.Element;
  additionalInfo?: JSX.Element | string;
  image?: JSX.Element;
  title: JSX.Element | string;
};

export const ErrorView = (props: Props) => {
  return (
    <div className={clsx(classes["error-view"])}>
      {props.image && (
        <div className={classes["error-view__image"]}>{props.image}</div>
      )}
      <h1 className={classes["error-view__title"]}>{props.title}</h1>
      {props.additionalInfo && (
        <p className={classes["error-view__additional"]}>
          {props.additionalInfo}
        </p>
      )}
      {props.action && (
        <div className={classes["error-view__action"]}>{props.action}</div>
      )}
    </div>
  );
};
