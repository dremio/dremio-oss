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
import classes from "./SimplePageHeader.less";

type PageHeaderWithBreadcrumbProps = {
  className?: string;
  title: JSX.Element | string;
  toolbar?: JSX.Element;
};

export const SimplePageHeader = (
  props: PageHeaderWithBreadcrumbProps,
): JSX.Element => {
  return (
    <div className={clsx(classes["simple-page-header"], props.className)}>
      <h1 className={classes["simple-page-header__title"]}>{props.title}</h1>
      {props.toolbar && (
        <div className={classes["simple-page-header__toolbar"]}>
          {props.toolbar}
        </div>
      )}
    </div>
  );
};
