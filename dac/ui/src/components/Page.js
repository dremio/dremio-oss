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
import { cloneElement, Component } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";

import { flexColumnContainer, fullHeight } from "@app/uiTheme/less/layout.less";
import { Suspense } from "@app/components/Lazy";

import { page } from "uiTheme/radium/general";
import { pageContent } from "./Page.less";
import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";

//todo (DX-17781) we should migrate all the pages to use MainMasterPage
export default class Page extends Component {
  static propTypes = {
    children: PropTypes.node,
  };

  render() {
    const { children } = this.props;
    return cloneElement(children, {
      style: { ...children.style, ...page },
    });
  }
}

export class MainMasterPage extends Component {
  static propTypes = {
    children: PropTypes.node,
  };

  render() {
    const { children } = this.props;
    return (
      <div className={classNames(flexColumnContainer, "mainPage")}>
        <div className={pageContent}>
          <SonarSideNav />
          <Suspense>{children}</Suspense>
        </div>
      </div>
    );
  }
}
