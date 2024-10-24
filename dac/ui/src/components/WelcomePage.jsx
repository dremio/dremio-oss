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
import { Component } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";

import LoginTitle from "#oss/pages/AuthenticationPage/components/LoginTitle";
import { welcomePage, contentWrapper } from "./WelcomePage.less";

export class WelcomePage extends Component {
  static propTypes = {
    title: PropTypes.node.isRequired,
    contentClassName: PropTypes.string,
    children: PropTypes.node,
  };

  render() {
    const { title, children, contentClassName } = this.props;

    return (
      <div className={classNames("page", welcomePage)}>
        <div className={classNames(contentWrapper, contentClassName)}>
          <LoginTitle
            key="title"
            subTitle={title}
            style={{ marginBottom: 20 }}
          />
          {children}
        </div>
      </div>
    );
  }
}
