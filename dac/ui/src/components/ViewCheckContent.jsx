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
import Immutable from "immutable";
import { CENTER } from "uiTheme/radium/flexStyle";

export default class ViewCheckContent extends Component {
  static propTypes = {
    dataIsNotAvailable: PropTypes.bool,
    viewState: PropTypes.instanceOf(Immutable.Map),
    children: PropTypes.node,
    message: PropTypes.string,
    customStyle: PropTypes.object,
    offsetHeight: PropTypes.number,
  };

  static defaultProps = {
    viewState: Immutable.Map(),
  };

  render() {
    const {
      dataIsNotAvailable,
      viewState,
      children,
      message,
      customStyle,
      offsetHeight,
    } = this.props;
    return !viewState.get("isInProgress") && dataIsNotAvailable ? (
      <div style={{ ...style, ...customStyle }}>
        <span>{message || "Nothing Here"}</span>
        {offsetHeight >= 240 && (
          <dremio-icon
            name="narwhal/query"
            alt="Gnarly"
            style={{ height: 160, width: 160 }}
          />
        )}
      </div>
    ) : (
      children || null
    );
  }
}

const style = {
  backgroundColor: "var(--fill--primary)",
  ...CENTER,
  width: "100%",
  fontSize: 20,
  lineHeight: 32,
  color: "var(--text--primary)",
  gap: 20,
};
