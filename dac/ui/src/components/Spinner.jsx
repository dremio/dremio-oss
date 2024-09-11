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
import { PureComponent } from "react";
import PropTypes from "prop-types";

import { Spinner as SpinnerNew } from "dremio-ui-lib/components";

const DEFAULT_ICON_SIZE = 30;

class Spinner extends PureComponent {
  static propTypes = {
    iconStyle: PropTypes.object,
    message: PropTypes.node,
    style: PropTypes.object,
  };

  render() {
    const iconStyle = { ...styles.iconStyle, ...this.props.iconStyle };
    return (
      <div
        style={{
          ...styles.base,
          ...(this.props.style || {}),
          ...(iconStyle?.width && {
            "--dremio--spinner--size": `${iconStyle.width}px`,
          }),
        }}
      >
        <SpinnerNew />
        {this.props.message && (
          <span style={styles.message}>{this.props.message}</span>
        )}
      </div>
    );
  }
}

const styles = {
  base: {
    left: 0,
    position: "absolute",
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    width: "100%",
    height: "100%",
    top: 0,
    zIndex: 99,
  },
  iconStyle: {
    width: DEFAULT_ICON_SIZE,
    height: DEFAULT_ICON_SIZE,
  },
  message: {
    color: "inherit",
  },
};

export default Spinner;
