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

import TextField from "./TextField";
import { IconButton } from "dremio-ui-lib/components";

class PasswordField extends Component {
  static propTypes = {
    initialFocus: PropTypes.bool,
    error: PropTypes.string,
    onChange: PropTypes.func,
    touched: PropTypes.bool,
    disabled: PropTypes.bool,
    default: PropTypes.string,
    style: PropTypes.object,
    value: PropTypes.string,
  };

  state = {
    showPass: false,
  };

  togglePassView = () => {
    this.setState((state) => ({
      showPass: !state.showPass,
    }));
  };

  render() {
    const type = this.state.showPass ? "text" : "password";
    return (
      <div className="field" style={{ marginRight: 10, position: "relative" }}>
        <TextField
          {...this.props}
          type={type}
          style={{
            marginRight: 0,
            paddingInlineEnd: "var(--dremio--spacing--4)",
          }}
        />
        {this.props.value && (
          <div style={styles.eyeIcon}>
            <IconButton
              aria-label={this.state.showPass ? "Hide" : "Show"}
              onClick={this.togglePassView}
            >
              <dremio-icon
                name={
                  this.state.showPass
                    ? "sql-editor/panel-hide"
                    : "sql-editor/panel-show"
                }
              ></dremio-icon>
            </IconButton>
          </div>
        )}
      </div>
    );
  }
}

const styles = {
  eyeIcon: {
    position: "absolute",
    cursor: "pointer",
    right: 0,
    top: 0,
  },
};
export default PasswordField;
