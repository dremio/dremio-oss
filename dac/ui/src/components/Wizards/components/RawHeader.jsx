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

import { IconButton } from "dremio-ui-lib/components";

class RawHeader extends PureComponent {
  static propTypes = {
    closeIconHandler: PropTypes.func,
    closeIcon: PropTypes.bool,
    separator: PropTypes.string,
    text: PropTypes.string,
    subSteps: PropTypes.string,
  };

  constructor(props) {
    super(props);
    this.defaultCloseIconClickHandler =
      this.defaultCloseIconClickHandler.bind(this);
  }

  getCloseIcon() {
    const handler = this.props.closeIconHandler
      ? this.props.closeIconHandler
      : this.defaultCloseIconClickHandler;
    const icon = this.props.closeIcon ? (
      <div className="mr-2">
        <IconButton aria-label="Close" onClick={handler}>
          <dremio-icon name="interface/close-small"></dremio-icon>
        </IconButton>
      </div>
    ) : null;
    return icon;
  }

  getSeparator() {
    const { separator } = this.props;
    return separator ? separator : ": ";
  }

  defaultCloseIconClickHandler() {
    console.info("close icon clicked");
  }

  subSteps() {
    const { subSteps } = this.props;
    return subSteps ? subSteps : null;
  }

  render() {
    return (
      <div className="raw-wizard-header" style={{ ...style.base }}>
        <h5 style={style.content}>
          {this.props.text}
          {this.getSeparator()}
          {this.subSteps()}
        </h5>
        {this.getCloseIcon()}
      </div>
    );
  }
}

const style = {
  base: {
    display: "flex",
    height: 38,
    justifyContent: "space-between",
  },
  content: {
    display: "flex",
    marginLeft: 15,
    alignItems: "center",
  },
};

export default RawHeader;
