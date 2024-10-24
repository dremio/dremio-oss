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

import Spinner from "#oss/components/Spinner";
import "./LoadingOverlay.less";

/**
 * A overlay that shows a spinner in the center of it's area.
 * Useful in case of any data load
 */
export default class LoadingOverlay extends Component {
  static propTypes = {
    style: PropTypes.object,
    showSpinner: PropTypes.bool,
    spinnerStyle: PropTypes.object,
    dataQa: PropTypes.string,
  };

  static defaultProps = {
    showSpinner: true,
  };

  render() {
    const { showSpinner, style, dataQa, spinnerStyle } = this.props;
    return (
      <div
        className="view-state-wrapper-overlay"
        style={style}
        data-qa={dataQa}
      >
        {showSpinner && <Spinner style={spinnerStyle} />}
      </div>
    );
  }
}
