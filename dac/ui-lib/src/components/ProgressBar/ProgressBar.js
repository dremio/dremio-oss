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

import React from "react";
import PropTypes from "prop-types";

import "./progressBar.scss";

const ProgressBar = (props) => {
  const { value, max, color } = props;

  return (
    <div
      className="progressBar"
      style={{
        width: `${Math.floor((value / max) * 100)}%`,
      }}
    >
      <div style={{ backgroundColor: color }} className="progressBar__value" />
    </div>
  );
};

ProgressBar.propTypes = {
  value: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  color: PropTypes.string,
};

ProgressBar.defaultProps = {
  color: "",
};

export default ProgressBar;
