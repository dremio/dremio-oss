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
import PropTypes from "prop-types";
import classNames from "clsx";

import "./MetricBar.less";

const MetricBar = ({ progress, className, isProgressBar, isLarge }) => {
  return (
    <div
      className={isProgressBar ? null : "metricContainer"}
      data-qa="metricContainer"
    >
      {isProgressBar ? (
        <progress
          className={classNames(
            "progressBar",
            className,
            isLarge ? "--large" : "--small"
          )}
          value={progress}
          max="100"
        />
      ) : (
        <div
          className={`metricContainer __metricBar ${
            isLarge ? "--large" : "--small"
          }`}
          style={{
            width: `${progress}%`,
          }}
        />
      )}
    </div>
  );
};

MetricBar.propTypes = {
  progress: PropTypes.number,
  className: PropTypes.string,
  isProgressBar: PropTypes.bool,
  isLarge: PropTypes.bool,
};

export default MetricBar;
