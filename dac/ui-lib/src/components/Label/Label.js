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

import clsx from "clsx";

import HoverHelp from "../HoverHelp";

import "./label.scss";

const Label = ({
  helpText,
  value,
  className,
  labelInnerClass,
  style,
  ...props
}) => {
  const containerClasses = clsx("label", { [className]: className });
  const labelClasses = clsx({ [labelInnerClass]: labelInnerClass });
  return (
    <div className={containerClasses}>
      <div role="label" className={labelClasses} style={style} {...props}>
        {value}
      </div>
      {helpText && (
        <HoverHelp arrow content={helpText} placement="bottom-start" />
      )}
    </div>
  );
};

Label.propTypes = {
  value: PropTypes.string.isRequired,
  className: PropTypes.string,
  labelInnerClass: PropTypes.string,
  style: PropTypes.object,
  helpText: PropTypes.string,
};

Label.defaultProps = {
  className: "",
  style: {},
};

export default Label;
