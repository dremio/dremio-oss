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

import "./formValidationMessage.scss";

const QUALIFIER = "qualifier";
const ERROR = "error";

const FormValidationMessage = ({ className, children, id, variant }) => {
  const updatedChildren = Array.isArray(children) ? children : [children];

  const rootClass = clsx(
    "validationError",
    { "--qualifier": variant === QUALIFIER },
    className,
  );

  return (
    <div className={rootClass} id={id}>
      {updatedChildren &&
        updatedChildren.map(
          (child, idx) => child && <div key={`error-${idx}`}>{child}</div>,
        )}
    </div>
  );
};

FormValidationMessage.propTypes = {
  id: PropTypes.string,
  children: PropTypes.oneOfType([
    PropTypes.arrayOf(PropTypes.string),
    PropTypes.string,
  ]).isRequired,
  variant: PropTypes.oneOf([QUALIFIER, ERROR]),
  className: PropTypes.string,
};

FormValidationMessage.defaultProps = {
  id: null,
  className: "",
  variant: ERROR,
};

export default FormValidationMessage;
