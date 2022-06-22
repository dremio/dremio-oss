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

import { noop } from "lodash";

const ExternalLink = (props) => {
  const { href, children, className, disableRedirect, onClick, ...otherProps } =
    props;
  return disableRedirect ? (
    <span className={className}>{children}</span>
  ) : (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={className}
      onClick={onClick}
      {...otherProps}
    >
      {children}
    </a>
  );
};

ExternalLink.propTypes = {
  href: PropTypes.string.isRequired,
  className: PropTypes.string,
  children: PropTypes.oneOfType([
    PropTypes.arrayOf(PropTypes.node),
    PropTypes.oneOfType([PropTypes.string, PropTypes.node]),
  ]),
  disableRedirect: PropTypes.bool,
  onClick: PropTypes.func,
};

ExternalLink.defaultProps = {
  className: "",
  children: [],
  disableRedirect: false,
  onClick: noop,
};

export default ExternalLink;
