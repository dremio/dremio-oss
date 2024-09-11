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
import { useState, useRef } from "react";
import PropTypes from "prop-types";
import classNames from "clsx";

import { Tooltip } from "components/Tooltip";

import "./TextWithHelp.less";

export const DARK = "dark";
export const LIGHT = "light";

const TextWithHelp = (props) => {
  const { text, helpText, showToolTip, className, color } = props;

  const textRef = useRef(null);
  const [isHover, setIsHover] = useState(false);

  const onMouseEnter = () => {
    const { clientWidth, scrollWidth } = textRef.current || {};
    setIsHover(clientWidth < scrollWidth || showToolTip);
  };

  const onMouseLeave = () => {
    setIsHover(false);
  };

  const tooltipClass = classNames("textWithHelp__tooltip", {
    "--light": color === LIGHT,
  });

  const arrowClass = classNames("textWithHelp__tooltipArrow", {
    "--light": color === LIGHT,
  });

  const contentClass = classNames("textWithHelp__content", {
    [className]: className,
  });

  return (
    <div
      className="textWithHelp"
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className={contentClass} ref={textRef}>
        {helpText || text}
      </div>
      <Tooltip
        key="tooltip"
        target={() => (isHover ? textRef.current : null)}
        placement="bottom-start"
        tooltipInnerClass={tooltipClass}
        tooltipArrowClass={arrowClass}
      >
        {text}
      </Tooltip>
    </div>
  );
};

TextWithHelp.propTypes = {
  text: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
  helpText: PropTypes.any,
  showToolTip: PropTypes.bool,
  className: PropTypes.string,
  color: PropTypes.oneOf([DARK, LIGHT]),
};

TextWithHelp.defaultProps = {
  className: "",
  color: DARK,
};

export default TextWithHelp;
