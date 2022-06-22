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

/**
 * @file - that define all Button Types.
 */
import * as ButtonTypes from "./ButtonTypes";

import "./button.scss";

/**
 * @description
 * Button class that define standard button.
 * If you want to bind clicker to this component just assign this function to onClick property.
 */
const Button = (props) => {
  const {
    classes,
    className,
    children,
    color,
    dataQa,
    disabled,
    disableMargin,
    onClick,
    onMouseDown,
    style,
    text,
    title,
    type,
    variant,
  } = props;

  const handleClick = (evt) => {
    if (onClick && !disabled) {
      onClick(evt);
    }
  };

  const rootClassName = clsx([
    "btn",
    { btn__primary: color === ButtonTypes.PRIMARY },
    { btn__secondary: color === ButtonTypes.SECONDARY },
    { btn__warn: color === ButtonTypes.WARNING },
    { btn__default: color === ButtonTypes.DEFAULT },
    { "--contained": variant === ButtonTypes.CONTAINED },
    { "--outlined": variant === ButtonTypes.OUTLINED },
    { "--text": variant === ButtonTypes.TEXT },
    { "--disabled": disabled },
    { "margin-top--double margin-bottom--double": !disableMargin },
    className,
    { [classes.root]: classes.root },
  ]);

  const contentClassName = clsx([
    "btn__content",
    { [classes.content]: classes.content },
  ]);

  return (
    <button
      className={rootClassName}
      data-qa={dataQa || text}
      onMouseDown={onMouseDown}
      onClick={handleClick}
      style={style}
      title={title}
      type={type}
      disabled={disabled}
    >
      <span className={contentClassName}>{children ? children : text}</span>
    </button>
  );
};

Button.propTypes = {
  children: PropTypes.any,
  classes: PropTypes.object,
  className: PropTypes.string,
  disabled: PropTypes.bool,
  disableMargin: PropTypes.bool,
  text: PropTypes.string,
  type: PropTypes.string,
  color: PropTypes.oneOf(ButtonTypes.COLORS_ARRAY).isRequired,
  variant: PropTypes.oneOf(ButtonTypes.VARIANTS_ARRAY).isRequired,
  onClick: PropTypes.oneOfType([PropTypes.func, PropTypes.bool]),
  onMouseDown: PropTypes.func,
  style: PropTypes.object,
  title: PropTypes.string,
  dataQa: PropTypes.string,
};

Button.defaultProps = {
  disabled: false,
  disableSubmit: false,
  disableMargin: false,
  color: ButtonTypes.DEFAULT,
  variant: ButtonTypes.CONTAINED,
  classes: {},
  text: "",
};

export default Button;
