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

import React from 'react';
import PropTypes from 'prop-types';

import clsx from 'clsx';

import './modalFormAction.scss';
import Button from '../../Button';
import * as ButtonTypes from '../../Button/ButtonTypes';

export const MODAL_FORM_ACTION_DIRECTION = {
  LEFT: 'left',
  RIGHT: 'right'
};

const ModalFormAction = (props) => {
  const {
    children,
    classes,
    className,
    dataQa,
    disabled,
    onClick,
    onMouseDown,
    color,
    variant,
    style,
    text,
    title,
    type,
    direction
  } = props;

  const updatedClassName = clsx([
    'modalFormAction',
    { 'modalFormAction--left': direction === MODAL_FORM_ACTION_DIRECTION.LEFT },
    { 'modalFormAction--right': direction === MODAL_FORM_ACTION_DIRECTION.RIGHT }
  ]);

  return (
    <span className={updatedClassName}>
      <Button
        classes={classes}
        className={className}
        dataQa={dataQa}
        disabled={disabled}
        disableMargin
        onClick={onClick}
        onMouseDown={onMouseDown}
        color={color}
        variant={variant}
        style={style}
        text={text}
        title={title}
        type={type}
      >
        {children}
      </Button>
    </span>
  );
};

ModalFormAction.propTypes = {
  children: PropTypes.any,
  classes: PropTypes.object,
  className: PropTypes.string,
  disabled: PropTypes.bool,
  text: PropTypes.string,
  type: PropTypes.string,
  color: PropTypes.oneOf(ButtonTypes.COLORS_ARRAY),
  variant: PropTypes.oneOf(ButtonTypes.VARIANTS_ARRAY),
  onClick: PropTypes.oneOfType([PropTypes.func, PropTypes.bool]),
  onMouseDown: PropTypes.func,
  style: PropTypes.object,
  title: PropTypes.string,
  dataQa: PropTypes.string,
  direction: PropTypes.oneOf(Object.values(MODAL_FORM_ACTION_DIRECTION))
};

ModalFormAction.defaultProps = {
  direction: MODAL_FORM_ACTION_DIRECTION.LEFT
};

export default ModalFormAction;
