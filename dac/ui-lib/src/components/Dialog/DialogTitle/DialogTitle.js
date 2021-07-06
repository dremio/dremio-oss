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

import MuiDialogTitle from '@material-ui/core/DialogTitle';

import { ReactComponent as CloseIcon } from '../../../art/XLarge.svg';

import './dialogTitle.scss';

const DialogTitle = (props) => {
  const {
    children,
    onClose,
    classes
  } = props;

  const titleClasses = {
    root: clsx(['dialogTitle', { [classes.root]: classes.root }]),
    ...classes
  };

  return (
    <MuiDialogTitle
      classes={titleClasses}
      disableTypography
    >
      <span className='dialogTitle__content'><h2>{children}</h2></span>
      {onClose && <span className='dialogTitle__icon' onClick={onClose}><CloseIcon/></span>}
    </MuiDialogTitle>
  );
};

DialogTitle.propTypes = {
  children: PropTypes.node,
  classes: PropTypes.object,
  onClose: PropTypes.func,
  disableSpacing: PropTypes.bool
};

DialogTitle.defaultProps = {
  classes: {}
};

export default DialogTitle;
