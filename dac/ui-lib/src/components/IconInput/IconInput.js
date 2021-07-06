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
import get from 'lodash.get';

import { makeStyles } from '@material-ui/core/styles';

import FormValidationMessage from '../FormValidationMessage';
import Label from '../Label';

import './iconInput.scss';

const IconInput = (props) => {

  const {
    label,
    labelStyle,
    classes = {},
    field,
    field: {
      name
    },
    form: {
      errors,
      touched
    },
    hideError,
    icon: Icon,
    iconColor,
    showIcon,
    ...otherProps
  } = props;

  const useStylesBase = makeStyles(theme => {
    const {
      typography: {
        fontSize,
        fontFamily,
        fontWeight
      } = {}
    } = theme || {};

    return {
      input: {
        fontSize,
        fontFamily,
        fontWeight
      }
    };
  });

  const classesBase = useStylesBase();

  const rootClass = clsx('input-root', { [classes.root]: classes.root });
  const inputClass = clsx(classesBase.input, 'iconInput-input', { [classes.input]: classes.input });
  const showError = !hideError && get(touched, name) && get(errors, name);

  return (
    <div className={rootClass}>
      <Label value={label} className={classes.label} style={labelStyle} id={`icon-input-label-${name}`} />
      <div className='input-container'>
        <input
          className={inputClass}
          aria-labelledby={`icon-input-label-${name}`}
          {...field}
          {...otherProps}
        />
        {showIcon && <Icon fontSize='small' classes={{ root: 'iconInput-icon' }} htmlColor={iconColor} data-testid={`icon-input-icon-${name}`} />}
      </div>
      {showError && (
        <FormValidationMessage className='input-validation-error' id={`icon-input-error-${name}`}>
          {get(errors, name)}
        </FormValidationMessage>
      )}
    </div>
  );
};

IconInput.propTypes = {
  label: PropTypes.string.isRequired,
  labelStyle: PropTypes.object,
  classes: PropTypes.shape({
    root: PropTypes.string,
    input: PropTypes.string
  }),
  field: PropTypes.object.isRequired,
  form: PropTypes.object.isRequired,
  hideError: PropTypes.bool,
  icon: PropTypes.object.isRequired,
  iconColor: PropTypes.string,
  showIcon: PropTypes.bool
};

IconInput.defaultProps = {
  labelStyle: {},
  classes: {},
  hideError: false,
  iconColor: '#000000',
  showIcon: false
};

export default IconInput;
