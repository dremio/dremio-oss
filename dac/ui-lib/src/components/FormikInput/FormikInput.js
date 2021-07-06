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

import FormValidationMessage from '../FormValidationMessage';
import Label from '../Label';

import './formikInput.scss';

const FormikInput = (props) => {

  const {
    label,
    labelStyle,
    classes = {},
    disabled,
    field,
    field: {
      name
    },
    form: {
      errors,
      touched
    },
    hideError,
    prefix,
    ...otherProps
  } = props;

  const hasError = get(touched, name) && get(errors, name);
  const showError = !hideError && hasError;

  const rootClass = clsx('input-root', { [classes.root]: classes.root });
  const containerClass = clsx(
    'input__container',
    { '--disabled': disabled },
    { '--error': hasError }
  );
  const inputClass = clsx({ [classes.input]: classes.input });

  return (
    <div className={rootClass}>
      {label && <Label
        value={label}
        className={classes.label}
        style={labelStyle}
        id={`input-label-${name}`}
      />}
      <div className={containerClass}>
        {prefix && <span className='input__prefix'>{prefix}</span>}
        <input
          className={inputClass}
          aria-labelledby={`input-label-${name}`}
          disabled={disabled}
          {...field}
          {...otherProps}
        />
      </div>
      {showError && (
        <FormValidationMessage className='input__validationError' id={`input-error-${name}`}>
          {get(errors, name)}
        </FormValidationMessage>
      )}
    </div>
  );
};

FormikInput.propTypes = {
  disabled: PropTypes.bool,
  label: PropTypes.string,
  labelStyle: PropTypes.object,
  classes: PropTypes.shape({
    root: PropTypes.string,
    input: PropTypes.string
  }),
  field: PropTypes.object.isRequired,
  form: PropTypes.object.isRequired,
  hideError: PropTypes.bool,
  prefix: PropTypes.string
};

FormikInput.defaultProps = {
  disabled: false,
  label: null,
  labelStyle: {},
  classes: {},
  hideError: false
};

export default FormikInput;
