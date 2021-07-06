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
import noop from 'lodash.noop';

import FormControl from '@material-ui/core/FormControl';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import RadioGroup from '@material-ui/core/RadioGroup';
import MaterialRadio from '@material-ui/core/Radio';

import Label from '../Label';

const Radio = (props) => {
  const {
    align,
    classes,
    defaultValue,
    label,
    name,
    onChange,
    options = [],
    value
  } = props;

  const rootClassName = clsx('radio-root', { [classes.root]: classes.root });
  const labelClassName = clsx('radio-label', { [classes.label]: classes.label });
  const optionContainerClassName = clsx('radio-option-container', { [classes.optionsContainer]: classes.optionsContainer });

  const rowAlign = align === 'row';
  return (
    <div className={rootClassName}>
      {label && <Label value={label} className={labelClassName} />}
      <FormControl component='fieldset' classes={{ root: optionContainerClassName }}>
        <RadioGroup aria-label={name} name={name} value={value} defaultValue={defaultValue} onChange={onChange} row={rowAlign}>
          {
            options.map(({ label: labelText, optValue, disabled, ...otherProps }, index) =>
              <FormControlLabel key={`${name}_radio_${index}`} value={optValue} disabled={disabled} control={<MaterialRadio color='primary' />} label={labelText} {...otherProps}/>
            )
          }
        </RadioGroup>
      </FormControl>
    </div>
  );
};

Radio.propTypes = {
  align: PropTypes.oneOf([
    'row',
    'column'
  ]),
  classes: PropTypes.shape({
    root: PropTypes.string,
    label: PropTypes.string,
    optionsContainer: PropTypes.string
  }),
  defaultValue: PropTypes.string,
  label: PropTypes.string,
  name: PropTypes.string,
  onChange: PropTypes.func,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string,
      value: PropTypes.string
    })
  ).isRequired,
  value: PropTypes.string
};

Radio.defaultProps = {
  align: 'column',
  classes: {},
  defaultValue: '',
  label: null,
  name: '',
  onChange: noop,
  value: ''
};

export default Radio;
