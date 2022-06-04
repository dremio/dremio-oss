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

import MuiSelect from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import { ReactComponent as ExpandMoreIcon } from '../../art/ArrowDown.svg';

import Label from '../Label';

import './select.scss';

const MENU_PROPS = {
  classes: {
    paper: 'selectRoot__menu'
  },
  MenuListProps: {
    disablePadding: true
  }
};

const Select = (props) => {

  const {
    classes,
    disabled,
    value,
    options,
    onChange,
    label,
    name,
    helpText,
    ...otherProps
  } = props;

  const rootClass = clsx('selectRoot', { [classes.root]: classes.root });
  const labelClass = clsx('selectRoot__label', { [classes.label]: classes.label });
  const containerClass = clsx(
    'selectRoot__select',
    { '--disabled': disabled }
  );

  return (
    <div className={rootClass}>
      { label && <Label value={label} className={labelClass} id={`select-label-${name}`} helpText={helpText}/> }

      <MuiSelect
        classes={{
          root: containerClass
        }}
        MenuProps={MENU_PROPS}
        name={name}
        value={value || ''}
        onChange={onChange}
        IconComponent={ExpandMoreIcon}
        displayEmpty
        aria-labelledby={`select-label-${name}`}
        role='combobox'
        disabled={disabled}
        {...otherProps}
      >
        {options && options.map(({
          label: optionLabel,
          value: optionValue,
          disabled:optionDisabled = false,
          classes: itemClasses = {}
        }, idx) => (
          <MenuItem
            key={idx}
            value={optionValue}
            classes={{
              selected: 'selectRoot__option --selected'
            }}
            ListItemClasses ={{
              disabled: itemClasses.disabled
            }}
            disabled={optionDisabled}
          >
            {optionLabel}
          </MenuItem>
        ))}
      </MuiSelect>
    </div>
  );
};

Select.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    label: PropTypes.string
  }),
  value: PropTypes.string,
  options: PropTypes.arrayOf(PropTypes.shape({
    label: PropTypes.node,
    value: PropTypes.string
  })).isRequired,
  onChange: PropTypes.func,
  label: PropTypes.string,
  name: PropTypes.string,
  helpText: PropTypes.string,
  disabled: PropTypes.bool
};

Select.defaultProps = {
  classes: {},
  value: '',
  onChange: noop,
  label: null,
  name: ''
};

export default Select;
