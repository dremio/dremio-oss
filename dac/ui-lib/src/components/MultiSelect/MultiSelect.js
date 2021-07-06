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

import React, { useState, useRef, useMemo } from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import get from 'lodash.get';

import Checkbox from '@material-ui/core/Checkbox';
import Chip from '@material-ui/core/Chip';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import { makeStyles } from '@material-ui/core/styles';

import { ReactComponent as ExpandMoreIcon } from '../../art/ArrowDown.svg';
import { ReactComponent as XIcon } from '../../art/XLarge.svg';

import Label from '../Label';

import './multiSelect.scss';

const MultiSelect = (props) => {

  const {
    classes,
    form: {
      errors,
      touched,
      setFieldValue
    } = {},
    handleChange,
    label,
    limitTags,
    name,
    options,
    placeholder,
    typeAhead,
    value
  } = props;

  const getMenuClass = (anchor) => makeStyles(() => {
    const {
      clientWidth: width
    } = anchor || {};
    return {
      list: {
        width
      }
    };
  })();

  const [showMenu, setShowMenu] = useState(false);
  const [filterText, setFilterText] = useState('');
  const inputRef = useRef(null);
  const valueContainerRef = useRef(null);

  const filteredValues = useMemo(() => {
    const noFilterText = !filterText || filterText === '';
    return (options || [])
      .filter(({ value: optionValue }) =>
        noFilterText ||
        optionValue.toLowerCase().indexOf(filterText.toLowerCase()) !== -1
      );
  }, [filterText]);

  const visibleValues = useMemo(() => (
    limitTags && !showMenu ? value.slice(0, limitTags) : value
  ), [value, limitTags, showMenu]);

  const hasError = get(touched, name) && get(errors, name);
  const rootClass = clsx(
    'multiSelect',
    { [classes.root]: classes.root }
  );
  const valueClass = clsx(
    'multiSelect__value',
    { '--error': hasError },
    { [classes.value]: classes.value }
  );

  const inputClass = clsx(
    'multiSelect__input',
    'margin-top',
    { [classes.input]: classes.input }
  );

  const labelClass = clsx('multiSelect__label', { [classes.label]: classes.label });

  const updateValue = (updatedValue) => {
    if (setFieldValue && typeof setFieldValue === 'function') {
      setFieldValue(name, updatedValue, true);
    }
    if (handleChange && typeof handleChange === 'function') {
      handleChange(updatedValue);
    }
  };

  const removeValue = (deleteValue) => {
    const updatedValue = value.filter((selectedVal) => selectedVal !== deleteValue);
    updateValue(updatedValue);
  };

  const addValue = (addedValue) => {
    const updatedValue = [...value, addedValue];
    updateValue(updatedValue);
  };

  const handleDelete = (event, deleteValue) => {
    removeValue(deleteValue);
    event.stopPropagation();
  };

  const handleOpen = (e) => {
    setShowMenu(true);
    inputRef.current.focus();
  };

  const handleClose = () => {
    setShowMenu(false);
    inputRef.current.blur(); // Todo: This doesn't work for some reason. Needs a fix.
  };

  const handleMenuItemClick = (selectedValue) => {
    if (value.indexOf(selectedValue) === -1) {
      addValue(selectedValue);
    } else {
      removeValue(selectedValue);
    }
    setFilterText('');
    inputRef.current.focus();
  };

  const handleChipClick = (e) => {
    e.stopPropagation();
  };

  const handleTypeAhead = (e) => {
    setFilterText(e.currentTarget.value);
  };

  const handleInputKeyDown = (e) => {
    const noFilterText = !filterText || filterText === '';
    if (noFilterText &&
      value &&
      value.length > 0 &&
      e.key === 'Backspace'
    ) {
      removeValue(value[value.length - 1]);
    }

    if (!noFilterText &&
      filteredValues.length === 1 &&
      e.key === 'Enter' &&
      value.findIndex((selectedVal) => selectedVal.toLowerCase() === filteredValues[0].value.toLowerCase()) === -1
    ) {
      addValue(filteredValues[0].value);
      setFilterText('');
    }

    if (!showMenu) {
      setShowMenu(true);
    }
  };

  const handleClear = (e) => {
    updateValue([]);
    e.stopPropagation();
  };

  const getDisplayName = (val) => {
    const { label: displayName = '' } = options.find(({ value: optionValue }) => val === optionValue) || {};
    return displayName;
  };

  const renderValue = () => {
    const hasValue = value && value.length > 0;
    return (
      <div
        ref={valueContainerRef}
        className={valueClass}
        onClick={handleOpen}
      >
        <div className='multiSelect__inputContainer'>
          {visibleValues.map((selectedVal) => (
            <Chip
              classes={{ root: 'margin-right margin-top' }}
              key={selectedVal}
              label={getDisplayName(selectedVal)}
              onClick={handleChipClick}
              onDelete={(ev) => handleDelete(ev, selectedVal)}
            />
          ))}
          {
            (visibleValues.length < value.length) && (
              <div className='margin-right margin-top'>
                + {value.length - visibleValues.length} More
              </div>
            )
          }
          {typeAhead && <input
            name={`${name}_typeahead`}
            onChange={handleTypeAhead}
            className={inputClass}
            value={filterText}
            ref={inputRef}
            onKeyDown={handleInputKeyDown}
            placeholder={placeholder && !hasValue ? placeholder : null}
          />}
        </div>
        <div className='multiSelect__iconContainer'>
          {hasValue && <span
            className='multiSelect__clearIcon'
            onClick={handleClear}
          >
            <XIcon/>
          </span>}
          <span className='multiSelect__dropdownIcon'>
            <ExpandMoreIcon />
          </span>
        </div>
      </div>
    );
  };

  const renderMenuItems = () => {
    if (filteredValues.length === 0) {
      return (
        <MenuItem>
          No values
        </MenuItem>
      );
    }

    return filteredValues.map(({ label: optionLabel, value: optionValue }, idx) => {
      const isSelected = value.indexOf(optionValue) !== -1;
      return (
        <MenuItem
          key={idx}
          value={optionValue}
          onClick={() => handleMenuItemClick(optionValue)}
          selected={isSelected}
          classes={{
            root: 'multiSelect__option',
            selected: 'multiSelect__option --selected'
          }}
        >
          {/* Todo: Use font icons for checkboxes */}
          <Checkbox
            checked={isSelected}
            color='primary'
            classes={{ root: 'gutter--none gutter-right--half' }}
          />
          {optionLabel}
        </MenuItem>
      );
    });
  };

  return (
    <div className={rootClass}>
      {label && <Label value={label} className={labelClass} id={`select-label-${name}`}/>}
      {renderValue()}
      <Menu
        anchorEl={valueContainerRef.current}
        open={showMenu}
        onClose={handleClose}
        classes={getMenuClass(valueContainerRef.current)}
        autoFocus={false}
        disableAutoFocus
        disableEnforceFocus
        getContentAnchorEl={null}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        transformOrigin={{ vertical: 'top', horizontal: 'center' }}
        transitionDuration={{
          exit: 0
        }}
        MenuListProps={{
          disablePadding: true,
          className: 'multiSelect__menuList'
        }}
      >
        {renderMenuItems()}
      </Menu>
    </div>
  );
};

MultiSelect.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    value: PropTypes.string,
    input: PropTypes.string,
    label: PropTypes.string
  }),
  value: PropTypes.array,
  options: PropTypes.arrayOf(PropTypes.shape({
    label: PropTypes.string,
    value: PropTypes.string
  })).isRequired,
  handleChange: PropTypes.func,
  style: PropTypes.object,
  label: PropTypes.string,
  limitTags: PropTypes.number,
  name: PropTypes.string,
  form: PropTypes.object,
  typeAhead: PropTypes.bool,
  placeholder: PropTypes.string
};

MultiSelect.defaultProps = {
  classes: {},
  value: [],
  style: {},
  label: null,
  name: '',
  typeAhead: true
};

export default MultiSelect;
