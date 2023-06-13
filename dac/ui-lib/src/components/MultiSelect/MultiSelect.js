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

import React, { useState, useRef, useMemo } from "react";
import PropTypes from "prop-types";
import clsx from "clsx";
import { get } from "lodash";

import Checkbox from "@mui/material/Checkbox";
import Chip from "@mui/material/Chip";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Tooltip from "../Tooltip/index";

import { ReactComponent as XIcon } from "../../art/XLarge.svg";

import Label from "../Label";

import "./multiSelect.scss";

const MultiSelectComponent = (props) => {
  const {
    classes,
    form: { errors, touched, setFieldValue } = {},
    handleChange,
    label,
    disabled,
    limitTags,
    name,
    options,
    placeholder,
    typeAhead,
    displayValues,
    value,
    onChange,
    loadNextRecords,
    nonClearableValue,
    getCustomChipIcon,
  } = props;

  const [showMenu, setShowMenu] = useState(false);
  const [filterText, setFilterText] = useState("");
  const inputRef = useRef(null);
  const valueContainerRef = useRef(null);

  const filteredValues = useMemo(() => {
    const noFilterText = !filterText || filterText === "";
    return (options || []).filter(
      ({ value: optionValue }) =>
        noFilterText ||
        optionValue.toLowerCase().indexOf(filterText.toLowerCase()) !== -1
    );
  }, [filterText, options]);

  const visibleValues = useMemo(() => {
    const preferredVisibleValues = displayValues.length ? displayValues : value;
    return limitTags && !showMenu
      ? preferredVisibleValues.slice(0, limitTags)
      : preferredVisibleValues;
  }, [value, limitTags, showMenu]); // eslint-disable-line react-hooks/exhaustive-deps

  const hasError = get(touched, name) && get(errors, name);
  const rootClass = clsx("multiSelect", { [classes.root]: classes.root });
  const valueClass = clsx(
    "multiSelect__value",
    { "--error": hasError },
    { [classes.value]: classes.value },
    { "--disabled": disabled }
  );

  const inputClass = clsx("multiSelect__input", "margin-top", {
    [classes.input]: classes.input,
  });

  const inputContainerClass = clsx("multiSelect__inputContainer", {
    "--disabled": disabled,
  });

  const labelClass = clsx("multiSelect__label", {
    [classes.label]: classes.label,
  });

  const updateValue = (updatedValue) => {
    if (setFieldValue && typeof setFieldValue === "function") {
      setFieldValue(name, updatedValue, true);
    }
    if (handleChange && typeof handleChange === "function") {
      handleChange(updatedValue);
    }
  };

  const removeValue = (deleteValue) => {
    const updatedValue = value.filter(
      (selectedVal) => selectedVal !== deleteValue
    );
    updateValue(updatedValue);
  };

  const addValue = (addedValue) => {
    const updatedValue = [...value, addedValue];
    updateValue(updatedValue);
  };

  const handleDelete = (event, deleteValue) => {
    let delValue = deleteValue;
    if (displayValues.length) {
      delValue = deleteValue.value;
    }
    removeValue(delValue);
    event.stopPropagation();
  };

  const handleOpen = () => {
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
    setFilterText("");
    inputRef.current.focus();
  };

  const handleChipClick = (e) => {
    e.stopPropagation();
  };

  const handleTypeAhead = (e) => {
    const stringValue = e.currentTarget.value;
    setFilterText(stringValue);
    onChange && onChange(stringValue);
  };

  const handleInputKeyDown = (e) => {
    const noFilterText = !filterText || filterText === "";
    if (noFilterText && value && value.length > 0 && e.key === "Backspace") {
      removeValue(value[value.length - 1]);
    }

    if (
      !noFilterText &&
      filteredValues.length === 1 &&
      e.key === "Enter" &&
      value.findIndex(
        (selectedVal) =>
          selectedVal.toLowerCase() === filteredValues[0].value.toLowerCase()
      ) === -1
    ) {
      addValue(filteredValues[0].value);
      setFilterText("");
    }

    if (!showMenu) {
      setShowMenu(true);
    }
  };

  const handleScroll = (event) => {
    const {
      target: { scrollHeight, scrollTop, clientHeight },
    } = event;
    const hasReachedBottom = scrollHeight - scrollTop === clientHeight;
    if (hasReachedBottom) {
      loadNextRecords && loadNextRecords(filterText);
    }
  };

  const handleClear = (e) => {
    if (nonClearableValue) {
      updateValue([nonClearableValue]);
    } else {
      updateValue([]);
    }
    onChange && onChange("");
    e.stopPropagation();
  };

  const getDisplayName = (val) => {
    if (displayValues.length) {
      return val.value;
    }
    const { label: displayName = val } =
      options.find(({ value: optionValue }) => val === optionValue) || {};
    return displayName;
  };

  const getChipIcon = (val) => {
    if (displayValues.length) {
      const Icon = val.icon;
      return Icon ? <Icon /> : null;
    }
    const { icon: IconComponent } =
      options.find(({ value: optionValue }) => val === optionValue) || {};
    return IconComponent ? <IconComponent /> : null;
  };

  const renderValue = () => {
    const hasValue = value && value.length > 0;
    const { innerRef } = props;
    return (
      <div
        ref={(node) => {
          valueContainerRef.current = node;
          if (innerRef) {
            innerRef.current = node;
          }
        }}
        className={valueClass}
        onClick={handleOpen}
      >
        <div className={inputContainerClass}>
          {visibleValues.map((selectedVal) => {
            const KEY = displayValues.length > 0 ? selectedVal.id : selectedVal;
            return (
              <Chip
                icon={getChipIcon(selectedVal)}
                classes={{
                  root: clsx(
                    "multiSelect__chip",
                    selectedVal === nonClearableValue &&
                      classes.nonClearableChip
                  ),
                  icon: "icon --md multiSelect__chip__icon",
                }}
                key={KEY}
                label={
                  <span title={getDisplayName(selectedVal)}>
                    {getDisplayName(selectedVal)}
                  </span>
                }
                onClick={handleChipClick}
                onDelete={
                  selectedVal !== nonClearableValue
                    ? (ev) => handleDelete(ev, selectedVal)
                    : null
                }
                deleteIcon={<XIcon />}
              />
            );
          })}
          {visibleValues.length < value.length && (
            <div className="margin-right margin-top">
              + {value.length - visibleValues.length} More
            </div>
          )}
          {typeAhead && (
            <input
              name={`${name}_typeahead`}
              onChange={handleTypeAhead}
              className={inputClass}
              autoComplete="off"
              value={filterText}
              ref={inputRef}
              onKeyDown={handleInputKeyDown}
              placeholder={placeholder && !hasValue ? placeholder : null}
            />
          )}
        </div>
        <div className="multiSelect__iconContainer">
          {hasValue && (
            <span className="multiSelect__clearIcon" onClick={handleClear}>
              <XIcon />
            </span>
          )}
        </div>
      </div>
    );
  };

  const renderMenuItemChipIcon = (item) => {
    const { icon: IconComponent } = item;
    if (getCustomChipIcon?.(item)) {
      return getCustomChipIcon?.(item);
    } else
      return IconComponent ? (
        <span className="multiSelect__optionIcon margin-right--half margin-left--half">
          <IconComponent />
        </span>
      ) : null;
  };

  const EllipisedMenuItem = ({ label }) => {
    const [showTooltip, setShowTooltip] = useState(false);
    return showTooltip ? (
      <Tooltip title={label}>
        <span className="multiSelect__label">{label}</span>
      </Tooltip>
    ) : (
      <span
        className="multiSelect__label"
        ref={(elem) => {
          if (elem?.offsetWidth < elem?.scrollWidth) {
            setShowTooltip(true);
          }
        }}
      >
        {label}
      </span>
    );
  };

  const renderMenuItems = () => {
    if (filteredValues.length === 0) {
      return <MenuItem>No values</MenuItem>;
    }

    return filteredValues.map((item, idx) => {
      const isSelected = value.indexOf(item.value) !== -1;
      const chip = renderMenuItemChipIcon(item);
      return (
        <MenuItem
          key={idx}
          value={item.value}
          onClick={() => handleMenuItemClick(item.value)}
          selected={isSelected}
          classes={{
            root: "multiSelect__option",
            selected: "multiSelect__option --selected",
          }}
          disabled={item.disabled}
        >
          {/* Todo: Use font icons for checkboxes */}
          <Checkbox
            checked={isSelected}
            color="primary"
            classes={{
              root: "gutter--none gutter-right--half multiSelect__checkbox",
            }}
            disabled={item.disabled}
          />
          <div className="multiSelect__label__container flex --alignCenter">
            {chip}
            <EllipisedMenuItem label={item.label} />
          </div>
        </MenuItem>
      );
    });
  };

  return (
    <div className={rootClass}>
      {label && (
        <Label
          value={label}
          className={labelClass}
          id={`select-label-${name}`}
        />
      )}
      {renderValue()}
      <Menu
        anchorEl={valueContainerRef.current}
        open={showMenu}
        onClose={handleClose}
        autoFocus={false}
        disableAutoFocus
        disableEnforceFocus
        transitionDuration={{
          exit: 0,
        }}
        MenuListProps={{
          disablePadding: true,
          className: "multiSelect__menuList",
          sx: {
            width: valueContainerRef.current?.clientWidth,
          },
        }}
        PaperProps={{
          onScroll: handleScroll,
        }}
      >
        {renderMenuItems()}
      </Menu>
    </div>
  );
};

MultiSelectComponent.propTypes = {
  innerRef: PropTypes.any,
  classes: PropTypes.shape({
    root: PropTypes.string,
    value: PropTypes.string,
    input: PropTypes.string,
    label: PropTypes.string,
    nonClearableChip: PropTypes.string,
  }),
  value: PropTypes.array,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string,
      value: PropTypes.string,
    })
  ).isRequired,
  handleChange: PropTypes.func,
  style: PropTypes.object,
  label: PropTypes.string,
  limitTags: PropTypes.number,
  name: PropTypes.string,
  form: PropTypes.object,
  typeAhead: PropTypes.bool,
  placeholder: PropTypes.string,
  loadNextRecords: PropTypes.func,
  onChange: PropTypes.func,
  displayValues: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string,
      value: PropTypes.string,
    })
  ),
  disabled: PropTypes.bool,
  nonClearableValue: PropTypes.string,
  getCustomChipIcon: PropTypes.func,
};

MultiSelectComponent.defaultProps = {
  classes: {},
  value: [],
  displayValues: [],
  style: {},
  label: null,
  name: "",
  typeAhead: true,
  hasChipIcon: false,
};

const MultiSelect = React.forwardRef((props, ref) => {
  return <MultiSelectComponent {...props} innerRef={ref} />;
});
export default MultiSelect;
