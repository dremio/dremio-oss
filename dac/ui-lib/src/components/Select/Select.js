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

import { noop } from "lodash";

import MuiSelect from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import { ReactComponent as ExpandMoreIcon } from "../../art/CaretDown.svg";

import Label from "../Label";

import "./select.scss";

const MENU_PROPS = {
  classes: {
    paper: "selectRoot__menu",
  },
  MenuListProps: {
    disablePadding: true,
  },
  anchorOrigin: {
    vertical: "bottom",
    horizontal: "left",
  },
  transformOrigin: {
    vertical: "top",
    horizontal: "left",
  },
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
    role,
    ...otherProps
  } = props;

  const rootClass = clsx("selectRoot", { [classes.root]: classes.root });
  const labelClass = clsx("selectRoot__label", {
    [classes.label]: classes.label,
  });
  const containerClass = clsx(
    "selectRoot__select",
    { "--disabled": disabled },
    classes.container
  );

  return (
    <div className={rootClass}>
      {label && (
        <Label
          value={label}
          className={labelClass}
          labelInnerClass={classes.labelInner}
          id={`select-label-${name}`}
          helpText={helpText}
        />
      )}

      <MuiSelect
        size="small"
        className={containerClass}
        MenuProps={MENU_PROPS}
        name={name}
        value={value || ""}
        onChange={onChange}
        IconComponent={ExpandMoreIcon}
        displayEmpty
        aria-labelledby={`select-label-${name}`}
        role={role || "combobox"}
        disabled={disabled}
        {...otherProps}
      >
        {options &&
          options.map(
            (
              {
                label: optionLabel,
                value: optionValue,
                disabled: optionDisabled = false,
                classes: itemClasses = {},
                onClick: optionOnClick,
              },
              idx
            ) => (
              <MenuItem
                key={idx}
                value={optionValue}
                classes={{
                  root: "selectRoot__option",
                  selected: "--selected",
                }}
                ListItemClasses={{
                  disabled: itemClasses.disabled,
                }}
                onClick={optionOnClick}
                disabled={optionDisabled}
              >
                {optionLabel}
              </MenuItem>
            )
          )}
      </MuiSelect>
    </div>
  );
};

Select.propTypes = {
  classes: PropTypes.shape({
    root: PropTypes.string,
    container: PropTypes.string,
    label: PropTypes.string,
    labelInner: PropTypes.string,
  }),
  value: PropTypes.string,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.node,
      value: PropTypes.string,
    })
  ).isRequired,
  onChange: PropTypes.func,
  label: PropTypes.string,
  name: PropTypes.string,
  helpText: PropTypes.string,
  disabled: PropTypes.bool,
  role: PropTypes.string,
};

Select.defaultProps = {
  classes: {},
  value: "",
  onChange: noop,
  label: null,
  name: "",
};

export default Select;
