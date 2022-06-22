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
// NESSIE REFACTOR DX-44738
import { Component } from "react";
import TextField from "components/Fields/TextField";
import FieldWithError from "components/Fields/FieldWithError";
import FormUtils from "utils/FormUtils/FormUtils";

import PropTypes from "prop-types";
import { getProjectIdFromUrl, getProjectUrl } from "@app/utils/nessieUtils";
import RepoSelector from "./RepoSelector";

import {
  flexContainer,
  fieldWithError,
  textFieldWrapper,
  textFieldBody,
  textFieldBodyFixed,
} from "./FormWrappers.less";

export default class TextWrapper extends Component {
  // NESSIE ONE OFF
  constructor(props) {
    super(props);
    const { value, initialValue } = props.field;
    this.state = {
      projectId: getProjectIdFromUrl(value ? value : initialValue) || null,
    };
  }
  /////

  static propTypes = {
    elementConfig: PropTypes.object,
    fields: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool,
    editing: PropTypes.bool,
  };

  onChangeHandler = (e) => {
    const { elementConfig, field } = this.props;
    if (
      elementConfig &&
      elementConfig.getConfig().scale &&
      field &&
      field.onChange
    ) {
      e.target.value = FormUtils.revertScaleValue(
        e.target.value,
        elementConfig.getConfig().scale
      );
      field.onChange(e);
    } else if (field && field.onChange) {
      field.onChange(e);
    }
  };

  onChangeProjectId = (id) => {
    const { field } = this.props;
    this.setState({ projectId: id });
    if (!field || !field.onChange) return;
    if (id !== "customRepo") {
      field.onChange(getProjectUrl(id));
    }
  };

  render() {
    const { elementConfig, field } = this.props;
    const elementConfigJson = elementConfig.getConfig();
    const {
      type,
      secure,
      tooltip,
      disabled,
      scale,
      size,
      label,
      focus,
      placeholder,
      errorPlacement,
      propertyName,
    } = elementConfigJson;
    const numberField = type === "number" ? { type: "number" } : null;
    const passwordField = secure ? { type: "password" } : null;
    const hoverHelpText = tooltip ? { hoverHelpText: tooltip } : null;
    const isFixedSize = typeof size === "number" && size > 0;
    const { value, onChange, initialValue, ...fieldProps } = field;
    // NESSIE ONE OFF
    const projectId = this.state.projectId;
    const isNessieField = propertyName === "config.nessieEndpoint";
    const curVal = value || initialValue;
    const isNessieProject =
      isNessieField &&
      (projectId ? getProjectUrl(projectId) === curVal : false);
    const nessieIsDisabled =
      isNessieField && projectId !== "customRepo" && isNessieProject;
    /////
    const currentValue = numberField
      ? FormUtils.scaleValue(value, scale)
      : value;
    const onChangeHandler = numberField ? this.onChangeHandler : onChange;
    const fieldClass = isFixedSize ? textFieldBodyFixed : textFieldBody;
    const style = isFixedSize ? { width: size } : {};
    const isDisabled =
      disabled ||
      this.props.disabled ||
      // special case in source forms: source name can not be changed after initial creation
      (elementConfig.getPropName() === "name" && this.props.editing) ||
      nessieIsDisabled
        ? { disabled: true }
        : null;

    return (
      <div className={flexContainer} style={{ flexDirection: "column" }}>
        {/* NESSIE ONE OFF */}
        {isNessieField && (
          <RepoSelector
            onChange={this.onChangeProjectId}
            {...((isNessieProject || projectId === null) && {
              defaultValue: projectId,
            })}
          />
        )}
        {/* ///// */}
        <FieldWithError
          errorPlacement={errorPlacement || "top"}
          {...field}
          {...hoverHelpText}
          label={label}
          name={elementConfig.getPropName()}
          className={fieldWithError}
        >
          <div className={textFieldWrapper}>
            <TextField
              initialFocus={focus}
              {...fieldProps}
              {...numberField}
              {...passwordField}
              {...isDisabled}
              value={currentValue}
              onChange={onChangeHandler}
              placeholder={placeholder || ""}
              style={style}
              className={fieldClass}
            />
          </div>
        </FieldWithError>
      </div>
    );
  }
}
