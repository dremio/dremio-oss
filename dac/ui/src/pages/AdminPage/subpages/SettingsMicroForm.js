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
import { PureComponent } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { propTypes as reduxFormPropTypes } from "redux-form";
import clsx from "clsx";

import {
  connectComplexForm,
  InnerComplexForm,
} from "components/Forms/connectComplexForm.js";
import SimpleButton from "components/Buttons/SimpleButton";
import TextField from "components/Fields/TextField";
import Toggle from "components/Fields/Toggle";
import { isInteger, isNumber, isRequired } from "utils/validation";
import ApiUtils from "utils/apiUtils/apiUtils";
import settingActions from "actions/resources/setting";

import { FIELD_OVERRIDES, LABELS } from "./settingsConfig";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

export class SettingsMicroForm extends PureComponent {
  static propTypes = {
    ...reduxFormPropTypes,
    viewId: PropTypes.string.isRequired,
    settingId: PropTypes.string.isRequired,
    setting: PropTypes.instanceOf(Immutable.Map),
    putSetting: PropTypes.func.isRequired,
    fields: PropTypes.object.isRequired,
    showLabel: PropTypes.bool.isRequired,
    allowEmpty: PropTypes.bool,
    style: PropTypes.object,
    resetSetting: PropTypes.func,
    beforeSubmit: PropTypes.func,
  };

  static defaultProps = {
    showLabel: true,
  };

  submit = (form) => {
    if (!this.props.fields.value.dirty) {
      return Promise.resolve(); // early enter/return reject
    }

    const data = {
      ...this.props.setting.toJS(),
      ...form,
    };

    const type = this.props.setting.get("type");
    if (type === "INTEGER" || type === "FLOAT") {
      data.value = +data.value;
    }

    this.props.beforeSubmit?.();

    return ApiUtils.attachFormSubmitHandlers(
      this.props.putSetting(data, { viewId: this.props.viewId })
    );
  };

  remove = () => {
    this.props.resetSetting && this.props.resetSetting();
  };

  renderField(type) {
    const OverrideField = FIELD_OVERRIDES[this.props.settingId];
    const { from } = this.props;
    if (OverrideField) {
      return (
        <OverrideField
          {...this.props.fields.value}
          from={from}
          style={{ width: "500px" }}
        />
      );
    }

    switch (this.props.setting.get("type")) {
      case "BOOLEAN":
        return (
          <div
            style={{
              display: "inline-block",
              marginTop: type === "subHeaderBool" ? "-11px" : 2,
              marginLeft: 3,
            }}
          >
            <Toggle {...this.props.fields.value} />
          </div>
        );
      case "INTEGER":
        return (
          <TextField
            style={{ marginRight: "6px" }}
            {...this.props.fields.value}
            type="number"
            from={from}
            wrapperClassName={"input-wrapper-advanced"}
          />
        );
      // DX-56918 - "number" type prevents typing decimal point after DX-47588
      case "FLOAT": // todo: create dedicated int and number inputs
      case "TEXT":
      default:
        return (
          <TextField
            style={{ marginRight: "6px" }}
            {...this.props.fields.value}
            from={from}
            wrapperClassName={"input-wrapper-advanced"}
          />
        );
    }
  }

  render() {
    const { setting, showLabel, from } = this.props;
    if (!setting) {
      return null;
    }
    const id = setting.get("id");

    const buttonStyle = {
      verticalAlign: 0,
      minWidth: 50,
      display: "inline-block",
    };
    const saveButtonStyle = {
      ...buttonStyle,
      // use visibility so that it takes up space (e.g. in Support table)
      visibility: this.props.fields.value.dirty ? "visible" : "hidden",
    };

    if (id.includes("enable") && from === "queueControl") {
      saveButtonStyle.marginTop = "-8px";
      saveButtonStyle.marginLeft = "0px";
    } else if (from === "queueControl") {
      saveButtonStyle.marginTop = "21px";
      saveButtonStyle.marginLeft = "10px";
    }

    // if the reset button is showing then we want the save button to not take up space
    if (this.props.resetSetting && !this.props.fields.value.dirty) {
      saveButtonStyle.display = "none";
    }

    let label = null;
    if (showLabel && LABELS[id] !== "") {
      // todo: ax
      label =
        from === "queueControl" ? (
          <span
            style={{ display: "block", fontSize: "14px", marginBottom: "4px" }}
          >
            {LABELS[id] || id}
          </span>
        ) : (
          <b style={{ display: "block" }}>{LABELS[id] || id}:</b>
        );
    }
    return (
      <InnerComplexForm
        {...this.props}
        onSubmit={this.submit}
        style={{ display: "block" }}
      >
        <div
          style={{
            ...(this.props.style || {}),
            display: from === "queueControl" ? "flex" : "inline-block",
            paddingRight: 20,
          }}
        >
          <label>
            {id.includes("enable") && from === "queueControl" ? (
              <div
                style={{
                  display: "flex",
                  width: "510px",
                  justifyContent: "space-between",
                }}
              >
                {label}
                {this.renderField("subHeaderBool")}
              </div>
            ) : (
              <>
                {label}
                {this.renderField()}
              </>
            )}
          </label>
          {/* todo: by default buttons and textfields and toggles don't align. need to (carefully) fix */}
          <SimpleButton
            data-qa="save-support-key"
            buttonStyle="secondary"
            className={clsx(classes["secondaryButtonPsuedoClasses"])}
            style={saveButtonStyle}
          >
            {la("Save")}
          </SimpleButton>
          {this.props.resetSetting && (
            <SimpleButton
              data-qa="reset-support-key"
              onClick={this.remove}
              buttonStyle="secondary"
              className={clsx(classes["secondaryButtonPsuedoClasses"])}
              style={buttonStyle}
            >
              {la("Reset")}
            </SimpleButton>
          )}
        </div>
      </InnerComplexForm>
    );
  }
}

function mapToFormState(state, ownProps) {
  return {
    initialValues: {
      ...ownProps.initialValues,
      value: ownProps.setting && ownProps.setting.get("value"),
    },
  };
}

const ConnectedForm = connectComplexForm(
  {
    fields: ["value"],
    validate: (values, props) => {
      if (!props.setting) {
        return;
      }

      let errors = props.allowEmpty
        ? {}
        : {
            ...isRequired("value", "Value")(values),
          };

      const type = props.setting.get("type");
      if (type === "INTEGER") {
        errors = { ...errors, ...isInteger("value", "Value")(values) };
      } else if (type === "FLOAT") {
        errors = { ...errors, ...isNumber("value", "Value")(values) };
      }

      return errors;
    },
  },
  [],
  mapToFormState,
  {
    putSetting: settingActions.put.dispatch,
  }
)(SettingsMicroForm);

// This little guard makes sure that the form doesn't initialize with the wrong value type with the data is loading in
export default connect((state, ownProps) => {
  const setting = state.resources.entities.getIn([
    "setting",
    ownProps.settingId,
  ]);
  return { setting };
})((props) => {
  if (!props.setting) return null;
  return <ConnectedForm {...props} />;
});
