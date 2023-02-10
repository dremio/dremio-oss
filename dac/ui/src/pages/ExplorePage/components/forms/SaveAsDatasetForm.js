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
import { Component } from "react";

import PropTypes from "prop-types";

import { ModalForm, FormBody, modalFormProps } from "components/Forms";
import { FieldWithError, TextField } from "components/Fields";
import { applyValidators, isRequired } from "utils/validation";
import { getInitialResourceLocation, constructFullPath } from "utils/pathUtils";
import ResourceTreeContainer from "components/Tree/ResourceTreeContainer";
import { connectComplexForm } from "components/Forms/connectComplexForm";
import Message from "components/Message";
import { formRow, label } from "uiTheme/radium/forms";

import * as classes from "./SaveAsDatasetForm.module.less";

export const FIELDS = ["name", "location", "reapply"];

function validate(values) {
  return applyValidators(values, [isRequired("name"), isRequired("location")]);
}

const locationType = PropTypes.string;
// I would like to enforce that initial value and field value for location has the same type,
// as inconsistency in these 2 parameters, cause redux-form treat a form as dirty.
// I created this as function, not as standalone object, to avoid eslint errors that requires to
// document the rest of redux-form properties: onChange, error, touched
const getLocationPropType = () =>
  PropTypes.shape({
    value: locationType,
  });

export class SaveAsDatasetForm extends Component {
  static propTypes = {
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    message: PropTypes.string,
    canReapply: PropTypes.bool,
    datasetType: PropTypes.string,
    handleSubmit: PropTypes.func.isRequired,
    dependentDatasets: PropTypes.array,
    updateFormDirtyState: PropTypes.func,

    // redux-form
    initialValues: PropTypes.shape({
      location: locationType,
    }),
    fields: PropTypes.shape({
      location: getLocationPropType(),
      name: PropTypes.object,
    }),
  };

  static contextTypes = {
    location: PropTypes.object,
  };

  handleChangeSelectedNode = (nodeId, node) => {
    const { onChange } = this.props.fields.location;
    if (!nodeId && !node) {
      onChange(undefined); //Clears selection
    } else {
      onChange(
        (node && constructFullPath(node.get("fullPath").toJS())) || nodeId
      );
    }
  };

  renderHistoryWarning() {
    const { version, tipVersion } = this.context.location.query;
    if (tipVersion && tipVersion !== version) {
      return (
        <Message
          messageType="warning"
          message={la("You may lose your previous changes.")}
          isDismissable={false}
        />
      );
    }

    return null;
  }

  submitOnEnter = (preventSubmit) => (e) => {
    const { handleSubmit, onFormSubmit } = this.props;
    if (e.key === "Enter") {
      e.preventDefault();
      if (preventSubmit) {
        return;
      }
      handleSubmit(onFormSubmit)();
    }
  };

  render() {
    const {
      fields: { name, location },
      handleSubmit,
      onFormSubmit,
      message,
    } = this.props;
    const preventSubmit = !location.value;
    return (
      <ModalForm
        {...modalFormProps(this.props)}
        {...(preventSubmit && { canSubmit: false })}
        onSubmit={handleSubmit(onFormSubmit)}
        wrapperStyle={{
          height: "100%",
          display: "flex",
          flexDirection: "column",
        }}
      >
        {this.renderHistoryWarning()}
        <FormBody className={classes["form-body"]}>
          {message && <div style={formRow}>{message}</div>}
          <div style={formRow}>
            <FieldWithError label="Name" {...name}>
              <TextField
                initialFocus
                {...name}
                onKeyDown={this.submitOnEnter(preventSubmit)}
              />
            </FieldWithError>
          </div>
          <div className={classes["tree-container"]}>
            <label style={label}>Location</label>
            <ResourceTreeContainer
              className={classes["resource-tree"]}
              hideDatasets
              onChange={this.handleChangeSelectedNode}
              preselectedNodeId={location.initialValue}
              fromModal
            />
            {this.props.fields.location.error &&
              this.props.fields.location.touched && (
                <Message
                  messageType="error"
                  message={this.props.fields.location.error}
                />
              )}
          </div>
        </FormBody>
      </ModalForm>
    );
  }
}

const mapStateToProps = (state, props) => ({
  initialValues: {
    location: getInitialResourceLocation(
      props.fullPath,
      props.datasetType,
      state.account.getIn(["user", "userName"])
    ),
  },
});

export default connectComplexForm(
  {
    form: "saveAsDataset",
    fields: FIELDS,
    validate,
  },
  [],
  mapStateToProps,
  null
)(SaveAsDatasetForm);
