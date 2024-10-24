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
import Immutable from "immutable";

import { FieldWithError, TextField } from "#oss/components/Fields";
import { connectComplexForm } from "#oss/components/Forms/connectComplexForm";
import { FormBody, ModalForm, modalFormProps } from "#oss/components/Forms";
import { changeWorkersSize } from "#oss/actions/resources/provisioning";
import { applyValidators, isNumber, isRequired } from "#oss/utils/validation";

import ResourceSummary from "./../ResourceSummary";

const VIEW_ID = "AdjustWorkersForm";
const FIELDS = ["containerCount"];

function validate(values) {
  return {
    ...applyValidators(values, [
      isRequired("containerCount", laDeprecated("Executors count")),
      isNumber("containerCount", laDeprecated("Executors count")),
    ]),
  };
}

export class AdjustWorkersForm extends Component {
  static propTypes = {
    onCancel: PropTypes.func,
    handleSubmit: PropTypes.func,
    changeWorkersSize: PropTypes.func,
    entity: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    values: PropTypes.object,
    style: PropTypes.object,
  };
  static defaultProps = {
    entity: Immutable.Map(),
  };

  submit = (values) => {
    return this.props
      .changeWorkersSize(values, this.props.entity.get("id"), VIEW_ID)
      .then(() => {
        this.props.onCancel(); //hide form
        return null;
      });
  };

  render() {
    const { fields, handleSubmit, style, entity } = this.props;
    return (
      <ModalForm
        {...modalFormProps(this.props)}
        onSubmit={handleSubmit(this.submit)}
        confirmText={laDeprecated("Adjust")}
      >
        <FormBody style={style}>
          <ResourceSummary entity={entity} />
          <FieldWithError
            style={styles.formRow}
            label={laDeprecated("Executors")}
            labelStyle={styles.formLabel}
            {...fields.containerCount}
          >
            <TextField
              {...fields.containerCount}
              type="number"
              style={{ width: 80 }}
              step={1}
              min={0}
            />
          </FieldWithError>
        </FormBody>
      </ModalForm>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const initialValues = {
    containerCount:
      ownProps.entity.getIn(["dynamicConfig", "containerCount"]) || 0,
  };
  return {
    initialValues,
  };
}

export default connectComplexForm(
  {
    form: "automaticAcceleration",
    validate,
    fields: FIELDS,
    initialValues: {},
  },
  [],
  mapStateToProps,
  {
    changeWorkersSize,
  },
)(AdjustWorkersForm);

const styles = {
  formRow: {
    display: "flex",
    width: "80px",
  },
  formLabel: {
    display: "block",
    margin: "6px 42px 0 0",
    fontWeight: 500,
    fontSize: 12,
    color: "var(--text--primary)",
  },
};
