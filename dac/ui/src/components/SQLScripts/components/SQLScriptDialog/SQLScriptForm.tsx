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

import * as React from "react";
import {
  connectComplexForm,
  FormBody,
  ModalForm,
  modalFormProps,
} from "#oss/components/Forms";
import { FieldWithError, TextField } from "components/Fields";
import { formRow } from "uiTheme/radium/forms";
import { applyValidators, isRequired } from "utils/validation";

type SQLScriptFormProps = {
  onFormSubmit: (values: any) => void;
  handleSubmit: any;
  initialValues: any;
  fields: any;
  intl: any;
  error: any;
  saveAsDialogError: any;
  onCancel: () => void;
};

export const FIELDS = ["name"];

function validate(values: any): any {
  return applyValidators(values, [isRequired("name")]);
}

function SQLScriptForm(props: SQLScriptFormProps): React.ReactElement {
  const {
    fields: { name },
    handleSubmit,
    onFormSubmit,
    initialValues,
    intl,
    error,
    saveAsDialogError,
  } = props;
  const endpointError =
    saveAsDialogError || error
      ? {
          error: error
            ? error.message.get("errorMessage")
            : saveAsDialogError?.responseBody?.errorMessage,
          touched: true,
        }
      : {};
  return (
    <ModalForm
      {...modalFormProps(props)}
      hideError
      onSubmit={handleSubmit(onFormSubmit)}
    >
      <FormBody>
        <div style={formRow}>
          <FieldWithError
            label={intl.formatMessage({ id: "Script.Name" })}
            {...name}
            {...endpointError}
          >
            <TextField
              initialFocus
              initialValue={initialValues && initialValues.name}
              {...name}
              {...endpointError}
            />
          </FieldWithError>
        </div>
      </FormBody>
    </ModalForm>
  );
}

export default connectComplexForm(
  {
    form: "saveAsScript",
    fields: FIELDS,
    validate,
  },
  [],
  null,
  null,
)(SQLScriptForm);
