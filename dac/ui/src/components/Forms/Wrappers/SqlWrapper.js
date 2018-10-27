/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import PropTypes from 'prop-types';
import SQLEditor from 'components/SQLEditor';
import FieldWithError from 'components/Fields/FieldWithError';

import { flexContainer, fieldWithError, sqlBody } from './FormWrappers.less';

export default class SqlWrapper extends Component {
  static propTypes = {
    elementConfig: PropTypes.object,
    field: PropTypes.object,
    disabled: PropTypes.bool
  };

  // interlnal ref to SQLEditor
  sqlEditor = null;

  //TODO get errors structure with range for SQL error highlighting from failed API response
  errors = null;

  getMonacoEditorInstance() {
    return this.sqlEditor.monacoEditorComponent.editor;
  }
  handleChange = () => {
    //handle field updates manually
    const value = this.getMonacoEditorInstance().getValue();
    this.props.field.onChange(value);
  };

  render() {
    const {elementConfig, field, disabled} = this.props;
    const value = field && field.value;
    return (
      <div className={flexContainer}>
        <FieldWithError {...field}
          label={elementConfig.getConfig().label}
          name={elementConfig.getPropName()}
          errorPlacement='top'
          className={fieldWithError}>
          <div className={sqlBody}>
            <SQLEditor
              height={elementConfig.getConfig().height}
              ref={(ref) => this.sqlEditor = ref}
              defaultValue={value}
              onChange={this.handleChange}
              readOnly={disabled}
              errors={this.errors}
              autoCompleteEnabled={elementConfig.getConfig().autoCompleteEnabled}
            />
          </div>
        </FieldWithError>
      </div>
    );
  }
}

