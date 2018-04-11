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
import Radium from 'radium';

import { connectComplexForm } from 'components/Forms/connectComplexForm';

import TransformForm, { formWrapperProps } from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';

@Radium
export class NoParamForm extends Component {
  static propTypes = transformProps;

  render() {
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        style={{ minHeight: 0 }}
        onFormSubmit={this.props.submit}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const { dropSourceField } = props.initialValues;
  return {
    initialValues: {
      dataType: (props.toType || '').toUpperCase(),
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'noParamForm',
  fields: ['newFieldName', 'dropSourceField', 'dataType']
}, [], mapStateToProps, null)(NoParamForm);
