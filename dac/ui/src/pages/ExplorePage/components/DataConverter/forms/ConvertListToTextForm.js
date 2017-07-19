/*
 * Copyright (C) 2017 Dremio Corporation
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

import Select from 'components/Fields/Select';
import Radio from 'components/Fields/Radio';
import TextField from 'components/Fields/TextField';

import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { formLabel, body } from 'uiTheme/radium/typography';
import { FLEX_NOWRAP_COL_START, LINE_START_CENTER } from 'uiTheme/radium/flexStyle';

import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';

const SECTIONS = [NewFieldSection];

@Radium
export class ConvertListToTextForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
    this.options = [
      {
        'option': ',',
        'label': 'Comma'
      },
      {
        'option': ' ',
        'label': 'Space'
      },
      {
        'option': '|',
        'label': 'Pipe'
      },
      {
        'option': '  ',
        'label': 'Custom...'
      }
    ];
  }

  renderListContent() {
    const { delimiter, format } = this.props.fields;
    return (
      <div style={styles.base}>
        <div style={styles.options}>
          <span style={formLabel}>{la('Options:')}</span>
          <Radio {...format} style={body} label='Delimiter' radioValue='text'/>
          <Radio {...format} style={body} label='JSON encoding' radioValue='json'/>
        </div>
        <span style={formLabel}>{la('Delimiter:')}</span>
        <div style={styles.options}>
          <span style={body}>{la('Separate list objects with a')}</span>
          <Select
            disabled={format.value !== 'text'}
            items={this.options}
            {...delimiter}/>
          <TextField
            style={styles.input}
            disabled={format.value !== 'text'}
            {...delimiter}
            type='text'/>
        </div>
      </div>
    );
  }

  render() {
    const {fields} = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.props.submit}>
        {this.renderListContent()}
        <NewFieldSection fields={fields}/>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { delimiter, format, dropSourceField } = props.initialValues;
  return {
    initialValues: {
      format: format || 'text',
      delimiter: delimiter || ',',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'convertListToTextForm',
  fields: ['format', 'delimiter'],
  overwriteOnInitialValuesChange: false
}, SECTIONS, mapStateToProps, null)(ConvertListToTextForm);

const styles = {
  base: {
    margin: '10px 0 10px 10px',
    ...FLEX_NOWRAP_COL_START
  },
  options: {
    ...LINE_START_CENTER,
    marginBottom: 10
  },
  input: {
    height: 28,
    width: 100,
    marginLeft: 10,
    borderRadius: 2,
    border: '1px solid #81d1eb'
  }
};
