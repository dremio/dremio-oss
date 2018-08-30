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

import Select from 'components/Fields/Select';
import Radio from 'components/Fields/Radio';
import TextField from 'components/Fields/TextField';

import NewFieldSection from 'components/Forms/NewFieldSection';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import TransformForm, {formWrapperProps} from '@app/pages/ExplorePage/components/forms/TransformForm';
import { transformProps } from '@app/pages/ExplorePage/components/forms/TransformationPropTypes';
import { radioStacked, sectionTitle, rowMargin, fieldsHorizontalSpacing } from '@app/uiTheme/less/forms.less';
import { delimiterContainer, customDelimiterInput, newField } from './ConvertListToTextForm.less';

const SECTIONS = [NewFieldSection];
// todo: loc

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
        'option': '',
        'label': 'Custom…'
      }
    ];
  }

  renderListContent() {
    const { delimiter, format } = this.props.fields;
    return (
      <div>
        <div>
          <span className={sectionTitle}>{la('Options:')}</span>
          <Radio {...format} label='Delimiter' radioValue='text' className={radioStacked} />
          <Radio {...format} label='JSON encoding' radioValue='json' className={radioStacked} />
        </div>
        <div className={sectionTitle}>{la('Delimiter:')}</div>
        <div>
          <div className={rowMargin}>{la('Separate list objects with a')}</div>
          <div className={delimiterContainer}>
            <Select
              className={fieldsHorizontalSpacing}
              disabled={format.value !== 'text'}
              items={this.options}
              {...delimiter}
              value={this.options.some(e => e.option === delimiter.value) ? delimiter.value : ''}/>
            <TextField
              className={customDelimiterInput}
              disabled={format.value !== 'text'}
              {...delimiter}
              type='text'/>
          </div>
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
        <NewFieldSection fields={fields} className={newField} />
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
