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
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { Radio } from 'components/Fields';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { body } from 'uiTheme/radium/typography';
import { formRow } from 'uiTheme/radium/forms';
import TransformForm, {formWrapperProps} from '../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';

const SECTIONS = [NewFieldSection];

export class DateToNumberForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
  }

  render() {
    const {fields, submit} = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={submit}
        style={{ minHeight: 0 }}
        >
        <div style={styles.radioWrap}>
          <Radio {...fields.format} style={styles.radio} label='Epoch' radioValue='EPOCH'/>
          <Radio {...fields.format} style={styles.radio} label='Excel' radioValue='EXCEL'/>
          <Radio {...fields.format} style={styles.radio} label='Julian' radioValue='JULIAN'/>
        </div>
        <NewFieldSection fields={fields}/>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { format, dropSourceField } = props.initialValues;
  return {
    initialValues: {
      format: format || 'EPOCH',
      newFieldName: props.columnName,
      dropSourceField: dropSourceField !== undefined ? dropSourceField : true
    }
  };
}

export default connectComplexForm({
  form: 'dateToNumber',
  fields: ['format'],
  overwriteOnInitialValuesChange: false
}, SECTIONS, mapStateToProps, null)(DateToNumberForm);

const styles = {
  radioWrap: {
    paddingLeft: 10
  },
  radio: {
    ...formRow,
    ...body,
    display: 'flex'
  }
};
