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
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { Radio } from 'components/Fields';
import NewFieldSection from 'components/Forms/NewFieldSection';
import TransformForm, {formWrapperProps} from '@app/pages/ExplorePage/components/forms/TransformForm';
import { transformProps } from '@app/pages/ExplorePage/components/forms/TransformationPropTypes';
import { radioStacked } from '@app/uiTheme/less/forms.less';
import { sectionMargin } from '@app/uiTheme/less/layout.less';

const SECTIONS = [NewFieldSection];

export class DateToNumberForm extends Component {
  static propTypes = transformProps;

  constructor(props) {
    super(props);
  }

  render() {
    const {fields, submit} = this.props;
    // radio button has much free space on top. To align radio label with top edge of a container, we have to add this negative margin
    const radiosAlignmentStyle = { marginTop: -7};
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={submit}
        style={{ minHeight: 0 }}
        >
        <div style={radiosAlignmentStyle}>
          <Radio {...fields.format} className={radioStacked} label='Epoch' style={{ marginTop: 0 }} radioValue='EPOCH'/>
          <Radio {...fields.format} className={radioStacked} label='Excel' radioValue='EXCEL'/>
          <Radio {...fields.format} className={radioStacked} label='Julian' radioValue='JULIAN'/>
        </div>
        <NewFieldSection fields={fields} className={sectionMargin}/>
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
  fields: ['format'],
  overwriteOnInitialValuesChange: false
}, SECTIONS, mapStateToProps, null)(DateToNumberForm);
