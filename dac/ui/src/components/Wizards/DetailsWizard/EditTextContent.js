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
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import ConvertCaseOptions from 'components/Forms/ConvertCaseOptions';
import TrimWhiteSpaceOptions from 'components/Forms/TrimWhiteSpaceOptions';
import NewFieldSection from 'components/Forms/NewFieldSection';
import TransformForm, { formWrapperProps } from 'pages/ExplorePage/components/forms/TransformForm';
import { transformProps } from 'pages/ExplorePage/components/forms/TransformationPropTypes';
import { sectionMargin } from '@app/uiTheme/less/layout.less';

const SECTIONS = [NewFieldSection, ConvertCaseOptions, TrimWhiteSpaceOptions];

@PureRender
@Radium
class EditTextContent extends Component {

  static propTypes = {
    ...transformProps,
    type: PropTypes.string.isRequired
  };

  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { transform, columnName, fields } = this.props;
    const content = this.props.type !== 'trim'
      ? <ConvertCaseOptions fields={fields}/>
      : <TrimWhiteSpaceOptions fields={fields}/>;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        transform={transform}
        onFormSubmit={this.props.submit}
        style={{ minHeight: 0 }}
        >
        <div style={[style.base]} className='edit-text-content'>
          {content}
          <NewFieldSection columnName={columnName} fields={fields} className={sectionMargin} />
        </div>
      </TransformForm>
    );
  }
}

const style = {
  base: {
    height: '100%',
    position: 'relative'
  }
};

export function getEditTextInitialValues(detailType, columnName) {
  return {
    newFieldName: columnName,
    action: detailType !== 'TRIM_WHITE_SPACES' ? 'UPPERCASE' : 'BOTH',
    dropSourceField: true
  };
}

function mapToFormState(state, props) {
  const { type } = state.routing.locationBeforeTransitions.query;
  return {
    initialValues: getEditTextInitialValues(type, props.columnName)
  };
}

export default connectComplexForm({
  form: 'convertCase'
}, SECTIONS, mapToFormState, null)(EditTextContent);
