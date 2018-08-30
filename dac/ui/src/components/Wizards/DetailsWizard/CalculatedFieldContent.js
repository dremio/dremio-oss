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
import Immutable from 'immutable';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';

import { applyValidators, isRequired } from 'utils/validation';
import exploreUtils from 'utils/explore/exploreUtils';
import NewFieldSection from 'components/Forms/NewFieldSection';
import AddFieldEditor from './components/AddFieldEditor';
import DefaultWizardFooter from './../components/DefaultWizardFooter';
import { content } from './CalculatedFieldContent.less';

function validate(values) {
  return applyValidators(values, [isRequired('expression'), isRequired('newFieldName')]);
}

@PureRender
@Radium
class CalculatedFieldContent extends Component {

  static propTypes = {
    columnName: PropTypes.string,
    columns: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    submit: PropTypes.func.isRequired,
    cancel: PropTypes.func.isRequired,
    changeFormType: PropTypes.func.isRequired,
    valid: PropTypes.bool,
    submitting: PropTypes.bool,
    error: PropTypes.object,
    errors: PropTypes.object,
    dragType: PropTypes.string
  };
  render() {
    const { fields: { expression }, columnName, errors, valid } = this.props;
    return (
      <div className='convert-case-content'>
        <InnerComplexForm
          {...this.props}
          onSubmit={this.props.submit}>
          <AddFieldEditor
            pageType='details'
            {...expression}
            activeMode
            dragType={this.props.dragType}
            />
          <div className={content}>
            <NewFieldSection columnName={columnName} fields={this.props.fields} showDropSource={Boolean(columnName)}/>
          </div>
          <DefaultWizardFooter
            onCancel={this.props.cancel}
            submitting={!valid && Object.keys(errors).length !== 0}
            onFormSubmit={this.props.submit} />
        </InnerComplexForm>
      </div>
    );
  }
}

const mapStateToProps = (state, props) => {
  const fieldsForColumn = props.columnName
  ? { sourceColumnName: props.columnName, dropSourceField: true }
  : { sourceColumnName: props.columns && props.columns.getIn([0, 'name']), dropSourceField: false};
  return {
    initialValues: {
      newFieldName: props.columnName || la('new_field'),
      expression: props.columnName ? exploreUtils.escapeFieldNameForSQL(props.columnName) : '',
      ...fieldsForColumn,
      ...props.initialValues
    }
  };
};

export default connectComplexForm({
  form: 'calculatedField',
  fields: ['expression', 'sourceColumnName'],
  validate
}, [NewFieldSection], mapStateToProps, null)(CalculatedFieldContent);
