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

import SortMultiplyController from 'pages/ExplorePage/components/MultiplySort/SortMultiplyController';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';
import Message from 'components/Message';
import { applyValidators, notEmptyArray } from 'utils/validation';

import DefaultWizardFooter from './../components/DefaultWizardFooter';

@PureRender
@Radium
class SortMultiplyContent extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static validate(values) {
    const errors = applyValidators(values, [
      notEmptyArray('sortColumns', 'Select at least one column.')
    ]);
    return errors;
  }

  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    columns: PropTypes.instanceOf(Immutable.List),
    changeFormType: PropTypes.func.isRequired,
    submit: PropTypes.func.isRequired,
    cancel: PropTypes.func.isRequired,
    fields: PropTypes.object,
    error: PropTypes.string
  };

  render() {
    const { error, submit } = this.props;
    return (
      <div style={[style.base]} className='sort-multiply-content'>
        <InnerComplexForm
          {...this.props}
          onSubmit={submit}>
          {error && <Message messageType='error' message={error.message} messageId={error.id} />}
          <SortMultiplyController
            dataset={this.props.dataset}
            columns={this.props.columns}
            fields={this.props.fields}
            location={this.context.location}
        />
          <DefaultWizardFooter
            {...this.props}
            onCancel={this.props.cancel}
            onFormSubmit={submit}/>
        </InnerComplexForm>
      </div>
    );
  }
}

const style = {
  'base': {
    'height': '100%'
  }
};

export default connectComplexForm({
  form: 'sortMultiply',
  fields: [
    'sortColumns[].direction',
    'sortColumns[].name'
  ],
  validate: SortMultiplyContent.validate
}, [], null, null)(SortMultiplyContent);
