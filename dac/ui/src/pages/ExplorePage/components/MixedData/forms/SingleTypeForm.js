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
import PropTypes from 'prop-types';
import { startCase } from 'lodash/string';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import NewFieldSection from 'components/Forms/NewFieldSection';
import {TextField, Select, Radio, Checkbox} from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';
import { sectionMargin } from '@app/uiTheme/less/layout.less';

import {
  radioStacked,
  title,
  sectionTitle,
  inputForRadio,
  fieldsHorizontalSpacing,
  columnsContainer,
  rowMargin,
  firstColumn,
  secondColumn,
  rowOfInputs
} from '@app/uiTheme/less/forms.less';
import classNames from 'classnames';
import TransformForm, {formWrapperProps} from './../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';
import NonMatchingValues from './../NonMatchingValues';

const SECTIONS = [NewFieldSection];

const validate = (values) => applyValidators(values, [isRequired('desiredType', 'Desired Type')]);

@Radium
export class SingleTypeForm extends Component {
  static propTypes = {
    nonMatchingCount: PropTypes.number,
    availableNonMatching: PropTypes.array,
    singles: PropTypes.array,
    ...transformProps
  };

  static defaultProps = {
    singles: []
  };

  static getDesiredTypeItems(singles, castWhenPossible) {
    const filteredSingles = singles.filter(
      single => single.castWhenPossible === Boolean(castWhenPossible));

    return filteredSingles.map(single => ({
      label: startCase(single.desiredType.toLowerCase()),
      option: single.desiredType
    }));
  }

  getCurrentDesiredTypeItem() {
    const {fields, singles} = this.props;
    const desiredType = fields.desiredType.value;
    const castWhenPossible = fields.castWhenPossible.value;
    return singles.find(
      (single) => single.desiredType === desiredType && single.castWhenPossible === castWhenPossible
    );
  }

  selectNonMatchingActions = value => {
    const { defaultValue, actionForNonMatchingValue } = this.props.fields;
    if (value !== 'REPLACE_WITH_DEFAULT') {
      defaultValue.onChange('');
    }
    actionForNonMatchingValue.onChange(value);
  }

  renderNonMatchingActions() {
    const { actionForNonMatchingValue, defaultValue } = this.props.fields;
    return (
      <div>
        <div className={sectionTitle}>{la('Action for Non-matching Values')}</div>
        <Radio
          {...actionForNonMatchingValue}
          onChange={this.selectNonMatchingActions}
          className={radioStacked}
          label='Delete records'
          radioValue='DELETE_RECORDS'/>
        <Radio
          {...actionForNonMatchingValue}
          onChange={this.selectNonMatchingActions}
          className={radioStacked}
          label='Replace values with null'
          radioValue='REPLACE_WITH_NULL'/>
        <div>
          <Radio
            {...actionForNonMatchingValue}
            onChange={this.selectNonMatchingActions}
            className={radioStacked}
            label='Replace values with:'
            radioValue='REPLACE_WITH_DEFAULT'/>
          <TextField
            className={inputForRadio}
            disabled={actionForNonMatchingValue.value !== 'REPLACE_WITH_DEFAULT'}
            {...defaultValue} />
        </div>
      </div>
    );
  }


  render() {
    const {submit, fields, singles} = this.props;
    const desiredTypeItems = SingleTypeForm.getDesiredTypeItems(singles, fields.castWhenPossible.value);
    const {nonMatchingCount, availableNonMatching} = this.getCurrentDesiredTypeItem() || {};
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={submit}
        submitting={this.props.submitting}>
        <div className={columnsContainer}>
          <div className={firstColumn}>
            <div>
              <div className={title}>{la('Desired Type')}</div>
              <div className={classNames([rowOfInputs, rowMargin])}>
                <Select {...fields.desiredType} dataQa='desiredType' items={desiredTypeItems} className={fieldsHorizontalSpacing} />
                <Checkbox
                  {...fields.castWhenPossible}
                  label={la('Cast when possible')}/>
              </div>
            </div>
            {this.renderNonMatchingActions()}
            <div>
              <NewFieldSection fields={fields} className={sectionMargin} />
            </div>
          </div>
          <div className={secondColumn}>
            <NonMatchingValues nonMatchingCount={nonMatchingCount || 0 } values={availableNonMatching || []}/>
          </div>
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { columnName } = props || {};
  const desiredTypeItem = SingleTypeForm.getDesiredTypeItems(props.singles, false)[0];
  return {
    initialValues: {
      typeMixed: 'convertToSingleType',
      defaultValue: '',
      desiredType: desiredTypeItem && desiredTypeItem.option,
      castWhenPossible: false,
      actionForNonMatchingValue: 'DELETE_RECORDS',
      newFieldName: columnName,
      dropSourceField: true
    }
  };
}

export default connectComplexForm({
  form: 'convertToSingleType',
  fields: ['typeMixed', 'actionForNonMatchingValue', 'desiredType', 'castWhenPossible', 'defaultValue'],
  validate
}, SECTIONS, mapStateToProps, null)(SingleTypeForm);
