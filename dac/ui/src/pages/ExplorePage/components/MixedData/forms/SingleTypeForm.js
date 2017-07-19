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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import { startCase } from 'lodash/string';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import NewFieldSection from 'components/Forms/NewFieldSection';
import {TextField, Select, Radio, Checkbox} from 'components/Fields';
import { applyValidators, isRequired } from 'utils/validation';

import { body } from 'uiTheme/radium/typography';
import { formSectionTitle } from 'uiTheme/radium/exploreTransform';
import { FLEX_COL_START, INLINE_NOWRAP_ROW_FLEX_START } from 'uiTheme/radium/flexStyle';
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
      <div style={styles.actionsWrap}>
        <span style={formSectionTitle}>{la('Action for Non-matching Values')}</span>
        <Radio
          {...actionForNonMatchingValue}
          onChange={this.selectNonMatchingActions}
          style={styles.radio}
          label='Delete records'
          radioValue='DELETE_RECORDS'/>
        <Radio
          {...actionForNonMatchingValue}
          onChange={this.selectNonMatchingActions}
          style={styles.radio}
          label='Replace values with null'
          radioValue='REPLACE_WITH_NULL'/>
        <div style={FLEX_COL_START}>
          <Radio
            {...actionForNonMatchingValue}
            onChange={this.selectNonMatchingActions}
            style={styles.radio}
            label='Replace values with:'
            radioValue='REPLACE_WITH_DEFAULT'/>
          <TextField
            disabled={actionForNonMatchingValue.value !== 'REPLACE_WITH_DEFAULT'}
            {...defaultValue}
            style={{marginLeft: 10}}/>
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
        <div style={styles.base}>
          <div>
            <div style={styles.desiredTypeSection}>
              <div style={formSectionTitle}>{la('Desired Type')}</div>
              <div style={styles.desiredTypeWrap}>
                <Select {...fields.desiredType} dataQa='desiredType' items={desiredTypeItems} />
                <Checkbox
                  {...fields.castWhenPossible}
                  label={la('Cast when possible')}
                  style={{marginLeft: 5, ...body}}/>
              </div>
            </div>
            {this.renderNonMatchingActions()}
            <div style={styles.newFieldWrap}>
              <NewFieldSection fields={fields}/>
            </div>
          </div>
          <div style={styles.nonMatchingWrap}>
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

const styles = {
  base: {
    ...INLINE_NOWRAP_ROW_FLEX_START
  },
  desiredTypeSection: {
    marginLeft: 10,
    width: 450
  },
  desiredTypeWrap: {
    display: 'flex',
    alignItems: 'center',
    marginBottom: 10
  },
  actionsWrap: {
    ...FLEX_COL_START,
    ...body,
    marginLeft: 10,
    marginBottom: 10
  },
  nonMatchingWrap: {
    marginLeft: 20
  },
  newFieldWrap: {
    marginBottom: 10
  },
  radio: {
    margin: '5px 0 5px 0'
  }
};
