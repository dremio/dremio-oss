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
import classNames from 'classnames';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import Select from 'components/Fields/Select';

import NewFieldSection from 'components/Forms/NewFieldSection';
import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import Radio from 'components/Fields/Radio';
import DateInput from 'components/Fields/DateInput';
import actionUtils from 'utils/actionUtils/actionUtils';
import { applyValidators, isRequiredIfAnotherPropertyEqual} from 'utils/validation';
import { formLabel } from 'uiTheme/radium/typography';
import { isDateType, BOOLEAN } from 'constants/DataTypes';
import BooleanSelect from './BooleanSelect';
import {
  base,
  wrap,
  replacementValue as replacementValueCls,
  replacementType,
  select as selectCls
} from './ReplaceFooter.less';

@PureRender
@Radium
export default class ReplaceFooter extends Component {
  static getFields() {
    return ['replaceType', 'replacementValue', 'replaceSelectionType'];
  }

  static validate(values) {
    if (values.transformType === 'replace') {
      return applyValidators(values, [
        isRequiredIfAnotherPropertyEqual('replacementValue', 'replaceType', 'NULL', 'Replacement value')
      ]);
    }
  }

  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    handleReplacementValue: PropTypes.func,
    toggleBottomRadioBtn: PropTypes.func,
    submitForm: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.options = [
      {
        label: 'Matching Text',
        option: 'Matching Text'
      },
      {
        label: 'Entire Value',
        option: 'Entire Value'
      }
    ];

    this.patternOptions = [
      {
        label: 'Text Selection',
        option: 'SELECTION'
      },
      {
        label: 'Entire Value',
        option: 'VALUE'
      }
    ];
  }

  onChange(data) {
    const { fields: { replacementValue } } = this.props;
    replacementValue.onChange(data);
    actionUtils.runAutoPreview(this.props.submitForm);
  }

  renderDefaultBtns() {
    return (
      <div style={[style.item]}>
        <label>Replacement Value</label>
        <input
          style={[style.input]}
          onChange={this.props.handleReplacementValue}/>
      </div>
    );
  }

  renderFooterByType() {
    const { transform } = this.props;
    const columnType = transform.get('columnType');
    const hash = {
      '#': () => this.renderDefaultBtns(),
      'A': () => this.renderDefaultBtns(),
      'integer': () => this.renderRadioBtn(),
      'timestamp': () => this.renderRadioBtn()
    };

    if (hash[columnType]) {
      return hash[columnType]();
    }
  }

  renderRadioBtn() {
    return (
      <div className='item-block'>
        <input
          defaultChecked
          onChange={this.props.toggleBottomRadioBtn}
          type='radio'/>
        <label className='item-label'>Value:</label>
        <input type='number'/>
        <input
          defaultChecked={false}
          onChange={this.props.toggleBottomRadioBtn}
          type='radio'/>
        <label className='item-label'>Null</label>
      </div>
    );
  }
  renderReplaceValueInput(columnType, replacementValue) {
    if (isDateType(columnType)) {
      return <DateInput type={columnType} {...replacementValue} />;
    }
    if (columnType === BOOLEAN) {
      return <BooleanSelect
        {...replacementValue}
        style={{ ...style.text, marginRight: 5 }}
      />;
    }
    return <TextField
      data-qa='replaceValueFooter'
      {...replacementValue}
      onChange={this.onChange}
      style={style.text}
    />;
  }

  render() {
    const { transform, fields: { replaceType, replacementValue, replaceSelectionType } } = this.props;
    const columnName = transform.get('columnName');
    const columnType = transform.get('columnType');
    const selectHash = { // todo: loc
      Values: null,
      Pattern: <Select className={selectCls} items={this.patternOptions} {...replaceSelectionType}/>
    };
    const select = selectHash[transform.get('method')] || null;

    return (
      <div className={classNames('replace-footer', base)}>
        {this.renderFooterByType()}
        {select && <div className={wrap}>
          <span style={formLabel}>{la('Replace')}</span>
          <div className={replacementType}>{select}</div>
        </div>
        }
        <div className={wrap}>
          <span style={formLabel}>{la('Replacement value')}</span>
          <div className={replacementValueCls}>
            <Radio
              {...replaceType}
              radioValue='VALUE'
              label='Value' />
            <FieldWithError
              {...replacementValue}
              errorPlacement='bottom'
              style={formLabel}>
              {this.renderReplaceValueInput(columnType, replacementValue)}
            </FieldWithError>
            <Radio
              {...replaceType}
              radioValue='NULL'
              label='Null'/>
          </div>
        </div>
        <NewFieldSection columnName={columnName} fields={this.props.fields} style={{ marginBottom: 0}}/>
      </div>
    );
  }
}

const style = {
  input: {
    width: 228,
    height: 29,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    padding: 5
  },
  item: {
    float: 'left',
    maxWidth: 228,
    marginLeft: 5
  }
};
