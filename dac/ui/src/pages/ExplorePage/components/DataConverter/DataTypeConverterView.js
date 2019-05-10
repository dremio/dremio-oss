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

import Tabs from 'components/Tabs';
import Select from 'components/Fields/Select';

import dataStoreUtils from 'utils/dataStoreUtils';
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

import { formLabel } from 'uiTheme/radium/typography';

import classNames from 'classnames';
import { typeToIconType, BINARY, TEXT, INTEGER, FLOAT, DECIMAL, LIST, DATE, TIME, DATETIME, MAP, BOOLEAN } from 'constants/DataTypes';
import SimpleButton from 'components/Buttons/SimpleButton';
import {
  NoParamToBinary,
  NoParamToDateTimeTimestamp,
  NoParamToFloat,
  NoParamToInt,
  NoParamToJSON,
  NoParamToText
} from 'components/Menus/ExplorePage/ColumnTypeMenu';
import {
  padding,
  selectLeftAligned,
  rowMargin,
  typeElement,
  selectItem as selectItemCls
} from './DataTypeConverterView.less';

import NonMatchingForm from './forms/NonMatchingForm';
import ConvertDateToTextForm from './forms/DateToTextForm';
import ConvertTextToDateForm from './forms/TextToDateForm';
import ConvertListToTextForm from './forms/ConvertListToTextForm';
import ConvertFloatToIntForm from './forms/ConvertFloatToIntForm';
import DateAndNumberForm from './forms/DateAndNumberForm';
import NoParamForm from './forms/NoParamForm';

const DateToNumberForm = props => <DateAndNumberForm form='dateToNumber' {...props} />;
const NumberToDateForm = props => <DateAndNumberForm form='numberToDate' {...props} />;

const NoParam = {
  BINARY: NoParamToBinary,
  TEXT: NoParamToText,
  FLOAT: NoParamToFloat,
  INTEGER: NoParamToInt,
  TIME: NoParamToDateTimeTimestamp,
  DATE: NoParamToDateTimeTimestamp,
  DATETIME: NoParamToDateTimeTimestamp,
  JSON: NoParamToJSON
};

@Radium
export default class DataTypeConverterView extends Component {
  static propTypes = {
    columnName: PropTypes.string,
    columnType: PropTypes.string,
    submit: PropTypes.func,
    cancel: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.dataForSelection = dataStoreUtils.getDataTypeForConverter(); // BINARY to TEXT is disabled due to BE bug DX-4110
    this.options = this.dataForSelection.find((data) => data.type === props.columnType).values;
    this.onConvertTypeChange = this.onConvertTypeChange.bind(this);
    this.onSubmit = this.onSubmit.bind(this);
  }

  onConvertTypeChange(value) {
    const { location, router } = this.context;
    router.push({ ...location, state: { ...location.state, toType: value }});
  }

  onSubmit(values, submitType) {
    const { submit } = this.props;
    const { toType, columnType } = this.context.location.state;

    if (submitType === 'apply') {
      this.setTransformationInLocalStorage({...values, columnType, toType});
    }
    return submit({ toType, ...values }, submitType);
  }

  setTransformationInLocalStorage = (values) => {
    const { newFieldName, ...restValues } = values;

    localStorageUtils.setTransformValue(restValues);
  }

  getTransformationValuesFromLocalStorage = () => {
    const { columnType, toType } = this.context.location.state;

    return localStorageUtils.getTransformValue(columnType, toType);
  }

  renderSelect() {
    const { toType } = this.context.location.state;
    const defaultValue = 'Select Type';
    const value = toType || defaultValue;
    const options = toType ? this.options : [{label: defaultValue, disabled: true}, ...this.options];
    return (
      <div className={padding}>
        <div style={styles.convertToLabel}>{la('Convert to')}</div>
        <div className={ rowMargin }>
          <Select
            dataQa='dataTypeSelect'
            name='selecttype'
            value={value}
            items={options}
            onChange={this.onConvertTypeChange}
            className={selectLeftAligned}
            itemClass={selectItemCls}
            itemRenderer={({ option, label }) => (<span className={classNames([typeElement, 'font-icon'])}>
              <span className={classNames(['icon-type', typeToIconType[option]])}></span>
              {label}
            </span>)}
          />
        </div>
      </div>
    );
  }

  renderCancelButton() {
    const { toType } = this.context.location.state || {};
    if (toType) {
      return null;
    }
    return (<SimpleButton
      buttonStyle='secondary'
      onClick={this.props.cancel}>
      {la('Cancel')}
    </SimpleButton>);
  }

  render() {
    const { toType } = this.context.location.state;
    const fromType = this.props.columnType;
    const dateType = [DATE, TIME, DATETIME].indexOf(toType) !== -1 ? toType : '';
    const noParamType = toType && NoParam[toType].indexOf(fromType) !== -1 ? toType : '';

    const formProps = {
      submit: this.onSubmit,
      toType,
      columnName: this.props.columnName,
      onCancel: this.props.cancel,
      initialValues: this.getTransformationValuesFromLocalStorage(toType)
    };

    return <div>
      {this.renderSelect()}
      {this.renderCancelButton()}
      <Tabs activeTab={fromType}>
        <Tabs tabId={TEXT} activeTab={toType}>
          <NonMatchingForm tabId={INTEGER} formKey='ConvertToInt' {...formProps} />
          <NonMatchingForm tabId={FLOAT} formKey='ConvertToFloat' {...formProps} />
          <ConvertTextToDateForm tabId={dateType} {...formProps} fromType={fromType} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={BINARY} activeTab={toType}>
          <NonMatchingForm tabId={INTEGER} formKey='ConvertToInt' {...formProps} />
          <NonMatchingForm tabId={FLOAT} formKey='ConvertToFloat' {...formProps} />
          <ConvertTextToDateForm tabId={dateType} {...formProps} fromType={fromType} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={INTEGER} activeTab={toType}>
          <NumberToDateForm tabId={dateType} {...formProps} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={FLOAT} activeTab={toType}>
          <ConvertFloatToIntForm tabId={INTEGER} {...formProps} />
          <NumberToDateForm tabId={dateType} {...formProps} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={DECIMAL} activeTab={toType}>
          <ConvertFloatToIntForm tabId={INTEGER} {...formProps} />
          <NumberToDateForm tabId={dateType} {...formProps} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={DATE} activeTab={toType}>
          <ConvertDateToTextForm tabId={TEXT} {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={INTEGER} formKey='ConvertToInt' {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={FLOAT} formKey='ConvertToFloat' {...formProps} fromType={fromType} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={TIME} activeTab={toType}>
          <ConvertDateToTextForm tabId={TEXT} {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={INTEGER} formKey='ConvertToInt' {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={FLOAT} formKey='ConvertToFloat' {...formProps} fromType={fromType} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={DATETIME} activeTab={toType}>
          <ConvertDateToTextForm tabId={TEXT} {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={INTEGER} formKey='ConvertToInt' {...formProps} fromType={fromType} />
          <DateToNumberForm tabId={FLOAT} formKey='ConvertToFloat' {...formProps} fromType={fromType} />
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={LIST} activeTab={toType}>
          <ConvertListToTextForm tabId={TEXT} {...formProps} />
        </Tabs>
        <Tabs tabId={MAP} activeTab={toType}>
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
        <Tabs tabId={BOOLEAN} activeTab={toType}>
          <NoParamForm tabId={noParamType} {...formProps} />
        </Tabs>
      </Tabs>
    </div>;
  }
}

const styles = {
  convertToLabel: {
    ...formLabel,
    marginRight: 10
  }
};
