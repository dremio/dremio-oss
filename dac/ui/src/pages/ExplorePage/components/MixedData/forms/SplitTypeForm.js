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

import Checkbox from 'components/Fields/Checkbox';
import FontIcon from 'components/Icon/FontIcon';
import Meter from 'components/Meter';
import { applyValidators, notEmptyArray } from 'utils/validation';

import { LINE_START_CENTER, INLINE_NOWRAP_ROW_FLEX_START } from 'uiTheme/radium/flexStyle';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import NewFieldSection from 'components/Forms/NewFieldSection';
import classNames from 'classnames';
import {
  title,
  columnsContainer,
  rowMargin,
  firstColumn,
  secondColumn
} from '@app/uiTheme/less/forms.less';
import TransformForm, { formWrapperProps } from './../../forms/TransformForm';
import { transformProps } from './../../forms/TransformationPropTypes';
import NonMatchingValues from './../NonMatchingValues';
import { description, newField } from './SplitTypeForm.less';

const SECTIONS = [NewFieldSection];

const validate = (values) => applyValidators(values, [notEmptyArray('selectedTypesList')]);

@Radium
export class SplitTypeForm extends Component {
  static propTypes = {
    ...transformProps,
    availableValuesCount: PropTypes.number,
    availableValues: PropTypes.array,
    dataTypes: PropTypes.array
  };

  static defaultProps = {
    dataTypes: []
  };

  toggleType = (value) => {
    const { selectedTypesList } = this.props.fields;
    const index = selectedTypesList.value.indexOf(value);
    if (index === -1) {
      selectedTypesList.onChange(selectedTypesList.value.concat(value));
    } else {
      selectedTypesList.onChange(selectedTypesList.value.slice(0, index)
        .concat((selectedTypesList.value.slice(index + 1))));
    }
  }

  renderTypes() {
    const { dataTypes } = this.props;
    const maxMatchingPercent = Math.max(...dataTypes.map((option) => option.matchingPercent));
    const { value } = this.props.fields.selectedTypesList;
    return (
      <div style={styles.typeList}>
        <div style={LINE_START_CENTER}>
          <div className={title}>{la('Available Data Types')}
            <span className={description}>{la('Types based on sample dataset')}</span>
          </div>

        </div>
        <table className={rowMargin}>
          {
            dataTypes.map((option) => <tr>
              <td>
                <Checkbox
                  style={styles.checkbox}
                  checked={Boolean(value.find(item => item === option.type))}
                  dataQa={`checkbox-${option.type}`}
                  onChange={this.toggleType.bind(this, option.type)}
                  label={[
                    <FontIcon
                      type={FontIcon.getIconTypeForDataType(option.type)}
                      theme={{ Container: { height: 24, width: 24 } }}
                    />,
                    <span style={{marginLeft: 5, marginRight: 5}}>{startCase(option.type.toLowerCase())}</span>
                  ]}
                />
              </td>
              <td style={styles.progressWrap}>
                <Meter value={option.matchingPercent} max={maxMatchingPercent}/>
              </td>
              <td style={styles.percent}>{`${option.matchingPercent.toPrecision(2)}%`}</td>
            </tr>)
          }
        </table>
      </div>
    );
  }

  render() {
    const { submit, fields, availableValuesCount, availableValues } = this.props;

    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={submit}
        submitting={this.props.submitting}>
        <div className={classNames(['clean-data-transform', columnsContainer])}>
          <div className={firstColumn}>
            {this.renderTypes()}
            <NewFieldSection fields={fields} className={newField}/>
          </div>
          <div className={secondColumn}>
            <NonMatchingValues nonMatchingCount={availableValuesCount} values={availableValues}/>
          </div>
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const { columnName } = props;
  return {
    availableValuesCount: props.split.availableValuesCount,
    availableValues: props.split.availableValues,
    dataTypes: props.split.dataTypes,
    initialValues: {
      typeMixed: 'splitByDataType',
      selectedTypesList: [],
      newFieldName: columnName,
      newColumnNamePrefix: `${columnName}_`,
      dropSourceField: true
    }
  };
}

export default connectComplexForm({
  overwriteOnInitialValuesChange: false,
  form: 'convertToSplitType',
  fields: ['typeMixed', 'selectedTypesList', 'newColumnNamePrefix'],
  validate
}, SECTIONS, mapStateToProps, null)(SplitTypeForm);

const styles = {
  base: {
    ...INLINE_NOWRAP_ROW_FLEX_START
  },
  typeList: {
    height: 199,
    overflowY: 'scroll'
  },
  checkbox: {
    marginTop: -8
  },
  progressWrap: {
    width: 300,
    paddingRight: 10
  },
  text: {
    paddingLeft: 10,
    paddingRight: 10
  },
  nonMatchingWrap: {
    marginLeft: 20
  }
};
