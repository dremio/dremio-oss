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
import PropTypes from 'prop-types';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import Immutable from 'immutable';

import { getExploreState } from '@app/selectors/explore';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';

import Tabs from 'components/Tabs';
import { getDefaultValue, parseTextToDataType } from 'constants/DataTypes';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ReplaceFooter from './../ReplaceFooter';
import Exact from './../ContentWithoutCards/Exact';

const SECTIONS = [NewFieldSection, ReplaceFooter, Exact];

export class ReplaceExactForm extends Component {

  static propTypes = {
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    fields: PropTypes.object,
    curSubtitle: PropTypes.string,
    columnName: PropTypes.string,
    loadTransformValuesPreview: PropTypes.func,
    submitForm: PropTypes.func,
    matchedCount: PropTypes.number,
    unmatchedCount: PropTypes.number,
    transform: PropTypes.instanceOf(Immutable.Map),
    dataset: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
  }

  componentWillReceiveProps(nextProps) {
    const newValues = nextProps.fields;
    const oldValues = this.props.fields;
    if (newValues.replaceValues[0].value !== oldValues.replaceValues[0].value ||
        newValues.replaceNull.value !== oldValues.replaceNull.value) {
      this.runLoadTransformValues(newValues);
    }
  }

  runLoadTransformValues(transformValues) {
    const value = [transformValues.replaceNull.value ? null : transformValues.replaceValues[0].value];
    this.props.loadTransformValuesPreview(value);
  }

  submit = (values, submitType) => {
    const transformType = this.props.transform.get('transformType');
    const columnType = this.props.transform.get('columnType');
    const submitValues = {...values};
    if (values.replaceNull) {
      submitValues.replaceValues = [null];
    }
    const data = transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, this.props.transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceExact(values, columnType)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, this.props.transform),
        filter: filterMappers.mapFilterExcludeValues(submitValues, columnType)
      };

    return this.props.submit(data, submitType);
  }

  render() {
    const { fields, submitForm, transform } = this.props;
    const columnType = transform.get('columnType');
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        >
        <Exact
          submitForm={submitForm}
          columnType={columnType}
          replaceValues={fields.replaceValues.length !== 0 && fields.replaceValues[0] || {}}
          replaceNull={fields.replaceNull}
          matchedCount={this.props.matchedCount}
          unmatchedCount={this.props.unmatchedCount}
          />
        <Tabs activeTab={transform.get('transformType')}>
          <ReplaceFooter
            tabId='replace'
            fields={fields}
            submitForm={submitForm}
            transform={transform}
            />
        </Tabs>
      </TransformForm>
    );
  }
}

export function getInitialReplaceValue(cellText, columnType, firstCardValue) {
  // - cellText === '' means no selection
  // - when cellText is null, the null value radio will be selected, but we still want to populate
  // the disabled replaceValue field with firstCardValue.
  // - cellText being undefined should not happen, but still guard against it.
  return (cellText === '' || cellText === null || cellText === undefined)
    ? firstCardValue
    : parseTextToDataType(cellText, columnType);
}

function mapStateToProps(state, props) {
  const { transform } = props;
  const columnName = transform.get('columnName');
  const columnType = transform.get('columnType');
  const selection = transform.get('selection');
  const cellText = selection.get('cellText');

  const cardValues = getExploreState(state).recommended.getIn(['transform', transform.get('transformType'), 'Exact', 'values']);
  const firstCardValue = cardValues && cardValues.getIn(['values', 0, 'value']);

  return {
    matchedCount: cardValues && cardValues.get('matchedCount'),
    unmatchedCount: cardValues && cardValues.get('unmatchedCount'),
    initialValues: {
      newFieldName: columnName,
      dropSourceField: true,
      replaceValues: [getDefaultValue(columnType, getInitialReplaceValue(cellText, columnType, firstCardValue))],
      replacementValue: getDefaultValue(columnType),
      replaceNull: cellText === null,
      replaceType: 'VALUE'
    }
  };
}

export default connectComplexForm({
  form: 'replaceExact'
}, SECTIONS, mapStateToProps, null)(ReplaceExactForm);
