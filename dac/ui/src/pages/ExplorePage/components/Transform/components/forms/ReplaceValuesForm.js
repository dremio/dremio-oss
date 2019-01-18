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
import deepEqual from 'deep-equal';
import Immutable from 'immutable';
import uuid from 'uuid';
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import { getExploreState } from '@app/selectors/explore';
import { parseTextToDataType } from 'constants/DataTypes';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';
import NewFieldSection from 'components/Forms/NewFieldSection';
import ReplaceValues from '../ContentWithoutCards/ReplaceValues';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ReplaceFooter from './../ReplaceFooter';

const SECTIONS = [ReplaceValues, NewFieldSection, ReplaceFooter];

export class ReplaceValuesForm extends Component {

  static propTypes = {
    submit: PropTypes.func,
    transform: PropTypes.instanceOf(Immutable.Map),
    columnType: PropTypes.string,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    fields: PropTypes.object,
    valueOptions: PropTypes.object,
    loadTransformValuesPreview: PropTypes.func,
    submitForm: PropTypes.func,
    type: PropTypes.string,
    sqlSize: PropTypes.number,
    dataset: PropTypes.instanceOf(Immutable.Map),
    values: PropTypes.object,
    dirty: PropTypes.bool
  };

  onValuesChange = (newValues, oldValues) => {
    if (!deepEqual(newValues.replaceValues, oldValues.replaceValues)) {
      this.runLoadTransformValues(newValues.replaceValues);
    }
  }

  runLoadTransformValues(transformValues) {
    this.props.loadTransformValuesPreview(transformValues);
  }

  submit = (values, submitType) => {
    const { columnType } = this.props;
    const transformType = this.props.transform.get('transformType');
    let { replaceValues, replacementValue } = values;
    replacementValue = parseTextToDataType(replacementValue, columnType);

    let replaceNull = false;
    const replaceValuesSet = new Set(replaceValues);
    // note: some APIs don't express null correctly (instead they drop the field)
    if (replaceValuesSet.delete(null) || replaceValuesSet.delete(undefined)) {
      // and need to transmit as separate field
      replaceNull = true;
    }
    replaceValues = [...replaceValuesSet].map((value) => parseTextToDataType(value, columnType));

    if (!replaceValues.length && !replaceNull) {
      return Promise.reject({_error: {
        message: la('Please select at least one value.'),
        id: uuid.v4()
      }});
    }

    const mapValues = {
      ...values,
      replacementValue,
      replaceValues,
      replaceNull
    };

    const data = transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(mapValues, this.props.transform),
        fieldTransformation: {
          type: 'ReplaceValue',
          ...fieldsMappers.getReplaceValues(mapValues, columnType)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(mapValues, this.props.transform),
        filter: filterMappers.mapFilterExcludeValues(mapValues, this.props.columnType)
      };

    return this.props.submit(data, submitType);
  }

  render() {
    const { fields, transform, submitForm, valueOptions, sqlSize } = this.props;
    return (
      <TransformForm
        {...formWrapperProps({...this.props})}
        onFormSubmit={this.submit}
        onValuesChange={this.onValuesChange}
      >
        <ReplaceValues
          fields={fields}
          sqlSize={sqlSize}
          valueOptions={valueOptions}
        />
        {transform.get('transformType') === 'replace' &&
          <ReplaceFooter
            tabId='replace'
            fields={fields}
            submitForm={submitForm}
            transform={transform}
          />
        }
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const columnName = props.transform.get('columnName');
  const transformType = props.transform.get('transformType');
  const columnType = props.transform.get('columnType');
  const cellText = props.transform.getIn(['selection', 'cellText']);
  const explorePageState = getExploreState(state);
  const valueOptions = explorePageState.recommended.getIn(['transform', transformType, 'Values', 'values']);
  return {
    columnType,
    valueOptions,
    sqlSize: explorePageState.ui.get('sqlSize'),
    initialValues: {
      replacementValue: '',
      newFieldName: columnName,
      dropSourceField: true,
      replaceValues: cellText !== undefined ? [cellText] : [],
      replaceType: 'VALUE',
      replaceSelectionType: 'VALUE'
    }
  };
}

export default connectComplexForm({
  form: 'replaceValues',
  overwriteOnInitialValuesChange: false
}, SECTIONS, mapStateToProps, null)(ReplaceValuesForm);
