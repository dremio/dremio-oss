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
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { getDefaultValue } from 'constants/DataTypes';
import Tabs from 'components/Tabs';
import { isDateType } from 'constants/DataTypes';
import exploreUtils from 'utils/explore/exploreUtils';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ReplaceFooter from './../ReplaceFooter';
import TransformRange from './../TransformRange';

const SECTIONS = [NewFieldSection, ReplaceFooter, TransformRange];

export class ReplaceRangeForm extends Component {

  static propTypes = {
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    fields: PropTypes.object,
    curSubtitle: PropTypes.string,
    cardValues: PropTypes.object,
    submitForm: PropTypes.func,
    transform: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);

    this.chartData = this.getDataForChart(props.cardValues);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.cardValues !== this.props.cardValues) {
      this.updateChartData(nextProps.cardValues);
    }
  }

  componentDidUpdate() {
    const newChartData = this.getDataForChart(this.props.cardValues);
    if (this.chartData && newChartData && (this.chartData.length !== newChartData.length)) {
      this.chartData = newChartData;
      this.forceUpdate();
    }
  }

  getDataForChart(cardValues) {
    const chartData = cardValues && cardValues.get('values').toJS()
      .filter(value => value.value !== undefined)
      .map(value => ({
        percent: value.percent,
        x: value.valueRange.lowerLimit,
        y: value.count,
        range: value.valueRange && {
          upperLimit: value.valueRange.upperLimit,
          lowerLimit: value.valueRange.lowerLimit
        }
      })).sort((b, a) => b.x - a.x);

    return chartData && this.combineBins(chartData);
  }

  getMaxBinsCountByWidth(chartWidth) {
    const MIN_BAR_WIDTH = 20;
    return chartWidth / MIN_BAR_WIDTH;
  }

  getChartWidth() {
    const { transform } = this.props;
    const offsetWidth = isDateType(transform.get('columnType')) ? 235 : 140;
    // fixed magic number for now. But we should get rid of them. TODO address the issue under DX-12895
    return exploreUtils.getDocumentWidth() - offsetWidth * 2 - 92;
  }

  updateChartData(cardValues) {
    this.chartData = this.getDataForChart(cardValues);
  }

  combineBins(chartData) {
    const chartWidth = this.getChartWidth();
    const count = this.getMaxBinsCountByWidth(chartWidth);

    if (chartData.length <= count) {
      return chartData;
    }

    const combinedBins = [];
    for (let i = 0; i < chartData.length; i += 2) {
      let merged;
      if (chartData[i + 1]) {
        merged = {
          percent: chartData[i].percent + chartData[i + 1].percent,
          x: chartData[i].range.lowerLimit,
          y: chartData[i].y + chartData[i + 1].y,
          range: {
            lowerLimit: chartData[i].range.lowerLimit,
            upperLimit: chartData[i + 1].range.upperLimit
          }
        };
      } else {
        merged = chartData[i];
      }

      combinedBins.push(merged);
    }

    return this.combineBins(combinedBins);
  }

  submit = (values, submitType) => {
    const { transform } = this.props;
    const transformType = transform.get('transformType');
    const data = transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, transform),
        fieldTransformation: {
          type: 'ReplaceRange',
          ...fieldsMappers.getReplaceRange(values, transform.get('columnType'))
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeRange(values, transform.get('columnType'))
      };

    return this.props.submit(data, submitType);
  }

  render() {
    const { fields, submitForm, transform } = this.props;
    const columnType = transform.get('columnType');
    const transformType = transform.get('transformType');
    const chartWidth = this.getChartWidth();

    return (
      <TransformForm {...formWrapperProps(this.props)} onFormSubmit={this.submit}>
        <TransformRange
          columnType={columnType}
          data={this.chartData}
          chartWidth={chartWidth}
          fields={fields}
          isReplace={transformType === 'replace'}
          />
        <Tabs activeTab={transformType}>
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

function mapStateToProps(state, props) {
  const columnName = props.transform.get('columnName');
  const transformType = props.transform.get('transformType');
  const columnType = props.transform.get('columnType');
  const cardValues = getExploreState(state).recommended.getIn(['transform', transformType || 'replace', 'Range', 'values']);
  const defaultValue = getDefaultValue(columnType);
  return {
    cardValues,
    initialValues: {
      newFieldName: columnName,
      dropSourceField: true,
      upperBound: '',
      lowerBound: '',
      lowerBoundInclusive: true,
      upperBoundInclusive: false,
      keepNull: false,
      replacementValue: defaultValue,
      replaceType: 'VALUE',
      replaceSelectionType: 'VALUE'
    }
  };
}

export default connectComplexForm({
  form: 'replaceRange'
}, SECTIONS, mapStateToProps, null)(ReplaceRangeForm);
