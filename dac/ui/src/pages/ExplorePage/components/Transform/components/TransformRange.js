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
import d3 from 'd3';
import moment from 'moment';

import Checkbox from 'components/Fields/Checkbox';
import { isDateType, dateTypeToFormat, convertToUnix } from 'constants/DataTypes';
import { isEmptyValue } from 'utils/validation';

import TransformRangeBound from './TransformRangeBound';
import TransformRangeGraph from './TransformRangeGraph';
import './TransformRange.less';

@Radium
export default class TransformRange extends Component {
  static getFields() {
    return ['upperBound', 'lowerBound', 'keepNull', 'lowerBoundInclusive', 'upperBoundInclusive'];
  }

  static propTypes = {
    fields: PropTypes.object,
    data: PropTypes.array,
    columnType: PropTypes.string,
    chartWidth: PropTypes.number,
    isReplace: PropTypes.bool
  };

  static getAnchorBound(bound, domain) {
    const diffs = domain.map(item => {
      return bound - item;
    });

    const min = diffs.reduce((prev, cur, index) => {
      return diffs[index] >= 0 && diffs[index] < diffs[prev] ? index : prev;
    }, 0);

    return domain[min];
  }

  static getChartOffsetForBound({ bound, width, data }) {
    if (bound === '' || bound === null || !data || data.length === 0) {
      return null;
    }

    const scale = TransformRange.getXScale(width, data.length);

    if (bound > data[data.length - 1]) {
      return width;
    }

    if (bound < data[0]) {
      return 0;
    }

    const anchorBound = TransformRange.getAnchorBound(bound, data);
    const index = data.reduce((findIndex, item, j) => {
      if (anchorBound === item) {
        return j;
      }
      return findIndex;
    }, -1);
    return scale(index);
  }

  static getXScale(width, length) {
    const range = [0, width];
    // copied from C3 for to exactly match positioning of sliders
    // https://github.com/c3js/c3/blob/master/src/scale.js
    const _scale = d3.scale.linear()
      .domain([0, length - 1])
      .range(range);
    const scale = (d, raw) => {
      const v = _scale(d);
      return raw ? v : Math.ceil(v);
    };
    for (const key in _scale) {
      scale[key] = _scale[key];
    }
    return scale;
  }

  static getDomainWithRange(data) {
    const domain = [data[0].range.lowerLimit];

    data.forEach((item) => {
      domain.push(item.range.upperLimit);
    });

    return domain;
  }

  constructor(props) {
    super(props);

    if (props.data) {
      this.domain = TransformRange.getDomainWithRange(props.data);
    }
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.data && this.props.data.length !== nextProps.data.length) {
      this.domain = TransformRange.getDomainWithRange(nextProps.data);
    }
  }

  getValue(index) {
    return this.transformValue(this.domain[index]);
  }

  transformValue(value) {
    const { columnType } = this.props;
    return isDateType(columnType) ? moment.utc(value).format(dateTypeToFormat[columnType]) : value;
  }

  updateValue(field, offset) {
    const scale = TransformRange.getXScale(this.props.chartWidth, this.domain.length);
    const maxDiff = (scale(1) - scale(0)) / 2;
    const newValue = this.domain.find((v, i) => {
      return (Math.abs(scale(i) - offset) <= maxDiff);
    });

    field.onChange(this.transformValue(newValue));
  }

  validateBound = (isRight) => (newValue) => {
    const { fields: { lowerBound, upperBound } } = this.props;
    const { value: lowerValue } = lowerBound;
    const { value: upperValue } = upperBound;

    if (isNaN(Number(newValue))) {
      return false;
    }

    if (isRight) {
      return isEmptyValue(lowerValue) || Number(lowerValue) < Number(newValue);
    }

    return isEmptyValue(upperValue) || Number(newValue) < Number(upperValue);
  }

  renderKeepNullCheckbox() {
    const { fields: { keepNull }, isReplace} = this.props;
    if (!isReplace) {
      return  (<div style={[styles.footer]}>
        <Checkbox
          {...keepNull}
          label={<span>{la('Keep null values')}</span>}
          labelStyle={styles.labelChecked }
          dummyInputStyle={styles.dummyStyle}
          style={styles.checkbox}
        />
      </div>);
    }
    return;
  }

  render() {
    const { columnType, fields: { lowerBound, upperBound }, data, chartWidth } = this.props;

    if (!data) {
      return null;
    }

    const boundOptions = {
      width: chartWidth,
      data: this.domain
    };
    const lowerValue = isDateType(columnType) ? convertToUnix(lowerBound.value, columnType) : lowerBound.value;
    const upperValue = isDateType(columnType) ? convertToUnix(upperBound.value, columnType) : upperBound.value;
    const leftRangeOffset =
      TransformRange.getChartOffsetForBound({
        ...boundOptions,
        bound: lowerValue
      });

    const rightRangeOffset =
      TransformRange.getChartOffsetForBound({
        ...boundOptions,
        bound: upperValue
      });
    const widthForColumns = isDateType(columnType)
      ? { minWidth: 235 }
      : {};

    return (
      <div className='transform-range' style={[styles.base]}>
        <div style={[styles.content]}>
          <div style={[styles.header]}>
            <div style={[styles.bound, styles.pad, widthForColumns]}>{la('Lower limit')}</div>
            <div style={[styles.distr, styles.pad]}>
              {la('Distribution of Values')}
            </div>
            <div style={[styles.bound, styles.pad, widthForColumns]}>{la('Upper limit')}</div>
          </div>
          <div style={[styles.body]} data-qa='graph-body'>
            <TransformRangeBound
              defaultValue={`${this.getValue(0)}`}
              columnType={columnType}
              noneLabel={la('None (-∞)')}
              fieldName='lower'
              field={lowerBound}
              validate={this.validateBound(false)}
              style={[styles.bound, styles.white, widthForColumns]}
            />
            <TransformRangeGraph
              style={styles.graph}
              leftRangeOffset={leftRangeOffset}
              rightRangeOffset={rightRangeOffset}
              width={chartWidth}
              lowerBound={lowerBound}
              upperBound={upperBound}
              columnType={columnType}
              data={data}
              updateValue={this.updateValue.bind(this)}
            />
            <TransformRangeBound
              defaultValue={`${this.getValue(this.domain.length - 1)}`}
              columnType={columnType}
              noneLabel={la('None (∞)')}
              fieldName='upper'
              field={upperBound}
              validate={this.validateBound(true)}
              style={[styles.bound, styles.white, widthForColumns]}
            />
          </div>
          {this.renderKeepNullCheckbox()}
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    width: 'calc(100% - 30px)',
    height: 170,
    margin: '0 10px'
  },
  pad: {
    paddingLeft: 10
  },
  dummyStyle: {
    marginTop: 3
  },
  labelChecked: {
    display: 'flex',
    alignItems: 'center'
  },
  checkbox: {
    marginTop: -1,
    marginLeft: 5
  },
  footer: {
    height: 30,
    borderLeft: '1px solid rgba(0,0,0,0.10)',
    borderRight: '1px solid rgba(0,0,0,0.10)',
    borderBottom: '1px solid rgba(0,0,0,0.10)',
    borderTop: '1px solid rgba(0,0,0,0.10)',
    marginBottom: 10,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
    backgroundColor: '#fff'
  },
  graph: {
    position: 'relative',
    width: 'calc(100% + 80px)',
    borderTop: '1px solid #5ED7B9',
    borderBottom: '1px solid #5ED7B9',
    borderLeft: '1px solid #5ED7B9',
    borderRight: '1px solid #5ED7B9'
  },
  body: {
    position: 'relative',
    display: 'flex',
    height: 110,
    justifyContent: 'space-between'
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    background: '#F3F3F3',
    height: 30,
    borderBottom: '1px solid rgba(0,0,0,0.10)'
  },
  distr: {
    width: '100%'
  },
  white: {
    height: 110,
    backgroundColor: '#fff',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'flex-start',
    alignItems: 'flex-start'
  },
  bound: {
    height: 30,
    display: 'flex',
    alignItems: 'center',
    width: 140,
    minWidth: 140,
    borderLeft: '1px solid rgba(0,0,0,0.10)',
    borderRight: '1px solid rgba(0,0,0,0.10)'
  }
};
