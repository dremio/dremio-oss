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
import deepEqual from 'deep-equal';
import Radium from 'radium';
import PropTypes from 'prop-types';
import moment from 'moment';
import c3 from 'c3';
import 'c3/c3.css';

import { isDateType, dateTypeToFormat, TIME, FLOAT, DECIMAL, DATE, DATETIME } from 'constants/DataTypes';
import ChartTooltip, { ARROW_OFFSET } from './ChartTooltip';

const BAR_CHART_HEIGHT = 108;
const MAX_TICK_COUNT = 8;
const tickFormat = {
  [TIME]: 'HH:mm',
  [DATE]: 'MMM D, YYYY',
  [DATETIME]: 'MMM D, YYYY HH:mm'
};

@Radium
export default class BarChart extends Component {
  static propTypes = {
    data: PropTypes.array,
    width: PropTypes.number,
    type: PropTypes.string,
    blockTooltips: PropTypes.bool
  };

  constructor(props) {
    super(props);

    this.state = {
      openTooltip: false,
      position: {}
    };
  }

  componentDidMount() {
    this.generateChart();
  }

  componentDidUpdate(prevProps) {
    if (this.props.width !== prevProps.width) {
      this.chart.resize({ width: this.props.width });
    }

    if (!deepEqual(this.props.data, prevProps.data)) {
      this.generateChart();
    }
  }

  componentWillUnmount() {
    if (this.chart) {
      this.chart.destroy();
    }
  }

  onMouseEnter = (e) => {
    const el = e.target;
    const index = $(el).index();
    const bar = $(`path.c3-shape-${index}`)[0];
    const barRect = bar.getBoundingClientRect();
    const top = barRect.top - this.refs.chart.getBoundingClientRect().top;

    const rectWidth = Number(el.getAttribute('width'));
    const left = Number(el.getAttribute('x')) - ARROW_OFFSET + rectWidth / 2;

    this.setState({
      openTooltip: true,
      position: { top, left },
      index
    });
  }

  onMouseLeave = () => {
    this.setState({
      openTooltip: false
    });
  }

  getTickValues(length) {
    const tickCount = MAX_TICK_COUNT > length ? length - 1 : MAX_TICK_COUNT;
    return Array.from(Array(tickCount).keys()).map((i) => i * Math.round(length / tickCount));
  }

  attachHandlers() {
    const rect = '.c3-event-rect';
    $(this.refs.chart)
      .on('mouseleave', rect, this.onMouseLeave)
      .on('mouseenter', rect, this.onMouseEnter);
  }

  formatNumber(value) {
    const { type } = this.props;
    return type === FLOAT || type === DECIMAL ? value.toFixed(5) : value;
  }

  formatDate(value, isTooltip) {
    const { type } = this.props;
    return moment.utc(value).format(isTooltip ? dateTypeToFormat[type] : tickFormat[type]);
  }

  formatValue = (value, isTooltip) => {
    const { type } = this.props;
    return isDateType(type)
      ? this.formatDate(value, isTooltip)
      : this.formatNumber(value);
  }

  formatTick = (index) => {
    const { data } = this.props;
    const item = data[index];
    return item && this.formatValue(item.range.upperLimit);
  }

  scaleData(data) {
    return data.map(item => item !== 0 ? Math.log(item) / Math.LN10 + 1 : item);
  }

  generateChart() {
    const { width, data = [] } = this.props;
    const xData = data.map((item) => item.x);
    const yData = this.scaleData(data.map((item) => item.y));
    const maxY = Math.max(...yData);

    this.chart = c3.generate({
      onrendered: this.adjustTicks,
      onresized: this.adjustTicks,
      tooltip: {
        //disable because can't use react with built-in tooltip
        show: false
      },
      size: {
        width,
        height: BAR_CHART_HEIGHT
      },
      bindto: this.refs.chart,
      data: {
        x: 'x',
        colors: {
          y: '#5ED7B9'
        },
        columns: [
          ['y'].concat(yData),
          ['x'].concat(xData)
        ],
        type: 'bar'
      },
      bar: {
        zerobased: false,
        width: {
          ratio: 0.8
        }
      },
      axis: {
        y: {
          show: false,
          min: 0,
          max: maxY,
          padding: {
            top: 0,
            bottom: 0
          }
        },
        x: {
          type: 'categories',
          tick: {
            outer: false,
            multiline: true,
            width: 80,
            format: this.formatTick,
            values: this.getTickValues(data.length)
          }
        }
      },
      legend: {
        show: false
      }
    });

    this.attachHandlers();
  }

  adjustTicks = () => {
    $('.c3-axis .tick').each((i, item) => {
      const line = $(item).find('line');
      const text = $(item).find('text');
      const x = Number(line.attr('x1'));
      text.css('transform', `translateX(${x + 2}px) translateY(-2px)`);
    });
  }

  showToolTip() {
    const { blockTooltips } = this.props;
    const { openTooltip } = this.state;
    const { index } = this.state;
    const { data } = this.props;

    return !blockTooltips && openTooltip && index >= 0 &&
      data[index].y > 0; // number of metched records more than 0
  }

  renderTooltipContent() {
    const { index } = this.state;
    const { data } = this.props;
    const item = data[index];
    const { range } = item;

    return (
      <p>
        <span>
          {`${la('Range')}:
            ${this.formatValue(range.lowerLimit, true)} - ${this.formatValue(range.upperLimit, true)}`}
        </span>
        <br/>({item.y} {la('records')})
      </p>
    );
  }

  render() {
    const { position } = this.state;
    return (
      <div>
        <div ref='chart' />
        {this.showToolTip() ? <ChartTooltip position={position} content={this.renderTooltipContent()} /> : null}
      </div>
    );
  }
}
