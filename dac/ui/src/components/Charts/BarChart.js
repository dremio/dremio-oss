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
import $ from 'jquery';

import { isDateType, dateTypeToFormat, TIME, FLOAT, DECIMAL, DATE, DATETIME } from 'constants/DataTypes';
import ChartTooltip from './ChartTooltip';

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
    // This coordinate is relative to chart's left edge
    sliderX: PropTypes.number // x coordinate of a slider if drag in a progress.
  };

  constructor(props) {
    super(props);

    this.state = {
      hoverBarTooltipInfo: null //  see this.calculateTooltipInfo for format
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

  getBar = (mouseXRelativeToChart) => {
    if (!this.chart || !mouseXRelativeToChart) return null;
    const bars = $('.c3-event-rect', this.refs.chart);
    let bar = null;
    const mouseX = mouseXRelativeToChart + this.refs.chart.getBoundingClientRect().left;

    bars.each((index, el) => {
      const rect = el.getBoundingClientRect();
      if (rect.left <= mouseX && mouseX <= rect.right) {
        bar = el;
        return false;
      }
    });

    return bar;
  }

  onMouseEnter = (e) => {
    this.setState({
      hoverBarTooltipInfo: this.calculateTooltipInfo(e.target, false)
    });
  }

  calculateTooltipInfo = (bar, isSliderTop) => {
    if (!this.refs.chart || !bar) return null;

    const index = $(bar).index();
    const barShape = $(`path.c3-shape-${index}`, this.refs.chart)[0]; // needed to calculate actual height of the bar.
    const barRect = bar.getBoundingClientRect();
    const chartRect = this.refs.chart.getBoundingClientRect();
    const top = isSliderTop ?  0 : barShape.getBoundingClientRect().top - chartRect.top;

    const rectWidth = barRect.width;
    const left = barRect.left - chartRect.left + rectWidth / 2;
    return {
      position: { top, left },
      index,
      // for case of slider we should use slide coordinates and anchor bar element is not needed here
      anchorEl: isSliderTop ? null : barShape
    };
  }

  onMouseLeave = () => {
    this.setState({
      hoverBarTooltipInfo: null
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
    $('.c3-axis .tick', this.refs.chart).each((i, item) => {
      const line = $(item).find('line');
      const text = $(item).find('text');
      const x = Number(line.attr('x1'));
      text.css('transform', `translateX(${x + 2}px) translateY(-2px)`);
    });
  }

  renderTooltipContent(index) {
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
    const { data, sliderX } = this.props;
    const sliderPositionBar = this.getBar(sliderX);
    const tooltipInfo = this.calculateTooltipInfo(sliderPositionBar, true) || this.state.hoverBarTooltipInfo;

    let pos = null;
    let showTooltip = false;
    if (tooltipInfo) {
      showTooltip = data[tooltipInfo.index].y > 0; // number of matched records more than 0
      pos = {
        ...tooltipInfo.position
      };
      if (sliderPositionBar) {
        pos.left = sliderX;
      }
    }

    return (
      <div>
        <div ref='chart' />
        {showTooltip ? <ChartTooltip position={pos} anchorEl={tooltipInfo.anchorEl}
          content={this.renderTooltipContent(tooltipInfo.index)} /> : null}
      </div>
    );
  }
}
