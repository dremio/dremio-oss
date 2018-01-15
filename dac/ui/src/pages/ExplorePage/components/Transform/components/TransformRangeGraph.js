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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import BarChart from 'components/Charts/BarChart';
import { isDateType } from 'constants/DataTypes';

import TransformRangeSlider, { POSITION_LEFT, POSITION_RIGHT } from './TransformRangeSlider';

const OFFSET_RIGHT = 152; //values based on numbers from getBarChartWidth from TransformRange
const OFFSET_LEFT = 151;

@Radium
export default class TransformRangeGraph extends Component {
  static propTypes = {
    leftRangeOffset: PropTypes.number,
    rightRangeOffset: PropTypes.number,
    columnType: PropTypes.string,
    lowerBound: PropTypes.object,
    upperBound: PropTypes.object,
    data: PropTypes.array,
    updateValue: PropTypes.func,
    width: PropTypes.number,
    style: PropTypes.object
  }
  constructor(props) {
    super(props);

    this.state = {
      activeSlider: null,
      leftRangeOffset: 0,
      rightRangeOffset: props.width
    };

    this.moveSlider = this.moveSlider.bind(this);
    this.stopSlide = this.stopSlide.bind(this);
    this.setActiveSlider = this.setActiveSlider.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    this.resetRangeOffsets(nextProps);
  }

  componentDidUpdate(prevProps) {
    if (prevProps.width !== this.props.width) {
      this.resetRangeOffsets(this.props);
    }
  }

  setActiveSlider(slider) {
    this.setState({
      activeSlider: slider
    });
  }

  resetRangeOffsets(props) {
    // for left should be plus two
    this.setState({
      leftRangeOffset: props.leftRangeOffset + 2 || 0,
      rightRangeOffset: props.rightRangeOffset || props.width
    });
  }

  moveSlider(event) {
    const { activeSlider } = this.state;
    if (activeSlider) {
      const offset = this.calculateOffset(activeSlider);
      this.setState({
        [`${activeSlider}RangeOffset`]: event.clientX - offset
      });
    }
  }

  calculateOffset(position) {
    const defaultOffset = position === POSITION_RIGHT ? OFFSET_RIGHT : OFFSET_LEFT;
    const dateOffset = isDateType(this.props.columnType) ? 93 : -2;
    return defaultOffset + dateOffset;
  }

  stopSlide() {
    const { activeSlider } = this.state;
    if (activeSlider) {
      const isRight = activeSlider === POSITION_RIGHT;
      const field = isRight ? this.props.upperBound : this.props.lowerBound;

      if (this.state.leftRangeOffset < this.state.rightRangeOffset) {
        this.props.updateValue(field, this.state[`${activeSlider}RangeOffset`]);
      } else {
        this.resetRangeOffsets(this.props);
      }

      this.setActiveSlider(null);
    }
  }

  render() {
    const {
      style,
      data,
      columnType,
      width
    } = this.props;

    const { activeSlider, leftRangeOffset, rightRangeOffset } = this.state;
    return (
      <div onMouseMove={this.moveSlider} onMouseLeave={this.stopSlide} style={[style]}>
        <BarChart
          type={columnType}
          data={data}
          width={width}
        />
        <TransformRangeSlider
          blockStyle={{
            borderRight: '1px solid #505050',
            width: leftRangeOffset
          }}
          position={POSITION_LEFT}
          offset={leftRangeOffset}
          activeSlider={activeSlider}
          stopSlide={this.stopSlide}
          setActiveSlider={this.setActiveSlider}
          dataQa='TransformRangeLeftSlider'
        />
        <TransformRangeSlider
          blockStyle={{
            borderLeft: '1px solid #505050',
            left: rightRangeOffset
          }}
          position={POSITION_RIGHT}
          offset={rightRangeOffset}
          stopSlide={this.stopSlide}
          activeSlider={activeSlider}
          setActiveSlider={this.setActiveSlider}
          dataQa='TransformRangeRightSlider'
        />
      </div>
    );
  }
}
