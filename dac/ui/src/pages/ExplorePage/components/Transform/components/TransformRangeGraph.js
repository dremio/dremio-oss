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
import { domUtils } from '@app/utils/domUtils';
import BarChart from 'components/Charts/BarChart';

import TransformRangeSlider, { POSITION_LEFT, POSITION_RIGHT } from './TransformRangeSlider';

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

  container = null; // will store root container element

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
    domUtils.disableSelection();
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
      const offset = this.container ? this.container.getBoundingClientRect().x : 0;
      this.setState({
        [`${activeSlider}RangeOffset`]: Math.min(Math.max(event.clientX - offset, 0), this.props.width)
      });
    }
  }

  getActiveSliderPosition() {
    const { activeSlider } = this.state;
    if (activeSlider) {
      return this.state[`${activeSlider}RangeOffset`];
    }
    return null;
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
      domUtils.enableSelection();
    }
  }

  onRef = (el) => {
    this.container = el;
  };

  render() {
    const {
      style,
      data,
      columnType,
      width
    } = this.props;

    const { activeSlider, leftRangeOffset, rightRangeOffset } = this.state;
    return (
      <div onMouseMove={this.moveSlider} onMouseUp={this.stopSlide} style={[style]}
        ref={this.onRef}>
        <BarChart
          type={columnType}
          data={data}
          width={width}
          sliderX={this.getActiveSliderPosition()}
        />
        <TransformRangeSlider
          blockStyle={{
            borderRight: '1px solid #505050',
            width: leftRangeOffset
          }}
          position={POSITION_LEFT}
          offset={leftRangeOffset}
          activeSlider={activeSlider}
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
          activeSlider={activeSlider}
          setActiveSlider={this.setActiveSlider}
          dataQa='TransformRangeRightSlider'
        />
      </div>
    );
  }
}
