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

export const POSITION_LEFT = 'left';
export const POSITION_RIGHT = 'right';

export default class TransformRangeSlider extends Component {

  static propTypes = {
    blockStyle: PropTypes.object,
    offset: PropTypes.number,
    position: PropTypes.string,
    activeSlider: PropTypes.string,
    clientX: PropTypes.number,
    setActiveSlider: PropTypes.func,
    dataQa: PropTypes.string
  };

  isActive(props) {
    return props.activeSlider === props.position;
  }

  onRef = (el) => {
    if (el) {
      el.addEventListener('pointerdown', this.startSlide);
    }
    this.slider = el;
  };

  startSlide = (pointerEvent) => {
    this.props.setActiveSlider(this.props.position);
    this.slider.setPointerCapture(pointerEvent.pointerId);
  };

  render() {
    if (this.props.offset === null) {
      return null;
    }

    const { blockStyle, position, dataQa } = this.props;

    const style = {
      ...styles.block,
      ...blockStyle,
      cursor: this.isActive(this.props) ? 'col-resize' : 'default',
      [position === POSITION_RIGHT ? 'left' : 'width']: this.props.offset
    };
    const sliderStyle = {
      ...styles.slider,
      // move slidebar center to a middle of edge
      [position === POSITION_RIGHT ? 'left' : 'right']: -(styles.slider.width / 2)
    };

    const pointerStyle = {
      ...styles.pointer,
      [position === POSITION_RIGHT ? 'left' : 'right']: -3.5
    };

    return <div style={style}>
      <div style={pointerStyle}></div>
      <div style={sliderStyle} data-qa={dataQa} ref={this.onRef}></div>
    </div>;
  }
}

const styles = {
  pointer: {
    position: 'absolute',
    top: -10,
    width: 6,
    borderTop: '9px solid black',
    borderLeft: '3px solid transparent',
    borderRight: '3px solid transparent'
  },
  block: {
    left: 0,
    position: 'absolute',
    backgroundColor: 'rgba(0,0,0,0.10)',
    top: 0,
    right: 0,
    height: 79
  },
  slider: {
    float: 'right',
    display: 'inline-block',
    cursor: 'col-resize',
    position: 'absolute',
    height: '100%',
    width: 20,
    borderRadius: 10
  }
};
