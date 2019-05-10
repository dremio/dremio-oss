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
import { connect } from 'react-redux';
import { domUtils } from '@app/utils/domUtils';
import { getSidebarSize } from '@app/selectors/home';
import { MIN_SIDEBAR_WIDTH, setSidebarSize } from '@app/actions/home';
import { gridMinWidth } from '@app/pages/HomePage/components/Columns.less';
import { slider as sliderCls} from './BrowseTable.less';

const GRID_MIN_WIDTH = parseInt(gridMinWidth, 10);

const mapStateToProps = state => ({
  position: getSidebarSize(state)
});
const mapDispatchToProps = {
  resizeSidebar: setSidebarSize
};

@connect(mapStateToProps, mapDispatchToProps)
export class BrowseTableResizer extends Component {
  static propTypes = {
    position: PropTypes.number,
    resizeSidebar: PropTypes.func.isRequired, // (int sideBarSize) => void
    anchorElementGetter: PropTypes.func.isRequired // () => dom element
  };

  state = {
    sliderPoistion: null,
    isResizeInProgress: false
  };

  sliderNode = null;

  onSliderRef = slider => {
    const eventName = 'mousedown'; // pointerdown is not supported by safari
    if (slider) {
      slider.addEventListener(eventName, this.startResize);
    } else if (this.sliderNode) { // slider is unmounted => check that previous reference exists
      this.sliderNode.removeEventListener(eventName, this.startResize);
    }
    this.sliderNode = slider;
  };

  startResize = () => {
    domUtils.disableSelection();
    domUtils.setCursor('col-resize');
    domUtils.captureMouseEvents(this.onMouseMove, this.stopResize);
    this.setState({
      sliderPoistion: this.props.position,
      isResizeInProgress: true
    });
  };

  onMouseMove = e => {
    if (this.state.isResizeInProgress) {
      const { anchorElementGetter } = this.props;
      const anchorElement = anchorElementGetter();
      const {
        left,
        right
      } =  anchorElement ? anchorElement.getBoundingClientRect() : { left: 0, right: 0};

      let sliderPoistion;
      // we have following structure
      // |grid|resizer|sidebar
      if (e.clientX - left < GRID_MIN_WIDTH) {
        sliderPoistion = right - left - GRID_MIN_WIDTH;
      } else {
        sliderPoistion = Math.max(right - e.clientX, MIN_SIDEBAR_WIDTH);
      }
      this.setState({
        sliderPoistion
      });
    }
  };

  stopResize = () => {
    if (!this.state.isResizeInProgress) return;
    this.props.resizeSidebar(this.state.sliderPoistion);
    this.setState({
      sliderPoistion: null,
      isResizeInProgress: false
    });
    domUtils.enableSelection();
    domUtils.resetCursor();
  };

  render() {
    const { isResizeInProgress, sliderPoistion } = this.state;
    const sliderStyle = isResizeInProgress ? { opacity: 1, right: sliderPoistion} : null;

    return <div
      ref={this.onSliderRef}
      className={sliderCls}
      style={sliderStyle}
    />;
  }
}
