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
import { Component, PropTypes } from 'react';
import pureRender from 'pure-render-decorator';

import getTooltipStyles from 'uiTheme/radium/tooltips';

@pureRender
export default class Tooltip extends Component {

  static propTypes = {
    content: PropTypes.node,
    type: PropTypes.string,
    placement: PropTypes.string,
    id: PropTypes.string,
    className: PropTypes.string,
    style: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    tooltipArrowStyle: PropTypes.object,
    // we can get arrowOffsetLeft like percent
    arrowOffsetLeft: PropTypes.oneOfType([
      PropTypes.number,
      PropTypes.string
    ]),
    arrowOffsetTop: PropTypes.oneOfType([
      PropTypes.number,
      PropTypes.string
    ])
  };

  constructor(props) {
    super(props);
    this.state = { content: this.props.content };
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.content) {
      this.setState({ content: nextProps.content });
    }
  }

  render() {
    const styles = getTooltipStyles(this.props.type, 'error', this.props.placement);
    const placementStyle = styles.placement[this.props.placement];

    const {
      className,
      id,
      style,
      tooltipInnerStyle,
      tooltipArrowStyle,
      arrowOffsetLeft: left = placementStyle.arrow.left,
      arrowOffsetTop: top = placementStyle.arrow.top
    } = this.props;

    const finalStyle = { ...styles.base, ...placementStyle.tooltip, ...style };

    return (
      <div id={id} className={className} data-qa='tooltip' style={finalStyle}>
        <div style={{ ...styles.arrow, ...placementStyle.arrow, ...tooltipArrowStyle, left, top }}/>
        <div style={{ ...styles.inner, ...tooltipInnerStyle }}>
          {this.state.content}
        </div>
      </div>
    );
  }
}
