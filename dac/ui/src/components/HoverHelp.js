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
import ReactDOM from 'react-dom';
import { Overlay } from 'react-overlays';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import Tooltip from 'components/Tooltip';

@pureRender
export default class HoverHelp extends Component {

  static propTypes = {
    style: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    content: PropTypes.node
  };

  constructor(props) {
    super(props);
    this.state = {hover: false};
    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onMouseLeave = this.onMouseLeave.bind(this);
  }

  onMouseEnter() {
    this.setState({hover: true});
  }

  onMouseLeave() {
    this.setState({hover: false});
  }

  render() {
    const { style, content, tooltipInnerStyle } = this.props;
    const {hover} = this.state;

    return <div className='hover-help' style={{position:'relative', ...style}}>
      <FontIcon
        type={hover ? 'InfoCircleSolid' : 'InfoCircle'}
        ref='target'
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
      />
      <Overlay
        show={hover}
        container={this}
        placement='right'
        target={() => ReactDOM.findDOMNode(this.refs.target)}>
        <Tooltip type='info' placement='right' content={content} tooltipInnerStyle={tooltipInnerStyle}/>
      </Overlay>
    </div>;
  }
}
