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
import pureRender from 'pure-render-decorator';
import classNames from 'classnames';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import { Tooltip } from 'components/Tooltip';

@pureRender
export default class HoverHelp extends Component {

  static propTypes = {
    style: PropTypes.object,
    iconStyle: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    content: PropTypes.node,
    className: PropTypes.string
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
    const { style, iconStyle, content, tooltipInnerStyle, className } = this.props;
    const {hover} = this.state;
    const finalInnerStyle = {...styles.defaultInnerStyle, ...tooltipInnerStyle};

    return <div
      className={classNames(['hover-help', className])}
      style={{position:'relative', ...style}}>
      <FontIcon
        type={hover ? 'InfoCircleSolid' : 'InfoCircle'}
        ref='target'
        iconStyle={{...styles.iconStyle, ...iconStyle}}
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
      />
      <Tooltip
        container={this}
        placement='right-start'
        target={() => hover ? this.refs.target : null}
        type='status'
        tooltipInnerStyle={finalInnerStyle}
      >
        {content}
      </Tooltip>
    </div>;
  }
}

const styles = {
  iconStyle: {
    width: 19,
    height: 19,
    marginLeft: 3,
    marginBottom: -3
  },
  defaultInnerStyle: {
    borderRadius: 5,
    padding: 10,
    width: 300
  }
};
