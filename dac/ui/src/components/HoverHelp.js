/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from 'react';
import classNames from 'classnames';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import { Tooltip } from 'components/Tooltip';

export default class HoverHelp extends PureComponent {

  static propTypes = {
    style: PropTypes.object,
    iconStyle: PropTypes.object,
    tooltipStyle: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    content: PropTypes.node,
    className: PropTypes.string,
    placement: PropTypes.string,
    wantedIconType: PropTypes.string,
    theme: PropTypes.object
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

  getIconType(hover, wantedIconType) {
    if (!wantedIconType) {
      return hover ? 'InfoCircleSolid' : 'InfoCircle';
    } else {
      return wantedIconType;
    }
  }

  render() {
    const { style, iconStyle, theme, wantedIconType, content, tooltipStyle, tooltipInnerStyle, className, placement } = this.props;
    const {hover} = this.state;
    const finalInnerStyle = {...styles.defaultInnerStyle, ...tooltipInnerStyle};
    const iconType = this.getIconType(hover, wantedIconType);

    return <div
      className={classNames(['hover-help', className])}
      style={{position:'relative', ...style}}>
      <FontIcon
        type={iconType}
        ref='target'
        iconStyle={{...styles.iconStyle, ...iconStyle}}
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
        theme={theme}
      />
      <Tooltip
        container={this}
        placement={placement || 'bottom-start'}
        target={() => hover ? this.refs.target : null}
        type='status'
        style={tooltipStyle}
        tooltipInnerStyle={finalInnerStyle}
      >
        {content}
      </Tooltip>
    </div>;
  }
}

const styles = {
  iconStyle: {
    width: 24,
    height: 24,
    marginLeft: 3,
    marginBottom: -3
  },
  defaultInnerStyle: {
    borderRadius: 5,
    padding: 10,
    width: 300
  }
};
