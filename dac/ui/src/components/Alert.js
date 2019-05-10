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
import { PureComponent, createRef } from 'react';
import PropTypes from 'prop-types';
import Art from '@app/components/Art';
import { Tooltip } from 'components/Tooltip';
import { container, tooltip } from './Alert.less';

export class Alert extends PureComponent {
  static propTypes = {
    text: PropTypes.string,
    height: PropTypes.number
  };

  state = {
    isHover: false
  };

  onMouseEnter = () => {
    this.setState({
      isHover: true
    });
  };

  onMouseLeave = () => {
    this.setState({
      isHover: false
    });
  };

  iconRef = createRef()

  render() {
    const { text, height } = this.props;
    const { isHover } = this.state;

    return (<span className={container} onMouseEnter={this.onMouseEnter} onMouseLeave={this.onMouseLeave}>
      <Art key='icon' ref={this.iconRef} src={isHover ? 'WarningSolid.svg' : 'Warning.svg'} alt={text} style={{ height: height || 18}} />
      <Tooltip key='tooltip'
        target={() => isHover ? this.iconRef.current : null}
        type='info'
        placement='right'
        className={tooltip}
        tooltipInnerStyle={{ width: 'auto', whiteSpace: 'nowrap' }}
      >
        {text}
      </Tooltip>
    </span>);
  }
}
