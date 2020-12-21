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
import { PureComponent, createRef } from 'react';
import PropTypes from 'prop-types';
import { Tooltip } from 'components/Tooltip';
import { container, tooltip, textWithHelp } from './TextWithHelp.less';

export class TextWithHelp extends PureComponent {
  static propTypes = {
    text: PropTypes.string
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

  textRef = createRef()

  render() {
    const { text } = this.props;
    const { isHover } = this.state;

    return (
      <span className={container} onMouseEnter={this.onMouseEnter} onMouseLeave={this.onMouseLeave}>
        <div className={textWithHelp} ref={this.textRef}>{text}</div>
        <Tooltip key='tooltip'
          target={() => isHover ? this.textRef.current : null}
          placement='bottom-start'
          className={tooltip}
          tooltipInnerStyle={{ width: 'auto', whiteSpace: 'nowrap' }}
        >
          {text}
        </Tooltip>
      </span>
    );
  }
}
