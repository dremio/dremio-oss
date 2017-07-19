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
import classNames from 'classnames';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import FontIcon from 'components/Icon/FontIcon';

@PureRender
@Radium
export default class ResourcePin extends Component {
  static propTypes = {
    name: PropTypes.string.isRequired,
    isActivePin: PropTypes.bool.isRequired,
    toggleActivePin: PropTypes.func.isRequired
  };

  constructor(props) {
    super(props);
    this.onPinClick = this.onPinClick.bind(this);
  }

  onPinClick(e) {
    e.preventDefault();
    e.stopPropagation();
    const {name, isActivePin, toggleActivePin} = this.props;
    toggleActivePin.apply(this, [name, isActivePin]);
  }

  render() {
    const { isActivePin } = this.props;
    const pinClass = classNames('pin', {'active': isActivePin});
    return (
      <span
        className='pin-wrap'
        onClick={this.onPinClick}>
        <span
          className={pinClass}
          style={[styles.pin, isActivePin ? styles.activePin : null]}>
          <FontIcon
            type='Pin'
            theme={styles.iconStyle}/>
        </span>
      </span>
    );
  }
}

const styles = {
  pin: {
    opacity: 0.2,
    ':hover': {
      opacity: 0.6,
      cursor: 'pointer'
    }
  },
  activePin: {
    opacity: 1
  },
  iconStyle: {
    'Container': {
      'display': 'inline-block',
      'verticalAlign': 'middle'
    }
  }
};
