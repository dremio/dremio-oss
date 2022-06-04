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
import PropTypes from 'prop-types';

import Art from '@app/components/Art';
import Spinner from '@app/components/Spinner';

export default class CopyButtonIcon extends PureComponent {
  static propTypes = {
    title: PropTypes.string,
    style: PropTypes.object,
    onClick: PropTypes.func,
    disabled: PropTypes.bool,
    showSpinner: PropTypes.bool,
    version: PropTypes.number
  };

  static defaultProps = {
    version: 1
  };

  render() {
    const { title, style, onClick, disabled, showSpinner, version } = this.props;
    const clickHandler = disabled ? undefined : onClick;
    const iconSrc = version === 2 ? 'copy.svg' : 'Clipboard.svg';
    return (
      <span style={{...styles.wrap, ...style}} >
        {showSpinner && <Spinner style={styles.spinner} iconStyle={styles.spinnerIcon}/>}
        <Art
          src={iconSrc}
          onClick={clickHandler}
          alt={title}
          title={title}
          className='copy-button'
          data-qa='copy-icon'
          style={{...styles.icon, ...(disabled && styles.disabled), ...(version === 2 && styles.version2)}} />
      </span>
    );
  }
}

const styles = {
  icon: {
    cursor: 'pointer',
    width: 14,
    height: 14
  },
  wrap: {
    display: 'inline-block',
    transform: 'translateY(4px)'
  },
  disabled: {
    opacity: 0.7,
    cursor: 'default',
    color: '#DDDDDD'
  },
  spinner: {
    top: -4,
    right: 18,
    left: 'inherit'
  },
  spinnerIcon: {
    width: 24,
    height: 24
  },
  version2: {
    height: 16,
    width: 18,
    filter: 'invert(50%) sepia(22%) saturate(275%) hue-rotate(174deg) brightness(89%) contrast(85%)'
  }

};
