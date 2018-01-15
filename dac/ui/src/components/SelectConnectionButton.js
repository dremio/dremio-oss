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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import Art from 'components/Art';
import { DIVIDER } from 'uiTheme/radium/colors';

@Radium
export default class SelectConnectionButton extends Component {
  static propTypes = {
    label: PropTypes.string.isRequired,
    iconType: PropTypes.string.isRequired,
    pillText: PropTypes.string,
    disabled: PropTypes.bool,
    onClick: PropTypes.func
  }

  static defaultProps = {
    disabled: false
  }

  render() {
    const { label, iconType, pillText, disabled, onClick } = this.props;
    const src = iconType === 'sources/NETEZZA' ? `${iconType}.png` : `${iconType}.svg`;

    return <button
      disabled={disabled}
      style={[styles.base, disabled ? styles.disabled : styles.clickable]}
      onClick={!disabled ? onClick : undefined}
      data-qa={iconType}
      key={iconType}
    >
      <Art src={src} alt='' style={styles.iconStyle} />
      <h3 style={styles.label}>
        {label}
      </h3>
      {pillText && <div className='whiteText' style={{...styles.comingBetaLabelWrapper }}>{pillText}</div>}
    </button>;
  }
}

const styles = {
  base: {
    position: 'relative',
    display: 'flex',
    margin: '10px 10px 10px 0',
    border: `1px solid ${DIVIDER}`,
    borderRadius: 2,
    width: 230,
    backgroundColor: 'white',
    alignContent: 'center',
    padding: '4px 10px'
  },
  disabled: {
    opacity: 0.5
  },
  label: {
    fontSize: 14,
    height: 60,
    display: 'flex',
    flex: 1,
    alignItems: 'center',
    marginLeft: 10,
    textAlign: 'left'
  },
  clickable: {
    ':hover': {
      border: '1px solid #C0E9F5',
      boxShadow: '0 0 5px 0 rgba(0, 0, 0, 0.10)'
    }
  },
  comingBetaLabelWrapper: {
    fontSize: 10,
    fontWeight: 500,
    height: 16,
    position: 'absolute',
    left: '50%',
    bottom: 0,
    transform: 'translate(-50%, 50%)',
    padding: '3px 5px',
    borderRadius: 8,
    background: '#999999',
    textTransform: 'uppercase'
  },
  iconStyle: {
    width: 60,
    height: 60
  }
};
