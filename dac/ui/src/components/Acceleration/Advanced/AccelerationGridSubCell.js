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
import FontIcon from 'components/Icon/FontIcon';

export default class AccelerationGridSubCell extends Component {
  static propTypes = {
    isChecked: PropTypes.bool,
    onClick: PropTypes.func,
    isLastCell: PropTypes.bool,
    subValue: PropTypes.string,
    subValueAltText: PropTypes.string,
    onValueClick: PropTypes.func
  };

  render() {
    const {isChecked, isLastCell, onClick, subValue, subValueAltText, onValueClick} = this.props;
    const iconType = isChecked ? 'OKSolid' : 'MinusSimple';
    const cellStyle = isLastCell ? styles.lastSubCell : styles.subCell;
    const altText = subValueAltText ? `${subValueAltText}` : null;
    // if the cell is not checked, only show minus icon
    // otherwise show check round icon
    //   and if subValue is given, show it
    //   and if onValueClick is defined, show caret and handle click on subvalue with caret
    return (
      <div style={cellStyle} className='subCell' onClick={onClick}>
        <FontIcon type={iconType} theme={styles.iconTheme} style={{flex: '1 1 auto'}}/>
        {isChecked && subValue &&
          <div title={altText} onClick={onValueClick} style={onValueClick ? styles.subValueClickable : styles.subValue}>
            {subValue}
            {onValueClick &&
            <FontIcon type='fa-caret-down' theme={styles.caretTheme}/>
            }
          </div>
        }
      </div>
    );
  }
}

const styles = {
  subCell: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: '100%',
    padding: '0 auto',
    borderRight: '1px solid #e1e1e1'
  },
  subValue: {
    flex: '1 1 auto',
    height: 24,
    paddingTop: 6
  },
  iconTheme: {
    Container: {
      cursor: 'pointer',
      height: 24
    }
  },
  caretTheme: {
    Container: {
      cursor: 'pointer'
    }
  }
};

styles.lastSubCell = {
  ...styles.subCell,
  borderRight: 0
};

styles.subValueClickable = {
  ...styles.subValue,
  borderLeft: '1px solid #e1e1e1'
};
