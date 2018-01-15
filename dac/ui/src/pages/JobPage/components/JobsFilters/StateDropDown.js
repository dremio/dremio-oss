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
import Immutable  from 'immutable';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import StateIconTypes from 'constants/jobPage/StateIconType.json';
import FontIcon from 'components/Icon/FontIcon';

@Radium
@PureRender
export default class StateDropDown extends Component {

  static propTypes = {
    options: PropTypes.instanceOf(Immutable.List).isRequired,
    onChange: PropTypes.func.isRequired,
    resetSelection: PropTypes.func.isRequired,
    selectedOptions: PropTypes.instanceOf(Immutable.List).isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { selectedOptions, onChange, resetSelection, options } = this.props;
    const staticOptions = options.map((item, index) => {
      const checked = selectedOptions.indexOf(item.get('value')) !== -1;
      return (
        <div key={`state${index}`} className='drop-down-item' style={[style.dropDownItem]}>
          <input
            type='checkbox'
            className='drop-down-checkbox'
            style={[style.checkbox]}
            onChange={onChange}
            value={item.get('value')}
            checked={checked} />
          <FontIcon type={StateIconTypes[item.get('value')]} theme={style.IconTheme}/>
          <span>{item.get('label')}</span>
        </div>);
    });
    const allLabel = selectedOptions.size !== 0
      ? <div onClick={resetSelection} className='all-label' style={[style.allLabel]}>{la('All')}</div>
      : null;
    return (
      <div className='state-dropdown' style={style.base}>
        {allLabel}
        {staticOptions}
      </div>);
  }
}

const style = {
  base: {
    padding: 4,
    boxShadow: '0 0 5px #999',
    borderRadius: 2,
    backgroundColor: '#fff',
    width: 200,
    zIndex: 99
  },
  allLabel: {
    margin: '5px 0 0 5px',
    cursor: 'pointer'
  },
  checkbox: {
    cursor: 'pointer',
    margin: 0
  },
  IconTheme : {
    Container: {
      display: 'inline-block',
      margin: '0 3px',
      height : 24
    }
  },
  dropDownItem: {
    display: 'flex',
    alignItems: 'center'
  }
};
