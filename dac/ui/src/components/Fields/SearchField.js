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
import Radium from 'radium';
import FontIcon from 'components/Icon/FontIcon';
import Keys from 'constants/Keys.json';

@Radium
export default class SearchField extends Component {
  static propTypes = {
    placeholder: PropTypes.string,
    onChange: PropTypes.func,
    value: PropTypes.string,
    style: PropTypes.object,
    inputStyle: PropTypes.object,
    searchIconTheme: PropTypes.object,
    showCloseIcon: PropTypes.bool,
    inputClassName: PropTypes.string,
    dataQa: PropTypes.string
  };

  constructor(props) {
    super(props);

    this.state = {
      value: props.value
    };
  }

  onChange = (value) => {
    this.setState({
      value
    });
    this.props.onChange(value);
  }

  handleKeyDown = (evt) => {
    if (evt.keyCode === Keys.ESCAPE) {
      this.clearFilter();
    }
  }

  clearFilter = () => {
    this.onChange('');
    this.focus();
  }

  focus() {
    this.refs.input && this.refs.input.focus();
  }

  render() {
    const showCloseIcon = this.props.showCloseIcon && this.state.value;
    return (
      <div style={[styles.base, this.props.style]}>
        <FontIcon
          type='Search'
          theme={this.props.searchIconTheme || styles.searchIcon}
        />
        <input
          data-qa={this.props.dataQa}
          className={'form-placeholder ' + (this.props.inputClassName || '')}
          type='text'
          ref='input'
          placeholder={this.props.placeholder}
          style={[styles.searchInput, this.props.inputStyle]}
          value={this.state.value}
          onChange={(e) => this.onChange(e.target.value)}
          onKeyDown={this.handleKeyDown}
        />
        {showCloseIcon && <FontIcon
          type='XBig'
          theme={styles.clearIcon}
          onClick={this.clearFilter}
        />}
      </div>
    );
  }
}

const styles = {
  base: {
    clear: 'both',
    display: 'flex',
    position: 'relative',
    width: '100%',
    paddingTop: 3,
    paddingRight: 3,
    paddingBottom: 3,
    paddingLeft: 3
  },
  searchInput: {
    display: 'block',
    border: '1px solid rgba(0,0,0, .1)',
    width: '100%',
    padding: '5px 10px 5px 25px'
  },
  searchIcon: {
    Icon: {
      width: 24,
      height: 24
    },
    Container: {
      position: 'absolute',
      margin: 'auto',
      width: 24,
      height: 24
    }
  },
  clearIcon: {
    Icon: {
      width: 22,
      height: 22
    },
    Container: {
      cursor: 'pointer',
      position: 'absolute',
      right: 3,
      top: 0,
      bottom: 0,
      margin: 'auto',
      width: 22,
      height: 22
    }
  }
};
