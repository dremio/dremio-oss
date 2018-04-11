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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';

@injectIntl
@PureRender
@Radium
export default class ContainsText extends Component {

  static propTypes = {
    onEnterText: PropTypes.func.isRequired,
    defaultValue: PropTypes.string,
    intl: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
    this.state = {
      searchInput: ''
    };
  }

  getSearchText() {
    return this.state.searchInput;
  }

  handleContainsEnterText(e) {
    const { onEnterText } = this.props;
    const text = e.target.value;
    this.setState({
      searchInput: text
    }, onEnterText.bind(this, text));
  }

  render() {
    return (
      <div className='contains-text'>
        <input
          className='form-placeholder'
          defaultValue={this.props.defaultValue}
          type='text'
          placeholder={this.props.intl.formatMessage({ id: 'Job.ContainsText' })}
          style={styles.searchInput}
          onInput={this.handleContainsEnterText.bind(this)}/>
      </div>
    );
  }
}


const styles = {
  searchInput: {
    display: 'block',
    border: '1px solid rgba(0,0,0, .1)',
    padding: '5px 10px',
    margin: '0 10px 0 0',
    outline: 0,
    borderRadius: 2,
    height: 24,
    width: 300
  }
};
