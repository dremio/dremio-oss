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
import { connect }   from 'react-redux';
import { Link } from 'react-router';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import { generateApiKey } from 'actions/account';
import { PALE_GREY } from 'uiTheme/radium/colors';
import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';

@Radium
@pureRender
class Api extends Component {
  static propTypes = {
    generateApiKey: PropTypes.string,
    apiKey: PropTypes.string
  }

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    this.props.generateApiKey();
  }

  render() {
    const apiKey = this.props.apiKey && this.props.apiKey || '';
    return (
      <div id='account-api'>
        <h2 style={styles.header}>{la('Api Access')}</h2>
        <div className='description'>
          Lorem ipsum dolor sit amet, consectetur elit, sed do eiusmod tempor incididunt ut
          labore et delore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco
          laboris nisi ut aliquip ex ea commodo consequat.
          For more information or assistance, please visit our  <Link to='#'>{la('help page')}</Link> .
        </div>
        <div style={styles.apiContainer}>
          <div className='property'>
            <label>{la('API Key')}:</label>
            <input id='ApiKeyID' style={styles.apiKey} type='text' value={apiKey} />
            <Button text='Regenerate Key' type={ButtonTypes.CANCEL}></Button>
          </div>
        </div>
        <div className='separator'/>
        <div className='buttons'>
          <Button text='Save' type={ButtonTypes.PRIMARY}></Button>
          <Button text='Cancel' type={ButtonTypes.CANCEL}></Button>
        </div>
      </div>
    );
  }
}

const styles = {
  header: {
    borderBottom: `2px solid ${PALE_GREY}`,
    marginBottom: 10,
    paddingBottom: 10
  },
  apiContainer: {
    overflow: 'hidden',
    margin: '20px 0'
  },
  apiKey: {
    float: 'left',
    height: 28,
    marginRight: 5,
    width: 400
  }
};

function mapStateToProps(state) {
  return {
    apiKey: state.account.apiKey
  };
}
export default connect(mapStateToProps, {
  generateApiKey
})(Api);
