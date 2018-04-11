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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import ViewStateWrapper from 'components/ViewStateWrapper';
import { formDescription } from 'uiTheme/radium/typography';
import { loadSourceCredentials, addSourceCredential, removeSourceCredential } from 'actions/account';
import { PALE_GREY } from 'uiTheme/radium/colors';
import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import AmazonS3Credential from './components/AmazonS3Credential';
import MongoDBCredential from './components/MongoDBCredential';

import './Datastore.less'; // TODO to Vasyl, need to use Radium

@Radium
@pureRender
class Datastore extends Component {
  static propTypes = {
    loadSourceCredentials: PropTypes.func,
    addSourceCredential: PropTypes.func,
    removeSourceCredential: PropTypes.func,
    sourceCredentialList: PropTypes.object
  }

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    this.props.loadSourceCredentials();
  }

  addCredential() {
    this.props.addSourceCredential();
  }

  removeAccountCredential(index) {
    this.props.removeSourceCredential(index);
  }

  renderAccountCredentials(credentials) {
    return credentials.map( (credential, index) => {
      switch (credential.type) {
      case 'amazon-s3':
        return (
          <AmazonS3Credential key={index}
            credential={credential} onRemoveClick={ this.removeAccountCredential.bind(this, index) }  />
        );
      case 'mongo' :
        return (
          <MongoDBCredential key={index}
            credential={credential} onRemoveClick={this.removeAccountCredential.bind(this, index) } />
        );
      default:
        return (
          <div key={index}>Unknown credentials</div>
        );
      }
    });
  }

  render() {
    const sourceCredentialItems = this.props.sourceCredentialList && this.props.sourceCredentialList.items || [];
    return (
      <div id='account-datastore'>
        <h2 style={styles.header}>{la('General Information')}</h2>
        <div className='content-block'>
          <div className='description' style={formDescription}>
            Data store credentials are auto-filled whenever you access a new source. Lorem ipsum dolor sit amet,
            consectetur adipiscing elit, sed do eiusmod tempor incididunt.
          </div>
          <div className='credentials'>
            <div className='add-credentials action' onClick={ this.addCredential.bind(this) } >+ Add credentials</div>
            <ViewStateWrapper viewState={this.props.sourceCredentialList}>
              {this.renderAccountCredentials(sourceCredentialItems)}
            </ViewStateWrapper>
          </div>
        </div>
        <div className='separator' />
        <div className='buttons'>
          <Button text='Save' type={ButtonTypes.PRIMARY}></Button>
          <Button text='Cancel' type={ButtonTypes.CANCEL}></Button>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    sourceCredentialList: state.account.sourceCredentialList
  };
}

export default connect(mapStateToProps, {
  loadSourceCredentials,
  addSourceCredential,
  removeSourceCredential
})(Datastore);


const styles = {
  header: {
    borderBottom: `2px solid ${PALE_GREY}`,
    marginBottom: 10,
    paddingBottom: 10
  }
};

