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
import { connect } from 'react-redux';
import Immutable from 'immutable';

import { formDescription } from 'uiTheme/radium/typography';
import { getViewState } from 'selectors/resources';
import { loadVersion } from 'actions/resources/version';

import Modal from 'components/Modals/Modal';
import FontIcon from 'components/Icon/FontIcon';
import ViewStateWrapper from 'components/ViewStateWrapper';

import { getEdition } from 'dyn-load/utils/versionUtils';

export const VIEW_ID = 'ABOUT_VIEW_ID';

export class AboutModal extends Component {

  static propTypes = {
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    version: PropTypes.instanceOf(Immutable.Map),
    loadVersion: PropTypes.func.isRequired,
    isOpen: PropTypes.bool,
    hide: PropTypes.func
  };

  state = {
    loadingVersion: false
  }

  componentDidMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(nextProps, prevProps = {}) {
    if (nextProps.isOpen && !nextProps.version && !this.state.loadingVersion) {
      this.setState({loadingVersion: true});
      nextProps.loadVersion({viewId: VIEW_ID}).then(() => {
        this.setState({loadingVersion: false});
      }).catch(() => {
        // TODO: show an error
      });
    }
  }

  renderVersion() {
    if (!this.props.version) {
      return null;
    }

    const version = this.props.version.toJS();

    return <dl>
      <dt style={styles.dtStyle}>{la('Build')}</dt>
      <dd>{version.version}</dd>

      <dt style={styles.dtStyle}>{la('Edition')}</dt>
      <dd>{getEdition()}</dd>

      <dt style={styles.dtStyle}>{la('Build Time')}</dt>
      <dd>{version.buildtime}</dd>

      <dt style={styles.dtStyle}>{la('Change Hash')}</dt>
      <dd>{version.commit.hash}</dd>

      <dt style={styles.dtStyle}>{la('Change Time')}</dt>
      <dd>{version.commit.time}</dd>
    </dl>;
  }

  render() {
    const { isOpen, hide } = this.props;

    return (
      <Modal
        size='small'
        title={la('About Dremio')}
        isOpen={isOpen}
        hide={hide}>
        <div style={styles.container}>
          <div style={styles.logoPane}>
            <FontIcon type='NarwhalLogo' iconStyle={{width: 150, height: 150}}/>
          </div>
          <div style={styles.pane}>
            <div style={{flex: 1}}>
              <div style={{fontSize: '2em', marginBottom: 10}}>Dremio</div>
              <div>
                <ViewStateWrapper viewState={this.props.viewState}>
                  {this.renderVersion()}
                </ViewStateWrapper>
              </div>
            </div>
            <div style={formDescription}>
              {la('Copyright © 2017 Dremio Corporation')}
            </div>
          </div>
        </div>
      </Modal>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
    version: state.resources.entities.get('version').first()
  };
}

const styles = {
  container: {
    display: 'flex',
    width: '100%',
    height: '100%',
    padding: 20
  },

  pane: {
    flex: 2,
    marginLeft: '20px',
    display: 'flex',
    flexDirection: 'column'
  },

  logoPane: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    display: 'flex'
  },

  dtStyle: {
    fontWeight: 'bold',
    marginTop: 15,
    fontSize: '14px'
  }
};

export default connect(mapStateToProps, { loadVersion })(AboutModal);
