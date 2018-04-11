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
import {connect} from 'react-redux';
import Immutable from 'immutable';
import Toggle from 'material-ui/Toggle';

import FontIcon from 'components/Icon/FontIcon';

import { getViewState } from 'selectors/resources';

import { convertDatasetToFolder, convertFolderToDataset } from 'actions/home';

import {title, contextCard, cardTitle, contextAttrs, attrLabel, attrValue} from 'uiTheme/radium/rightContext';

export const TOGGLE_VIEW_ID = 'toggleFolderPhysicalDataset';

export class FolderContext extends Component {

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    toggleView: PropTypes.instanceOf(Immutable.Map),
    convertDatasetToFolder: PropTypes.func.isRequired,
    convertFolderToDataset: PropTypes.func.isRequired
  };

  static contextTypes = {
    location: PropTypes.object,
    router: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.handleIsPhysicalDatasetChange = this.handleIsPhysicalDatasetChange.bind(this);
  }

  handleIsPhysicalDatasetChange(e) {
    const {entity} = this.props;
    const checked = e.target.checked;
    this.setState({isTogglePhysicalDatasetInProgress: true});
    if (checked) {
      this.context.router.push({...this.context.location, state: {
        modal: 'FolderFormatModal',
        query: {entityId: entity.get('id'), entityType: 'folder'}
      }});
    } else {
      this.props.convertDatasetToFolder(entity, TOGGLE_VIEW_ID);
    }
  }

  renderPhysicalDatasetSwitch() {
    const { entity, toggleView } = this.props;

    return <label style={styles.base}>
      <div style={styles.toggleWrap}>
        { toggleView.get('isInProgress') ?
          <FontIcon type='Loader spinner'/>
            : <div>
              <Toggle
                onToggle={this.handleIsPhysicalDatasetChange}
                toggled={entity.get('isPhysicalDataset')}
                style={styles.toggle}
              />
              <span data-on='On' data-off='Off'></span>
            </div>
          }
      </div>
      <p style={styles.toggleDescription}>Allow this folder to be queried like a single dataset</p>
    </label>;
  }

  render() {
    const {entity} = this.props;

    return <div>
      {entity.get('fileSystemFolder') && this.renderPhysicalDatasetSwitch()}
      <h4 style={title}>About this Folder</h4>
      <div style={contextCard}>
        <div style={cardTitle}>Details</div>
        <ul style={contextAttrs}>
          <li>
            <span style={attrLabel}>Created At:</span>
            <span style={attrValue}>{entity.get('ctime')}</span>
          </li>
        </ul>
        <div className='description'>
          {entity.get('description')}
        </div>
      </div>
    </div>;
  }
}

function mapStateToProps(state) {
  return {
    toggleView: getViewState(state, TOGGLE_VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  convertDatasetToFolder,
  convertFolderToDataset
})(FolderContext);

const styles = {
  base: {
    display: 'flex',
    margin: '0 0 10px 10px',
    cursor: 'pointer'
  },
  toggleWrap: {
    width: 60
  },
  toggle: {
    marginRight: 8,
    marginLeft: -8
  }
};
