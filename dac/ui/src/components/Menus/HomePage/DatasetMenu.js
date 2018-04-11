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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import urlParse from 'url-parse';
import { removeDataset, removeFile, removeFileFormat } from 'actions/resources/spaceDetails';
import { convertDatasetToFolder } from 'actions/home';
import { showConfirmationDialog } from 'actions/confirmation';
import { constructFullPath, getFullPathListFromEntity } from 'utils/pathUtils';
import copy from 'copy-to-clipboard';

import { TOGGLE_VIEW_ID } from 'components/RightContext/FolderContext';

import DatasetMenuMixin from 'dyn-load/components/Menus/HomePage/DatasetMenuMixin';

// todo: all these entities have a lot of similarities (they are all Datasets of some sort)
// but do not share a protocol/interface. This code *should* be able to
// exist without any special casing.

@DatasetMenuMixin
export class DatasetMenu extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map).isRequired,
    entityType: PropTypes.string.isRequired, // todo: remove and get from #entity (physicalDataset || dataset || file)

    closeMenu: PropTypes.func.isRequired,
    removeDataset: PropTypes.func.isRequired,
    removeFile: PropTypes.func.isRequired,
    removeFileFormat: PropTypes.func.isRequired,
    convertDatasetToFolder: PropTypes.func.isRequired,
    showConfirmationDialog: PropTypes.func
  };

  // Only tested for VDS
  getGraphUrl() {
    const { entity } = this.props;
    // todo: seems very brittle, and it should be a computed prop of the entity
    const editUrl = entity.getIn(['links', 'edit']);
    const parseUrl = urlParse(editUrl);
    return `${parseUrl.pathname}/graph${parseUrl.query}`;
  }

  // only tested with VDS
  getRenameLocation() {
    const { entity } = this.props;
    return {
      ...this.context.location,
      state: {
        ...this.context.location.state,
        modal: 'UpdateDataset',
        item: entity,
        query: {
          fullPath: entity.get('fullPath'),
          name: entity.get('datasetName'),
          mode: 'rename'
        }
      }
    };
  }

  // only tested with VDS
  getMoveLocation() {
    const location = this.getRenameLocation();
    location.state.query.mode = 'move';
    return location;
  }

  getSettingsLocation() {
    const { entity, entityType } = this.props;

    return {
      ...this.context.location,
      state: {
        modal: 'DatasetSettingsModal',

        // todo: normalize
        entityId: entity.get('versionedResourcePath') // VDS
          || entity.get('id'), // file, folder, PDS (see resourceDecorators)

        entityType
      }
    };
  }

  // only tested with File, Folder
  removeFormat = () => {
    if (this.props.entityType === 'file') {
      this.props.removeFileFormat(this.props.entity);
    } else {
      this.props.convertDatasetToFolder(this.props.entity, TOGGLE_VIEW_ID); // todo: cross-knowledge for view id seems brittle
    }
  }

  handleRemoveFormat = () => {
    this.props.showConfirmationDialog({
      title: la('Remove Format'),
      text: la('Are you sure you want to remove format for this item?'),
      confirmText: la('Remove'),
      confirm: () => this.removeFormat()
    });
    this.props.closeMenu();
  }

  removeEntity() {
    if (this.props.entityType === 'file') {
      this.props.removeFile(this.props.entity);
    } else {
      this.props.removeDataset(this.props.entity);
    }
  }

  handleRemove = () => {
    const title = this.props.entityType === 'file'
      ? la('Remove File') : la('Remove Dataset');
    this.props.showConfirmationDialog({
      text: la('Are you sure you want to remove this item?'),
      confirmText: la('Remove'),
      confirm: () => this.removeEntity(),
      title
    });
    this.props.closeMenu();
  }

  copyPath = () => {
    const fullPath = constructFullPath(getFullPathListFromEntity(this.props.entity));
    copy(fullPath);
    this.props.closeMenu();
  }
}

export default connect(null, {
  removeDataset,
  removeFileFormat,
  removeFile,
  showConfirmationDialog,
  convertDatasetToFolder
})(DatasetMenu);
