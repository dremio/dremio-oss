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
import copy from 'copy-to-clipboard';

import { removeDataset, removeFile } from 'actions/resources/spaceDetails';
import { showConfirmationDialog } from 'actions/confirmation';
import { constructFullPath, getFullPathListFromEntity } from 'utils/pathUtils';
import { UpdateMode } from 'pages/HomePage/components/modals/UpdateDataset/UpdateDatasetView';

import DatasetMenuMixin from 'dyn-load/components/Menus/HomePage/DatasetMenuMixin';

// todo: all these entities have a lot of similarities (they are all Datasets of some sort)
// but do not share a protocol/interface. This code *should* be able to
// exist without any special casing.

export const getSettingsLocation = (location, entity, entityType) => ({
  ...location,
  state: {
    modal: 'DatasetSettingsModal',

    // todo: normalize
    entityId: entity.get('versionedResourcePath') // VDS
      || entity.get('id'), // file, folder, PDS (see resourceDecorators)

    entityType
  }
});

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
    showConfirmationDialog: PropTypes.func
  };

  getMenuItemUrl(itemCode) {
    const { entity } = this.props;
    // todo: seems very brittle, and it should be a computed prop of the entity
    const url = entity.getIn(['links', 'query']);
    const parseUrl = urlParse(url);
    return `${parseUrl.pathname}/${itemCode}${parseUrl.query}`;
  }

  getLocationConfig = (mode) => {
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
          getGraphLink: this.getGraphLink(),
          mode
        }
      }
    };
  };
  // only tested with VDS
  getRenameLocation() {
    return this.getLocationConfig(UpdateMode.rename);
  }

  // only tested with VDS
  getMoveLocation() {
    return this.getLocationConfig(UpdateMode.move);
  }

  getRemoveLocation() {
    return this.getLocationConfig(UpdateMode.remove);
  }

  getRemoveFormatLocation() {
    return this.getLocationConfig(UpdateMode.removeFormat);
  }

  getSettingsLocation() {
    const { entity, entityType } = this.props;

    return getSettingsLocation(this.context.location, entity, entityType);
  }

  handleRemoveFile = () => {
    const { closeMenu, entity } = this.props;
    this.props.showConfirmationDialog({
      text: la(`Are you sure you want to remove file "${entity.get('name')}"?`),
      confirmText: la('Remove'),
      confirm: () => this.props.removeFile(entity),
      title: la('Remove File')
    });
    closeMenu();
  };

  copyPath = () => {
    const fullPath = constructFullPath(getFullPathListFromEntity(this.props.entity));
    copy(fullPath);
    this.props.closeMenu();
  }
}

export default connect(null, {
  removeDataset,
  removeFile,
  showConfirmationDialog
})(DatasetMenu);
