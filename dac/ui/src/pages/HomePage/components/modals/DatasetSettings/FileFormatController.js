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
import invariant from 'invariant';

import {getEntity, getViewState} from 'selectors/resources';
import ApiUtils from 'utils/apiUtils/apiUtils';

import { resetViewState } from 'actions/resources';
import {
  loadFileFormat,
  loadFilePreview,
  saveFileFormat,
  resetFileFormatPreview
} from 'actions/modals/addFileModal';

import FileFormatForm from 'pages/HomePage/components/forms/FileFormatForm';

export const VIEW_ID = 'FileFormatModal';
const PREVIEW_VIEW_ID = 'FileFormatModalPreview';

export class FileFormatController extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  static propTypes = {
    onDone: PropTypes.func,
    query: PropTypes.object,
    entity: PropTypes.instanceOf(Immutable.Map),
    fileFormat: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    previewViewState: PropTypes.instanceOf(Immutable.Map),
    loadFileFormat: PropTypes.func,
    saveFileFormat: PropTypes.func,
    loadFilePreview: PropTypes.func,
    resetFileFormatPreview: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    resetViewState: PropTypes.func
  }

  componentDidMount() {
    this.loadFormat(this.props);
  }

  componentWillReceiveProps(nextProps) {
    if (!this.props.entity) {
      this.loadFormat(nextProps);
    }
  }

  loadFormat(props) {
    if (props.entity) {
      props.resetViewState(VIEW_ID);
      props.resetViewState(PREVIEW_VIEW_ID);
      props.resetFileFormatPreview();
      props.loadFileFormat(props.entity, VIEW_ID);
    }
  }

  getFileFormatForSubmit = (entity) => {
    let fileFormat = entity.get('fileFormat');
    if (!fileFormat) {
      // replace /file/ in 'id' with /file_format/ to make self link
      const selfLink = entity.get('id').replace('/file/', '/file_format/');
      fileFormat = Immutable.fromJS({links: {self: selfLink}});
    }
    return fileFormat;
  };

  onSubmitFormat = (values) => {
    const {entity} = this.props;
    const queryUrl = entity.getIn(['links', 'query']);
    // saveFileFormat needs link url in 1st argument. If entity.fileFormat is not ready, make it up with entityId
    return ApiUtils.attachFormSubmitHandlers(
      this.props.saveFileFormat(this.getFileFormatForSubmit(entity), values)
    ).then(() => {
      if (this.props.query && this.props.query.then === 'query') {
        this.context.router.replace(queryUrl);
      } else {
        this.props.onDone(null, true);
      }
    });
  }

  onPreview = (values) => {
    const {entity} = this.props;
    this.props.loadFilePreview(entity, values, PREVIEW_VIEW_ID);
  }

  render() {
    const { entity, viewState, previewViewState, updateFormDirtyState } = this.props;
    return (
      <FileFormatForm
        updateFormDirtyState={updateFormDirtyState}
        file={entity}
        onFormSubmit={this.onSubmitFormat}
        onPreview={this.onPreview}
        onCancel={this.props.onDone}
        viewState={viewState}
        previewViewState={previewViewState}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const { entityType, entityId } = props;
  invariant(!entityType || ['file', 'folder'].indexOf(entityType) !== -1, // todo: DRY this up - all other checks like this moved to DatasetSettings
    'FileFormatController can only work on file or folder entities. Got ' + entityType);

  const entity = getEntity(state, entityId, entityType);
  const fileFormat = entity ? getEntity(state, entity.getIn(['links', 'format']), 'fileFormat') : undefined;

  return {
    entity: entity ? entity.set('fileFormat', fileFormat) : undefined,
    viewState: getViewState(state, VIEW_ID),
    previewViewState: getViewState(state, PREVIEW_VIEW_ID)
  };
}

export default connect(mapStateToProps, {
  loadFileFormat,
  saveFileFormat,
  loadFilePreview,
  resetFileFormatPreview,
  resetViewState
})(FileFormatController);
