/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { Component } from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import Immutable from "immutable";
import invariant from "invariant";

import { getEntity, getViewState } from "selectors/resources";
import ApiUtils from "utils/apiUtils/apiUtils";

import { resetViewState } from "actions/resources";
import {
  loadFileFormat,
  loadFilePreview,
  saveFileFormat,
  resetFileFormatPreview,
} from "actions/modals/addFileModal";

import FileFormatForm from "pages/HomePage/components/forms/FileFormatForm";
import {
  getSaveFormatUrl,
  getFormatPreviewUrl,
  getCurrentFormatUrl,
  getQueryUrl,
} from "@app/selectors/home";

export const VIEW_ID = "FileFormatModal";
const PREVIEW_VIEW_ID = "FileFormatModalPreview";

export class FileFormatController extends Component {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };

  static propTypes = {
    // a root entity must be source or home space
    fullPath: PropTypes.arrayOf(PropTypes.string.isRequired),
    // current entity is folder or file
    isFolder: PropTypes.bool,

    //connected
    formatUrl: PropTypes.string,

    onDone: PropTypes.func,
    query: PropTypes.object,
    fileFormat: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    previewViewState: PropTypes.instanceOf(Immutable.Map),
    loadFileFormat: PropTypes.func,
    saveFileFormat: PropTypes.func,
    loadFilePreview: PropTypes.func,
    resetFileFormatPreview: PropTypes.func,
    updateFormDirtyState: PropTypes.func,
    resetViewState: PropTypes.func,
  };

  componentDidMount() {
    this.loadFormat(this.props);
  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    if (!this.props.formatUrl) {
      this.loadFormat(nextProps);
    }
  }

  loadFormat(props) {
    const { formatUrl } = props;
    if (formatUrl) {
      props.resetViewState(VIEW_ID);
      props.resetViewState(PREVIEW_VIEW_ID);
      props.resetFileFormatPreview();
      props.loadFileFormat(formatUrl, VIEW_ID);
    }
  }

  onSubmitFormat = (values) => {
    const { fullPath, isFolder } = this.props;
    return ApiUtils.attachFormSubmitHandlers(
      this.props.saveFileFormat(getSaveFormatUrl(fullPath, isFolder), values)
    ).then(() => {
      if (this.props.query && this.props.query.then === "query") {
        this.context.router.replace(getQueryUrl(fullPath));
      } else {
        this.props.onDone(null, true);
      }
      return null;
    });
  };

  onPreview = (values) => {
    const { fullPath, isFolder } = this.props;
    this.props.loadFilePreview(
      getFormatPreviewUrl(fullPath, isFolder),
      values,
      PREVIEW_VIEW_ID
    );
  };

  render() {
    const { viewState, previewViewState, updateFormDirtyState, fileFormat } =
      this.props;
    return (
      <FileFormatForm
        updateFormDirtyState={updateFormDirtyState}
        fileFormat={fileFormat}
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
  const { fullPath, entityType } = props;
  invariant(
    !entityType || ["file", "folder"].indexOf(entityType) !== -1, // todo: DRY this up - all other checks like this moved to DatasetSettings
    "FileFormatController can only work on file or folder entities. Got " +
      entityType
  );

  const isFolder = entityType === "folder";
  const formatUrl = fullPath ? getCurrentFormatUrl(fullPath, isFolder) : null;

  const fileFormat = getEntity(state, formatUrl, "fileFormat");

  return {
    fileFormat,
    isFolder,
    formatUrl,
    viewState: getViewState(state, VIEW_ID),
    previewViewState: getViewState(state, PREVIEW_VIEW_ID),
  };
}

export default connect(mapStateToProps, {
  loadFileFormat,
  saveFileFormat,
  loadFilePreview,
  resetFileFormatPreview,
  resetViewState,
})(FileFormatController);
