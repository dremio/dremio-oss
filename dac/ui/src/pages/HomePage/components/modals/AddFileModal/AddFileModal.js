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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { destroy } from 'redux-form';
import Immutable from 'immutable';
import { injectIntl } from 'react-intl';

import Modal from 'components/Modals/Modal';
import Message from 'components/Message';
import { denormalizeFile } from '@app/reducers/home/content';
import { MAX_UPLOAD_FILE_SIZE } from '@app/constants/Constants';
import NumberFormatUtils from 'utils/numberFormatUtils';

import {
  uploadFileToPath,
  loadFilePreview,
  uploadFinish,
  uploadCancel,
  resetFileFormatPreview
} from 'actions/modals/addFileModal';

import ApiUtils from 'utils/apiUtils/apiUtils';
import { getHomeEntity } from '@app/selectors/home';
import { getViewState } from 'selectors/resources';
import { resetViewState } from 'actions/resources';

import FileFormatForm from '../../forms/FileFormatForm';
import AddFileFormPage1 from './AddFileFormPage1';


export const PREVIEW_VIEW_ID = 'AddFileModalPreview';

@injectIntl
export class AddFileModal extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    hide: PropTypes.func,

    //connected

    previewViewState: PropTypes.instanceOf(Immutable.Map),
    fileName: PropTypes.string,
    file: PropTypes.instanceOf(Immutable.Map),
    uploadFileToPath: PropTypes.func,
    loadFilePreview: PropTypes.func.isRequired,
    uploadFinish: PropTypes.func.isRequired,
    uploadCancel: PropTypes.func.isRequired,
    destroy: PropTypes.func.isRequired,
    resetViewState: PropTypes.func.isRequired,
    resetFileFormatPreview: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  static contextTypes = {username: PropTypes.string};

  tooLargeMsg = la(`The file is too large. Dremio UI supports file uploads up to ${NumberFormatUtils.formatMemoryInMB(MAX_UPLOAD_FILE_SIZE)}.`);

  constructor(props) {
    super(props);
    this.state = { page: 0, tooLarge: false };
  }

  componentWillMount() {
    this.success = false;
    this.props.resetViewState(PREVIEW_VIEW_ID);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.fileName // fileName is undefined after success
      && nextProps.fileName !== this.props.fileName) {
      this.cancelUpload();
    }
  }

  resetState = () => {
    this.setState({ page: 0, tooLarge: false });
  };

  onHide = (success) => {
    this.resetState();
    if (!success) {
      this.cancelUpload();
    }
    this.props.destroy('addFile');
    this.props.resetViewState(PREVIEW_VIEW_ID);
    this.props.hide();
  };

  checkIfFileIsTooLarge = (file) => {
    const isTooLarge = (file.size > MAX_UPLOAD_FILE_SIZE);
    this.setState({ tooLarge: isTooLarge });
    return isTooLarge;
  };

  onSubmitFile = (values) => {
    const { file, name, extension } = values;
    if (this.checkIfFileIsTooLarge(file)) return;

    this.props.resetFileFormatPreview();

    return ApiUtils.attachFormSubmitHandlers(this.props.uploadFileToPath(file, { name }, extension))
      .then(() => {
        this.goToPage(1);
      });
  };

  onSubmitFormat = (values) => {
    const { file } = this.props;
    if (this.checkIfFileIsTooLarge(file)) return;

    return ApiUtils.attachFormSubmitHandlers(
      this.props.uploadFinish(file, values, PREVIEW_VIEW_ID)
    ).then(() => {
      this.onHide(true);
    });
  };

  onPreview = (values) => {
    const { file } = this.props;

    this.props.loadFilePreview(file.getIn(['links', 'format_preview']), values, PREVIEW_VIEW_ID);
  };

  onFileChange = (file) => {
    this.checkIfFileIsTooLarge(file);
  };

  cancelUpload() {
    const { file } = this.props;
    if (file) {
      this.props.uploadCancel(file);
    }
  }

  goToPage = (pageNumber) => {
    this.setState({ page: pageNumber });
    if (pageNumber !== 1) {
      this.props.resetViewState(PREVIEW_VIEW_ID);
    }
  };

  renderTooLargeMessage() {
    const { tooLarge } = this.state;
    return tooLarge && (
      <Message
        messageType='error'
        message={this.tooLargeMsg}
        onDismiss={this.resetState}
        detailsStyle={{maxHeight: 100}}
      />
    );
  }

  render() {
    const { file, isOpen, previewViewState, intl } = this.props;
    const { page } = this.state;
    const pageSettings = [{
      title: intl.formatMessage({ id: 'File.AddFileStep1' }),
      size: 'small'
    }, {
      title: intl.formatMessage({ id: 'File.AddFileStep2' }),
      size: 'large'
    }];
    return (
      <Modal
        size={pageSettings[page].size}
        title={pageSettings[page].title}
        isOpen={isOpen}
        hide={this.onHide}>
        {this.renderTooLargeMessage()}
        {page === 0 &&
          <AddFileFormPage1
            ref='form'
            onFormSubmit={this.onSubmitFile}
            onCancel={this.onHide}
            onChange={this.onFileChange}
          />
        }
        {page === 1 &&
          <FileFormatForm
            fileFormat={file ? file.get('fileFormat') : null}
            onFormSubmit={this.onSubmitFormat}
            onCancel={this.goToPage.bind(this, 0)}
            cancelText={intl.formatMessage({ id: 'Common.Back' })}
            onPreview={this.onPreview}
            previewViewState={previewViewState}
          />
        }
      </Modal>
    );
  }
}

function mapStateToProps(state) {
  const parentEntity = getHomeEntity(state) || Immutable.Map();
  const fileName = state.form.addFile && state.form.addFile.name ? state.form.addFile.name.value : undefined;
  let file;
  if (parentEntity && fileName) {
    const fileUrlPath = parentEntity.getIn(['links', 'file_prefix']) + '/' + encodeURIComponent(fileName);
    file = denormalizeFile(state.resources, fileUrlPath);
  }
  return {
    previewViewState: getViewState(state, PREVIEW_VIEW_ID),
    fileName,
    file
  };
}

export default connect(mapStateToProps, {
  uploadFileToPath,
  loadFilePreview,
  uploadFinish,
  uploadCancel,
  destroy,
  resetViewState,
  resetFileFormatPreview
})(AddFileModal);
