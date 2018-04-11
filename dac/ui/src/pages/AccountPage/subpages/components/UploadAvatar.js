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
import './UploadAvatar.less';
import { Component } from 'react';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

@pureRender
export default class UploadAvatar extends Component {
  static propTypes = {
    onLoad: PropTypes.func,
    url: PropTypes.string
  };

  constructor(props) {
    super(props);

    this.state = {
      progress: -1,
      hasError: false
    };

    this.onSelectFile = this.onSelectFile.bind(this);
    this.doUpload = this.doUpload.bind(this);
    this.renderProgress = this.renderProgress.bind(this);
  }

  onSelectFile(e) {
    if (e.target.value.length > 0) {
      this.setState({
        progress:0,
        hasError: false
      }, this.doUpload);
    }
  }

  doUpload() {
    const form = new FormData(this.refs.form);
    const req = new XMLHttpRequest(form);
    req.open('POST', this.props.url);

    // Upload avatar in process
    req.upload.addEventListener('progress', (e) => {
      const progress = e.total !== 0 ? parseInt((e.loaded / e.total) * 100, 10) : 0;
      this.setState({
        progress
      });
    }, false);

    // Upload avatar complete
    req.addEventListener('load', (e) => {
      this.setState( {progress: -1, hasError: req.status !== 200 }, () => {
        this.props.onLoad(e, req);
      });
    }, false);

    req.send(form);
  }

  renderForm() {
    return (
      <div>
        <div className='action'>Change Avatar</div>
        <form className='_react_fileupload_form_content' ref='form' method='post'>
          <input type='file' name='file' onChange={this.onSelectFile} />
        </form>
      </div>
    );
  }

  renderProgress(progress) {
    if (progress > -1 ) {
      const barStyle = { width : progress + '%' };

      let message = (<span>Uploading ...</span>);
      if (progress === 100) {
        message = (<span >Successfully uploaded</span>);
      }

      return (
        <div>
          <div className='progressWrapper' >
            <div className='progressBar' style={barStyle}></div>
          </div>
          <div style={{'clear':'left'}}>
            {message}
          </div>
        </div>
      );
    }
    return; // TODO to Vasyl, I think it excess
  }

  render() {
    return (
      <div id='upload-avatar'>
        { this.renderForm() }
        { this.renderProgress(this.state.progress) }
      </div>
    );
  }
}
