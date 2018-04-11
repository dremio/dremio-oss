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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import { FormattedMessage, injectIntl } from 'react-intl';

import { formDescription } from 'uiTheme/radium/typography';
import { BLUE, PALE_GREY, PALE_BLUE } from 'uiTheme/radium/colors';
import { FLEX_NOWRAP_ROW_SPACE_BETWEEN_START, FLEX_NOWRAP_CENTER_START } from 'uiTheme/radium/flexStyle';
import fileUtils, { BYTES_IN_MB } from 'utils/fileUtils/fileUtils';

import Dropzone from 'react-dropzone';
import FontIcon from 'components/Icon/FontIcon';

const PROGRESS_BAR_WIDTH = 180;

@injectIntl
@Radium
@pureRender
export default class FileField extends Component {
  static propTypes = {
    style: PropTypes.object,
    value: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    onChange: PropTypes.func,
    intl: PropTypes.object.isRequired
  };

  state = {
    loadProgressTime: 0,
    total: 0,
    loaded: 0,
    loadSpeed: 0
  };

  onOpenClick = () => {
    this.refs.dropzone.open();
  }

  onDrop = (f) => {
    const file = f[0];
    const reader = new FileReader();
    reader.onprogress = this.updateProgress;
    reader.onloadend = () => {
      this.props.onChange(file, false);
      this.endProgress();
    };
    reader.onloadstart = () => {
      this.props.onChange(file, true);
      this.startProgress();
    };
    reader.readAsText(file);
  }

  getFileName(value) {
    if (value && value.name) {
      return value.name;
    }
    return '';
  }

  getFileSize(value) {
    if (value && value.size) {
      return fileUtils.convertFileSize(value.size);
    }
    return '';
  }

  startProgress = () => {
    this.setState({
      loadProgressTime: (new Date()).getTime()
    });
  }

  endProgress = () => {
    this.setState({
      loadProgressTime: 0,
      total: 0,
      loaded: 0,
      loadSpeed: 0
    });
  }

  updateProgress = (event) => {
    const currentTime = (new Date()).getTime();
    const loaded = Number(event.loaded);

    this.setState({
      total: Number(event.total),
      loaded,
      loadSpeed: this.calculateLoadSpeed(loaded, currentTime),
      loadProgressTime: currentTime
    });
  }

  calculateLoadSpeed(loaded, currentTime) {
    const loadDiff = (loaded - this.state.loaded) / BYTES_IN_MB;
    const timeDiff = (currentTime - this.state.loadProgressTime) / 1000;
    return Number(loadDiff / timeDiff).toFixed(1);
  }

  renderFileInfo(value) {
    if (!value) {
      return null;
    }
    return (
      <div style={styles.fileInfo}>
        <span>{this.getFileName(value)}{' '}</span>
        <b>{this.getFileSize(value)}</b>
      </div>
    );
  }

  renderProgressBar() {
    const { total, loaded, loadSpeed } = this.state;

    if (total === 0) {
      return null;
    }
    const progress = Math.round((loaded / total) * 100) + '%';
    return (
      <div>
        <progress value={loaded} max={total} style={styles.progressBar}/>
        <div style={{display: 'flex', justifyContent: 'space-between'}}>
          <span>{progress}</span>
          <span>{`${loadSpeed}MB/s`}</span>
        </div>
      </div>
    );
  }

  render() {
    const { style, value } = this.props; // todo: loc with sub patterns
    const inProgressStyle = this.state.total
      ? { background: PALE_BLUE, borderStyle: 'solid' }
      : { borderStyle: 'dashed' };
    return (
      <div className='field' style={[styles.base, style]}>
        <Dropzone ref='dropzone' onDrop={this.onDrop} disableClick multiple={false}
          style={{...styles.dropTarget, ...inProgressStyle}}>
          <FontIcon type='Upload' theme={styles.dropIcon}/>
          <div style={[FLEX_NOWRAP_CENTER_START, formDescription, {whiteSpace: 'pre'}]}>
            <span><FormattedMessage id='File.DropLocalFile'/>{' '}</span> {/* todo: loc better (sentence should be one string) */}
            <a onClick={this.onOpenClick}><FormattedMessage id='File.Browse'/></a>.
          </div>
          {this.renderFileInfo(value)}
          {/* should be upload progress, not read this.renderProgressBar() */}
        </Dropzone>
      </div>
    );
  }
}
const styles = {
  progressBar: {
    width: PROGRESS_BAR_WIDTH,
    height: 5,
    marginBottom: 5
  },
  fileInfo: {
    marginTop: 10,
    minWidth: PROGRESS_BAR_WIDTH,
    padding: '5px 10px',
    borderRadius: 3,
    border: `2px solid ${PALE_GREY}`,
    display: 'flex',
    justifyContent: 'space-between',
    backgroundColor: '#fff',
    whiteSpace: 'pre',
    color: '#000'
  },
  base: {...FLEX_NOWRAP_ROW_SPACE_BETWEEN_START},
  dropTarget: {
    height: 270,
    display: 'flex',
    alignItems: 'center',
    flexDirection: 'column',
    width: '100%',
    paddingTop: 85,
    border: `1px dashed ${BLUE}`,
    marginBottom: 40,
    cursor: 'pointer',
    ...formDescription
  },
  dropIcon: {
    Icon: {
      height: 75,
      width: 90
    },
    Container: {
      position: 'relative',
      bottom: 6,
      height: 75,
      width: 90
    }
  }
};
