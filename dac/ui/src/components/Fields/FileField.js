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
import {Component, PropTypes} from 'react';
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import { formDescription } from 'uiTheme/radium/typography';
import { BLUE } from 'uiTheme/radium/colors';
import { FLEX_NOWRAP_ROW_SPACE_BETWEEN_START, FLEX_NOWRAP_CENTER_START } from 'uiTheme/radium/flexStyle';

import Dropzone from 'react-dropzone';
import FontIcon from 'components/Icon/FontIcon';

@Radium
@pureRender
export default class FileField extends Component {
  static propTypes = {
    style: PropTypes.object,
    value: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    onChange: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.onDrop = this.onDrop.bind(this);
    this.onOpenClick = this.onOpenClick.bind(this);
  }

  onOpenClick() {
    this.refs.dropzone.open();
  }

  onDrop(f) {
    const file = f[0];
    new Promise(resolve => {
      const reader = new FileReader();
      reader.onload = result => resolve([result, file]);
      reader.readAsText(file);
    })
    .then(zippedResults => this.props.onChange(zippedResults[1]));
  }

  getTextValue(value) {
    if (value && value.name) {
      return value.name;
    }
    return '';
  }

  render() {
    const {style, value} = this.props; // todo: loc with sub patterns
    return (
      <div style={[styles.base, style]}>
        <Dropzone ref='dropzone' onDrop={this.onDrop} disableClick multiple={false} style={styles.dropTarget}>
          <FontIcon type='Upload' theme={styles.dropIcon}/>
          <div style={[FLEX_NOWRAP_CENTER_START, formDescription]}>
            <span style={{margin: '0 5px'}}>Drop a local file here, or</span>
            <a onClick={this.onOpenClick}>browse</a>.
          </div>
          <span style={{marginTop: 10}}>{this.getTextValue(value)}</span>
        </Dropzone>
      </div>
    );
  }
}
const styles = {base: {...FLEX_NOWRAP_ROW_SPACE_BETWEEN_START},
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
