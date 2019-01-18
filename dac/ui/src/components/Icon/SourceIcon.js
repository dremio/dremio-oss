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
import Art from 'components/Art';


export default class SourceIcon extends Component {
  static propTypes = {
    style: PropTypes.object,
    src: PropTypes.string
  };

  extractSvgFromSrc = src => {
    const startSvgTag = '<svg ';
    const endSvgTag = '</svg>';
    const startPos = src.indexOf(startSvgTag);
    const endPos = src.indexOf(endSvgTag) + endSvgTag.length;
    return src.substring(startPos, endPos);
  };

  isSvgSafe = src => {
    // - does not include any of '<script ', '<foreignObject', TODO?: 'http://', 'https://'
    const srcUpper = src.toUpperCase();
    return !srcUpper.includes('<SCRIPT') && !srcUpper.includes('<FOREIGNOBJECT');
  };

  render() {
    const { src, style } = this.props;
    const iconStyle = {...styles.iconStyle, ...style};

    // svg icon can be an inline svg/xml;
    // example:
    // <?xml version="1.0" encoding="UTF-8" standalone="no"?><!-- Copyright ...--><svg version="1.1" xmlns="http://www.w3.org/2000/svg"><text x="10" y="34" style="color:#ff0000">FakeSVG</text></svg>"
    if (src.includes('<svg')) {
      const svgPart = this.extractSvgFromSrc(src);
      if (!this.isSvgSafe(svgPart)) {
        console.warn('Source icon SVG code is unsafe');
        return null;
      }
      return (<div style={iconStyle} dangerouslySetInnerHTML={{__html: svgPart}}></div>);
    }

    // icon src can be a file name from our code base with .svg or .png extension
    // example: 'S3.svg' or 'sources/NETEZZA.png'
    if (src.includes('.svg') || src.includes('.png')) {
      // icon src may include path; otherwise assume that it is in sources folder
      const iconSrc = (src.includes('/')) ? src : `sources/${src}`;
      return (<Art src={iconSrc} alt={''} style={iconStyle} />);
    }

    // as a last option icon can be a raw base64 image source
    return (<img src={`data:image/svg+xml;base64,${src}`} style={iconStyle}></img>);
  }

}

const styles = {
  iconStyle: {
    margin: '0 20px 0 10px',
    width: 60,
    height: 60
  }
};
