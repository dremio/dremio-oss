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
import { PureComponent, PropTypes } from 'react';

import allBitmaps from 'glob-loader!art/bitmap.pattern';

import SVG from './SVG';


// TBD: impact of not having inline dimensions on render jumps
// can default dimensions be pulled in via webpack?

// There is already window.Image, so...
export default class Art extends PureComponent {
  static propTypes = {
    src: PropTypes.string.isRequired,

    alt: PropTypes.string.isRequired,
    title: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.bool // set to true to take the aria-label
    ])

  }

  render() {
    let {src, alt, title, ...props} = this.props;

    const bitmapURL = allBitmaps[`./${src}`];

    if (title === true) {
      title = alt;
    }

    if (!bitmapURL) {
      return <SVG src={src} aria-label={alt} title={title} {...props} />;
    }

    return <img src={bitmapURL} alt={alt} title={title} {...props} />;
  }
}
