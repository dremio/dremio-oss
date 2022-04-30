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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';

import { allBitmaps } from 'dyn-load/components/bitmapLoader';
import { Tooltip } from 'dremio-ui-lib';
// for lottie jsons
import { allJsons } from '@app/components/jsonImageLoader';
import Lottie from 'react-lottie';
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
      PropTypes.any,
      PropTypes.bool // set to true to take the aria-label
    ]),
    interactive: PropTypes.bool,
    id: PropTypes.any,
    tooltipOpen: PropTypes.bool
  }

  render() {
    let {src, alt, title, id, interactive, tooltipOpen, ...props} = this.props;

    const bitmapURL = allBitmaps[`./${src}`];
    if (title === true) {
      title = alt;
    }

    if (src.includes('.json')) {
      const defaultOptions = {
        loop: true,
        autoplay: true,
        animationData: allJsons[`./${src}`],
        rendererSettings: {
          preserveAspectRatio: 'xMidYMid slice'
        }
      };
      return (
        title ?
          <div>
            <Tooltip title={title} interactive={interactive} open={tooltipOpen}>
              <div>
                <Lottie
                  options={defaultOptions}
                  height={24}
                  width={24}
                />
              </div>
            </Tooltip>
          </div> :
          <div>
            <Lottie
              options={defaultOptions}
              height={24}
              width={24}
            />
          </div>
      );
    }

    if (!bitmapURL) {
      return <SVG src={src} aria-label={alt} title={title} dataQa={src} id={id} interactive={interactive} {...props} />;
    }


    return (
      title ?
        <Tooltip title={title}>
          <img src={bitmapURL} alt={alt} {...props} />
        </Tooltip>
        :
        <img src={bitmapURL} alt={alt} {...props} />
    );
  }
}
