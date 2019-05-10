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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import ReactDOMServer from 'react-dom/server';
import invariant from 'invariant';

import allSVGs from 'glob-loader!art/svg.pattern';

// inline SVGs are cool:
// - can inherit currentColor
// - can provide their own default accessibility text - BUT, want to avoid as it's not easily localized
// but:
// - they duplicate DOM, wich is less performant

// TBD: impact of not having inline dimensions on render jumps

// Notes:
// - inline SVG sprite sheets just create shadow DOM, which is actually slower: http://bit.ly/2wBu6iV

const URL_CACHE = {};
const SHOULD_INLINE_CACHE = {};

export default class SVG extends PureComponent {
  static propTypes = {
    src: PropTypes.string.isRequired,

    'aria-label': PropTypes.string.isRequired,
    role: PropTypes.string.isRequired,
    title: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.bool // set to true to take the aria-label
    ]),
    dataQa: PropTypes.string
  }

  static defaultProps = {
    role: 'img' // https://stackoverflow.com/a/4756461/2860787: NVDA, JAWS, and WindowEyes will read the aria-label when the element also contains role="img".
  }

  render() {
    let {src, title, dataQa, ...props} = this.props;

    if (!allSVGs[`./${src}`]) {
      return null;
    }

    const SpecificSVG = allSVGs[`./${src}`].default;

    let shouldInline = false;
    if (SHOULD_INLINE_CACHE.hasOwnProperty(src)) {
      shouldInline = SHOULD_INLINE_CACHE[src];
    }

    let url = URL_CACHE[src];
    if (!shouldInline) {
      if (!url) {
        const svgText = ReactDOMServer.renderToStaticMarkup(<SpecificSVG />);
        shouldInline = SHOULD_INLINE_CACHE[src] = !!svgText.match(/currentColor/i); // there may be more, but starting with this
        invariant(
          !shouldInline || !(svgText.match(/\s(id|class)=/i) || svgText.match(/<style(>|\s)/i)),
          `Inline-prefered SVG ${src} contains non-inlineable features.`
        );
        if (!shouldInline) {
          const blob = new Blob([svgText], { type: 'image/svg+xml' });
          url = URL_CACHE[src] = URL.createObjectURL(blob);
        }
      }
    }

    if (!url) {
      invariant(!title, 'Title not yet supported on inline SVGs.');
      return <SpecificSVG {...props} />;
    }

    if (title === true) {
      title = props['aria-label'];
    }

    return <img src={url} title={title} data-qa={dataQa} {...props} />;
  }
}
