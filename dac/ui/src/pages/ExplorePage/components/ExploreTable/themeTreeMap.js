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
const HOVER_COLOR = '#EEEEEE';

import classnames from 'classnames';
import {fixedWidthDefault, fixedWidthBold} from 'uiTheme/radium/typography';

export default function getTheme(canSelect) {
  return {
    scheme: 'default',
    author: 'chris kempson (http://chriskempson.com)',
    base00: '#FFFFFF',
    base01: '#181818',
    base02: '#181818',
    base03: '#181818',
    base04: '#181818',
    base05: '#181818',
    base06: '#181818',
    base07: '#181818',
    base08: '#181818',
    base09: '#0000FF',
    base0A: '#181818',
    base0B: '#008000',
    base0C: '#181818',
    base0D: '#181818',
    base0E: '#181818',
    base0F: '#181818',
    value: ({style}, nodeType, keyPath, hover) => ({
      style: {
        ...style,
        float: 'left',
        marginLeft: '0.25em',
        paddingLeft: 5,
        textIndent: '0',
        backgroundColor: hover && canSelect ? HOVER_COLOR : undefined,
        clear: 'both'
      }
    }),

    valueText: ({style}) => {
      return {
        style: {
          ...fixedWidthDefault,
          ...style
        }
      };
    },

    label: {
      ...fixedWidthBold,
      cursor: canSelect ? 'pointer' : 'default'
    },

    nestedNode: ({style}, keyPath, nodeType, expanded, expandable, hover) => ({
      className: classnames(`${nodeType}-node`, {expanded, expandable: expandable && !expanded}),
      style: {
        ...style,
        float: 'left',
        clear: 'both',
        cursor: canSelect ? 'pointer' : 'default',
        marginLeft: '0.25em',
        backgroundColor: hover && canSelect ? HOVER_COLOR : undefined
      }
    }),

    arrow: () => ({
      style: {
        ...fixedWidthDefault
      }
    })
  };
}


