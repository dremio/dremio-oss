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
import { PALE_NAVY, NAVY } from 'uiTheme/radium/colors';
import { body } from './typography';

const arrowWidth = '5px';

const typeToColor = {
  error: '#FEEAEA',
  info: PALE_NAVY,
  status: NAVY
};



export default function getTooltipStyles(type) {
  const bgColor = typeToColor[type];
  return {
    base: {
      ...body,
      position: 'absolute',
      padding: '0 5px',
      paddingRight: 4,
      borderRadius: 1,
      // chris notes: not super happy about zIndex here
      // but these are currently inline in the DOM (not at the end)
      // and they need to be on top of any subsequent stacking context (caused by any reason).
      // Having them inline also avoids a bunch of scrolling sync issues.
      // Note: multiple tooltips in the same context will still respect DOM order
      // e.g. problematic for persistent tooltips like errors
      // (DX-9150 Tooltip appears behind other text)
      zIndex: 1
    },

    inner: {
      padding: '3px 8px',
      textAlign: 'center',
      backgroundColor: bgColor,
      width: 180
    },

    arrow: {
      position: 'absolute',
      width: 0, height: 0,
      borderColor: 'transparent',
      borderStyle: 'solid'
    },
    placement: {
      top: {
        tooltip: {marginTop: -3, padding: `${arrowWidth} 0`},
        arrow: {
          bottom: 0, marginLeft: `-${arrowWidth}`, borderWidth: `${arrowWidth} ${arrowWidth} 0`, borderTopColor: bgColor
        }
      },
      right: {
        tooltip: {marginRight: 3, padding: `0 ${arrowWidth}`},
        arrow: {
          left: 0,
          marginTop: `-${arrowWidth}`, borderWidth: `${arrowWidth} ${arrowWidth} ${arrowWidth} 0`,
          borderRightColor: bgColor
        }
      },
      bottom: {
        tooltip: {marginBottom: 3, padding: `${arrowWidth} 0`},
        arrow: {
          top: 0,
          marginLeft: `-${arrowWidth}`, borderWidth: `0 ${arrowWidth} ${arrowWidth}`,
          borderBottomColor: bgColor
        }
      },
      left: {
        tooltip: {marginLeft: -3, padding: `0 ${arrowWidth}`, left: 0},
        arrow: {
          right: 0,
          marginTop: `-${arrowWidth}`, borderWidth: `${arrowWidth} 0 ${arrowWidth} ${arrowWidth}`,
          borderLeftColor: bgColor
        }
      }
    }
  };
}
