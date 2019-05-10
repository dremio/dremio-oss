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
import { PALE_NAVY, NAVY } from 'uiTheme/radium/colors';
import { bodyWhite } from 'uiTheme/radium/typography';

export const arrowWidth = '5px';

const typeToColor = {
  error: '#FEEAEA',
  info: PALE_NAVY,
  status: NAVY
};


const baseToolTipStyle = {
  position: 'relative'
};

export default function getTooltipStyles(type) {
  const bgColor = typeToColor[type];
  return {
    base: {
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
      textAlign: 'left',
      backgroundColor: bgColor,
      width: 180,
      borderRadius: 5,
      padding: 10,
      ...(type === 'status' ? bodyWhite : null)
    },

    arrow: {
      position: 'absolute',
      width: 0,
      height: 0,
      borderColor: 'transparent',
      color: 'transparent',
      borderStyle: 'solid'
    },
    placement: {
      top: {
        tooltip: { ...baseToolTipStyle, paddingBottom: arrowWidth },
        arrow: {
          bottom: 0,
          borderWidth: `${arrowWidth} ${arrowWidth} 0`,
          borderTopColor: bgColor
        }
      },
      right: {
        tooltip: { ...baseToolTipStyle, paddingLeft: arrowWidth },
        arrow: {
          left: 0,
          borderWidth: `${arrowWidth} ${arrowWidth} ${arrowWidth} 0`,
          borderRightColor: bgColor
        }
      },
      bottom: {
        tooltip: { ...baseToolTipStyle, paddingTop: arrowWidth },
        arrow: {
          top: 0,
          borderWidth: `0 ${arrowWidth} ${arrowWidth}`,
          borderBottomColor: bgColor
        }
      },
      left: {
        tooltip: { ...baseToolTipStyle, paddingRight: arrowWidth },
        arrow: {
          right: 0,
          borderWidth: `${arrowWidth} 0 ${arrowWidth} ${arrowWidth}`,
          borderLeftColor: bgColor
        }
      }
    }
  };
}
