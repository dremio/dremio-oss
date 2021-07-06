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

const theme = {
  props: {
    MuiButtonBase: {
      disableRipple: true
    }
  },
  overrides: {
    MuiSwitch: {
      switchBase: {
        height: 'auto'
      }
    },
    MuiMenuItem: {
      root: {
        fontWeight: 'inherit',
        fontFamily: 'inherit',
        fontSize: 'inherit'
      }
    },
    MuiSelect: {
      root: {
        backgroundColor: '#F4F6F7'
      }
    },
    MuiDialogTitle: {
      root: {
        padding: '0 16px'
      }
    },
    MuiPaper: {
      root: {
        color: 'inherit'
      },
      rounded: {
        borderRadius: 3
      }
    },
    MuiDialog: {
      paperWidthXs: {
        maxWidth: 'unset',
        width: 450,
        height: 200
      },
      paperWidthSm: {
        width: 670,
        height: 360
      },
      paperWidthMd: {
        width: 840,
        height: 480
      },
      paperWidthLg: {
        width: 840,
        height: 600
      },
      paperWidthXl: {
        width: '80%',
        height: 720,
        maxWidth: 1200
      }
    },
    MuiTooltip: {
      tooltip: {
        backgroundColor: '#2A394A',
        fontSize: '12px'
      }
    }
  }
};

export default theme;
