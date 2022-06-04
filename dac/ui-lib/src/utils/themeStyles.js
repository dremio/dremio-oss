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
        borderRadius: '4px'
      }
    },
    MuiDialog: {
      container: {
        top: '10%',
        bottom: '10%',
        height: 'unset',
        position: 'relative',
        minWidth: 1280
      },
      paper: {
        margin: 'unset'
      },
      paperWidthSm: {
        width: '25%',
        minWidth: 320,
        minHeight: 200
      },
      paperWidthMd: {
        width: '50%',
        minWidth: 640,
        minHeight: 200
      },
      paperWidthLg: {
        width: '75%',
        minWidth: 960,
        minHeight: 200
      },
      paperWidthXl: {
        width: '95%',
        minHeight: 200,
        maxWidth: 1200
      }
    },
    MuiTooltip: {
      arrow: {
        color: '#32383E',
        '&::before': {
          backgroundColor: '#32383E !important'
        }
      },
      tooltip: {
        backgroundColor: '#32383E',
        borderRadius: '2px',
        minHeight: '32px',
        display: 'flex',
        fontWeight: '400',
        fontFamily: 'inherit',
        fontSize: '14px',
        alignItems: 'center',
        justifyContent: 'center'
      }
    },
    MuiMenu: {
      paper: {
        borderRadius: '4px'
      },
      list: {
        paddingTop: '4px',
        paddingBottom: '4px'
      }
    },
    MuiBackdrop: {
      root: {
        backgroundColor: 'rgba(41, 56, 73, 0.5)'
      }
    },
    MuiChip: {
      root: {
        fontSize: 'unset',
        height: 'inherit'
      }
    },
    MuiRadio: {
      root: {
        '&:hover': {
          backgroundColor: '#F4FAFC'
        },
        '&:focus': {
          backgroundColor: '#F4FAFC'
        }
      },
      colorSecondary: {
        '&$checked': {
          color: '#6ECBD9',
          '&:hover': {
            backgroundColor: '#F4FAFC'
          }
        }
      }
    },
    MuiOutlinedInput: {
      root: {
        '&$focused $notchedOutline': {
          borderWidth: 1
        }
      }
    }
  }
};

export default theme;
