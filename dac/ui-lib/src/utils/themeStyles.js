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
import { lightBlue, green, orange } from "@mui/material/colors";

// https://mui.com/material-ui/migration/v5-style-changes/#change-default-theme-palette-info-colors
const palleteOverrides = {
  info: {
    main: lightBlue[700],
    light: lightBlue[500],
    dark: lightBlue[900],
  },
  success: {
    main: green[800],
    light: green[500],
    dark: green[900],
  },
  warning: {
    main: "#ED6C02",
    light: orange[500],
    dark: orange[900],
  },
  text: { hint: "rgba(0, 0, 0, 0.38)" },
};

const theme = {
  palette: palleteOverrides,
  components: {
    // https://mui.com/material-ui/migration/v5-component-changes/#update-body-font-size
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          fontSize: "0.875rem",
          lineHeight: 1.43,
          letterSpacing: "0.01071em",
        },
      },
    },
    MuiButtonBase: {
      defaultProps: {
        disableRipple: true,
      },
    },
    MuiButton: {
      defaultProps: {
        disableRipple: true,
      },
    },
    MuiSwitch: {
      styleOverrides: {
        switchBase: {
          height: "auto",
        },
      },
    },
    MuiMenuItem: {
      styleOverrides: {
        root: {
          fontWeight: "inherit",
          fontFamily: "inherit",
          fontSize: "inherit",
        },
      },
    },
    MuiSelect: {
      styleOverrides: {
        root: {
          color: "var(--text--primary)",
          backgroundColor: "#F4F6F7",
        },
      },
    },
    MuiDialogTitle: {
      styleOverrides: {
        root: {
          padding: "0 16px",
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundColor: "var(--fill--popover)",
          color: "inherit",
        },
        rounded: {
          borderRadius: "4px",
        },
      },
    },
    MuiDialog: {
      styleOverrides: {
        container: {
          top: "10%",
          bottom: "10%",
          height: "unset",
          position: "relative",
          minWidth: 1280,
        },
        paper: {
          margin: "unset",
        },
        paperWidthSm: {
          width: "25%",
          minWidth: 320,
          minHeight: 200,
        },
        paperWidthMd: {
          width: "50%",
          minWidth: 640,
          minHeight: 200,
        },
        paperWidthLg: {
          width: "75%",
          minWidth: 960,
          minHeight: 200,
        },
        paperWidthXl: {
          width: "95%",
          minHeight: 200,
          maxWidth: 1200,
        },
      },
    },
    MuiMenu: {
      styleOverrides: {
        paper: {
          borderRadius: "4px",
        },
        list: {
          paddingTop: "4px",
          paddingBottom: "4px",
        },
      },
    },
    MuiBackdrop: {
      styleOverrides: {
        root: {
          backgroundColor: "rgba(41, 56, 73, 0.5)",
        },
        invisible: {
          backgroundColor: "transparent",
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          fontSize: "unset",
          height: "inherit",
        },
      },
    },
    MuiRadio: {
      styleOverrides: {
        root: {
          "&:hover": {
            backgroundColor: "#F4FAFC",
          },
          "&:focus": {
            backgroundColor: "#F4FAFC",
          },
        },
        colorSecondary: {
          "&.Mui-checked": {
            color: "#6ECBD9",
            "&:hover": {
              backgroundColor: "#F4FAFC",
            },
          },
        },
      },
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
            borderWidth: 1,
          },
        },
      },
    },
  },
};

export default theme;
