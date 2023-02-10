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

import { useState } from "react";
// @ts-ignore
import ReactDOM from "react-dom";
import { IconButton, Snackbar } from "@mui/material";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import FontIcon from "../Icon/FontIcon";
import { POPUP_ICON_TYPES } from "./popupNotificationUtils";

import "./PopupNotification.less";

const unmountPopup = (anchorClass?: string) => {
  const container = anchorClass
    ? document.querySelector(`.${anchorClass}`)
    : document.querySelector(".popup-notifications");
  if (container != null) {
    ReactDOM.unmountComponentAtNode(container);
  }
};

type PopupNotificationProps = {
  message: string;
  type?: "success" | "error" | "warning" | "default";
  autoClose?: number;
  anchorClass?: string;
};

export const PopupNotification = (props: PopupNotificationProps) => {
  const { autoClose, message, type = "default", anchorClass } = props;
  const [isOpen, setIsOpen] = useState<boolean>(true);

  const handleClose = () => {
    setIsOpen(false);
    unmountPopup(anchorClass);
  };

  return (
    <Snackbar
      className={`popupNotification --${type}`}
      anchorOrigin={{ vertical: "top", horizontal: "center" }}
      open={isOpen}
      autoHideDuration={autoClose}
      onClose={handleClose}
      message={
        <>
          <Tooltip title={type}>
            <dremio-icon
              name={POPUP_ICON_TYPES[type]}
              alt={type}
              class="type-icon"
            />
          </Tooltip>
          {message}
        </>
      }
      action={
        <IconButton
          size="small"
          className="popupNotification__close"
          aria-label="close"
          color="inherit"
          onClick={handleClose}
        >
          <FontIcon type="XBig" />
        </IconButton>
      }
    />
  );
};

export default function openPopupNotification(
  renderProps: PopupNotificationProps
): void {
  const autoClose = renderProps.autoClose ? renderProps.autoClose : 2000;
  const popup = <PopupNotification {...renderProps} autoClose={autoClose} />;
  const anchorElement = renderProps.anchorClass
    ? document.querySelector(`.${renderProps.anchorClass}`)
    : document.querySelector(".popup-notifications");

  ReactDOM.render(popup, anchorElement);

  const timeout = setTimeout(() => unmountPopup(), renderProps.autoClose);

  return clearTimeout(timeout);
}
