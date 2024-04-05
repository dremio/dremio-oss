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

import * as React from "react";
import { createPortal } from "react-dom";
import { forwardRef, useCallback, useEffect, useRef, useState } from "react";
import mergeRefs from "react-merge-refs";
import clsx from "clsx";
import dialogPolyfill from "dialog-polyfill";

export type DrawerProps = {
  isOpen: boolean;
  open: () => void;
  close: () => void;
  children: JSX.Element | JSX.Element[];
  className?: string;
};

export const Drawer = forwardRef<HTMLDialogElement, DrawerProps>(
  (props, ref) => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { open, close, isOpen, className, ...rest } = props;
    const dialogRef = useRef<HTMLDialogElement>(null);

    useEffect(() => {
      const dialogEl = dialogRef.current;
      if (!dialogEl) {
        return;
      }
      dialogPolyfill.registerDialog(dialogEl);
    }, [isOpen]);

    useEffect(() => {
      if (!isOpen) {
        dialogRef.current?.close();
      } else {
        dialogRef.current?.show();
      }
    }, [isOpen]);

    return isOpen
      ? createPortal(
          <dialog
            className={clsx("dremio-drawer", className)}
            ref={mergeRefs([ref, dialogRef])}
            {...rest}
          />,
          window.document.body,
        )
      : null;
  },
);

export const useDrawer = () => {
  const [isOpen, setIsOpen] = useState(false);

  return {
    open: useCallback(() => {
      setIsOpen(true);
    }, []),
    close: useCallback(() => {
      setIsOpen(false);
    }, []),
    isOpen,
  };
};
