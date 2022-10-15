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

type ModalContainerProps = {
  isOpen: boolean;
  open: () => void;
  close: () => void;
};

export const ModalContainer = forwardRef<
  HTMLDialogElement,
  ModalContainerProps
>((props, ref) => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { open, close, isOpen, ...rest } = props;

  return isOpen
    ? createPortal(
        <dialog className="dremio-modal-container" ref={ref} {...rest} />,
        window.document.body
      )
    : null;
});

export const useModalContainer = () => {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    const dialogEl = dialogRef.current;

    if (!dialogEl) {
      return;
    }

    if (!isOpen) {
      dialogEl.close();
    } else {
      dialogEl.showModal();
    }

    return () => {
      dialogEl.close();
    };
  }, [isOpen]);

  return {
    open: useCallback(() => {
      setIsOpen(true);
    }, []),
    close: useCallback(() => {
      setIsOpen(false);
    }, []),
    ref: dialogRef,
    isOpen,
  };
};
