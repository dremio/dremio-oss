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
import {
  ModalContainer,
  DialogContent,
  type ModalContainerProps,
  type DialogContentProps,
  IconButton,
} from "dremio-ui-lib/dist-esm";

type MessageDialogProps = ModalContainerProps &
  DialogContentProps & { expandable: boolean };

export const MessageDialog = (props: MessageDialogProps) => {
  const [expanded, setExpanded] = useState(false);

  //ModalContainer props
  const { isOpen, open, close, expandable, ...dialogContentProps } = props;

  const { toolbar, ...rest } = dialogContentProps;

  return (
    <ModalContainer isOpen={isOpen} open={open} close={close}>
      <DialogContent
        {...rest}
        expanded={expanded}
        toolbar={
          <>
            {toolbar}
            {expandable && (
              <IconButton
                aria-label="Expand"
                onClick={() => setExpanded((prev) => !prev)}
              >
                {/* @ts-ignore */}
                <dremio-icon name="interface/expand" alt=""></dremio-icon>
              </IconButton>
            )}
            <IconButton aria-label="Close" onClick={close}>
              {/* @ts-ignore */}
              <dremio-icon name="interface/close-big" alt=""></dremio-icon>
            </IconButton>
          </>
        }
      />
    </ModalContainer>
  );
};
