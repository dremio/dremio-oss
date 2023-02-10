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
import { useEffect, useRef, useState } from "react";
import { useIntl } from "react-intl";
//@ts-ignore
import { IconButton } from "dremio-ui-lib";
import Keys from "@app/constants/Keys.json";
import { DialogContent, ModalContainer } from "dremio-ui-lib/dist-esm";

import * as classes from "./JobProfileModal.module.less";

import clsx from "clsx";

type JobProfileModalProps = {
  isOpen: boolean;
  hide: () => void;
  profileUrl: string;
};

export default function JobProfileModal({
  profileUrl,
  hide,
  isOpen,
}: JobProfileModalProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const urlRef = useRef(profileUrl);
  const [isOpenState, setIsOpenState] = useState(isOpen);
  const { formatMessage } = useIntl();
  const registerEscape = () => {
    const iframe = iframeRef.current;
    if (!iframe || !iframe.contentWindow) return;
    // DX-5720: when focus is in Profile modal, `esc` doesn't close it
    try {
      iframe.contentWindow.addEventListener("keydown", (evt) => {
        if (evt.keyCode === Keys.ESCAPE) {
          // todo: keyCode deprecated, but no well-supported replacement yet
          hide();
        }
      });
    } catch (error) {
      // if the iframe content fails to load, suppress the cross-origin frame access error
      console.error(error);
    }
  };

  useEffect(() => {
    setIsOpenState(isOpen);
  }, [isOpen]);

  return (
    <ModalContainer isOpen={isOpenState} close={hide} open={() => {}}>
      <DialogContent
        className={clsx(
          "dremio-dialog-content--no-overflow",
          classes["job-profile-content"]
        )}
        title={formatMessage({ id: "TopPanel.Profile" })}
        toolbar={
          <IconButton aria-label="Close" onClick={hide}>
            <dremio-icon name="interface/close-small" />
          </IconButton>
        }
      >
        <iframe
          id="profile_frame"
          src={urlRef.current}
          style={{ height: "100%", width: "100%", border: "none" }}
          ref={iframeRef}
          onLoad={registerEscape}
        />
      </DialogContent>
    </ModalContainer>
  );
}
