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

import { Button } from "dremio-ui-lib/components";
import { getIntlContext } from "../contexts/IntlContext";

type EndTutorialDialogProps = {
  onCancel: () => void;
  onAccept: () => void;
};

export const EndTutorialDialog = (props: EndTutorialDialogProps) => {
  const { t } = getIntlContext();
  return (
    <div
      className="bg-brand-600 rounded p-2 leading-normal"
      style={{ maxWidth: "357px" }}
    >
      <p className="text-semibold mb-05 text-base">
        {t("Tutorials.EndTutorial.Title")}
      </p>
      <p>
        {t("Tutorials.EndTutorial.Description")} <br />
        <svg
          className="mt-1 mb-2"
          width="64"
          height="64"
          viewBox="0 0 64 64"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
        >
          <rect width="64" height="64" fill="#2A394A" />
          <rect width="64" height="64" fill="#2A394A" />
          <path
            fillRule="evenodd"
            clipRule="evenodd"
            d="M25.25 22.25C23.4551 22.25 22 23.7051 22 25.5V32.5635C22.1795 32.2391 22.4047 31.9384 22.6716 31.6716C22.9209 31.4222 23.1998 31.2092 23.5 31.0359V25.5C23.5 24.5335 24.2835 23.75 25.25 23.75H38.75C39.7165 23.75 40.5 24.5335 40.5 25.5V35C40.5 35.9665 39.7165 36.75 38.75 36.75H38.5V34.95C38.5 34.8395 38.4105 34.75 38.3 34.75H32.7C32.5895 34.75 32.5 34.8395 32.5 34.95V36.75H29.4844C30.1906 37.0438 30.7715 37.5777 31.1254 38.25H38.75C40.5449 38.25 42 36.7949 42 35V25.5C42 23.7051 40.5449 22.25 38.75 22.25H25.25ZM28 34.5C28 35.163 27.7366 35.7989 27.2678 36.2678C26.7989 36.7366 26.163 37 25.5 37C24.837 37 24.2011 36.7366 23.7322 36.2678C23.2634 35.7989 23 35.163 23 34.5C23 33.837 23.2634 33.2011 23.7322 32.7322C24.2011 32.2634 24.837 32 25.5 32C26.163 32 26.7989 32.2634 27.2678 32.7322C27.7366 33.2011 28 33.837 28 34.5ZM30 39.875C30 41.431 28.714 43 25.5 43C22.286 43 21 41.437 21 39.875V39.772C21 38.792 21.794 38 22.773 38H28.227C29.207 38 30 38.793 30 39.772V39.875Z"
            fill="white"
          />
        </svg>
      </p>
      <div className="flex flex-row items-center">
        <Button
          variant="tertiary"
          className="text-sm"
          onClick={props.onCancel}
          style={{
            marginInlineStart: "calc(var(--scale-2) * -1.9)",
            color: "#16E3EE",
          }}
        >
          Cancel
        </Button>
        <div className="dremio-button-group ml-auto">
          <Button variant="primary" onClick={props.onAccept}>
            Got it
          </Button>
        </div>
      </div>
    </div>
  );
};
