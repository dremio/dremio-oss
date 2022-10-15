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

import sentryUtil from "@app/utils/sentryUtil";
import { useState } from "react";
import CopyButton from "components/Buttons/CopyButton";
import * as classes from "./SupportInfo.module.less";
import clsx from "clsx";
import { intl } from "@app/utils/intl";

/**
 * The most recent sentry event ID is stored in a global variable
 * and can change over time, so this component should save the most
 * recent one that occurred when this component was mounted.
 */
const useEventId = (): string | undefined => {
  const [eventId] = useState(sentryUtil.getEventId());
  return eventId;
};

const renderSupportInfoItem = ({
  title,
  contents,
}: {
  title: string;
  contents: JSX.Element | string;
}): JSX.Element => {
  return (
    <div className={classes["support-info__item"]}>
      <div className={classes["support-info__item-title"]}>
        <span className={classes["support-info__item-title-text"]}>
          {title}
        </span>
        <CopyButton
          className={classes["support-info__item-copy-button"]}
          text={contents}
          title={intl.formatMessage({ id: "Common.Copy" })}
        />
      </div>
      <div
        className={clsx(
          "dremio-typography-monospace",
          classes["support-info__item-contents"]
        )}
      >
        {contents}
      </div>
    </div>
  );
};

export const SupportInfo = () => {
  const eventId = useEventId();
  return (
    <div className={classes["support-info"]}>
      {renderSupportInfoItem({
        title: intl.formatMessage({ id: "Support.session.id" }),
        contents: sentryUtil.sessionUUID,
      })}
      {eventId &&
        renderSupportInfoItem({
          title: intl.formatMessage({ id: "Support.event.id" }),
          contents: eventId,
        })}
    </div>
  );
};
