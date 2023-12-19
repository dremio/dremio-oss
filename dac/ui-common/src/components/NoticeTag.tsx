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

import clsx from "clsx";
import { useEffect, useState, type ReactNode } from "react";

type NoticeTagProps = {
  inProgressMessage: ReactNode;
  completedMessage: ReactNode;
  inProgress: boolean;
  className?: string;
  hideDelay: number;
};
export const NoticeTag = (props: NoticeTagProps) => {
  const {
    inProgressMessage,
    completedMessage,
    className,
    inProgress,
    hideDelay,
  } = props;
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (inProgress) {
      setVisible(true);
    } else {
      const timerId = setTimeout(() => {
        setVisible(false);
      }, hideDelay);

      return () => {
        setVisible(false);
        clearTimeout(timerId);
      };
    }
  }, [inProgress, hideDelay]);

  if (!visible) {
    return;
  }

  return (
    <div
      className={clsx(
        className,
        "bg-neutral-25 dremio-typography-less-important px-1 py-05 rounded-md"
      )}
      style={{ whiteSpace: "nowrap" }}
    >
      {inProgress ? inProgressMessage : completedMessage}
    </div>
  );
};
