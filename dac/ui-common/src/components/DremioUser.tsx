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

import { Avatar } from "dremio-ui-lib/components";

type DremioUserProps = {
  user: any;
};

const getInitials = (user: {
  email: string;
  firstName: string;
  lastName: string;
}) => {
  if (!user.firstName) {
    return user.email.slice(0, 2);
  }
  if (user.lastName.length) {
    return user.firstName.charAt(0) + user.lastName.charAt(0);
  } else {
    return user.firstName.slice(0, 2);
  }
};

export const DremioUser = (props: DremioUserProps) => {
  return (
    <div className="inline-flex flex-row gap-1 items-center">
      <Avatar initials={getInitials(props.user).toLowerCase()} />{" "}
      {props.user.firstName + " " + props.user.lastName}
    </div>
  );
};
