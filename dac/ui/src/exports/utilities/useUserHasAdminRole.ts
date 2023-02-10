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

import { useEffect, useMemo, useState } from "react";
import { useUsersDetails } from "../providers/useUsersDetails";

export const useUserHasAdminRole = ({ id }: { id: string | null }) => {
  const payload = useMemo(() => {
    const ids = id ? [id] : [];
    return { ids };
  }, [id]);

  const [users, err, status] = useUsersDetails(payload);
  const [hasAdmin, setHasAdmin] = useState(false);

  useEffect(() => {
    if (!id) return;

    const user = users?.get(id);
    if (!user) return;

    const hasAdminRole = !!user.roles.find(
      (role: { name: string; type: string }) =>
        role.name === "ADMIN" && role.type === "SYSTEM"
    );

    setHasAdmin(hasAdminRole);
  }, [users, id]);

  return [hasAdmin, err, status] as const;
};
