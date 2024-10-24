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
/**
 * Gets session metadata for Intercom. Throws an error if it does not exist.
 */
export const getIntercomSessionMetadata = () => {
  try {
    const user = JSON.parse(localStorage.getItem("user")!);
    return {
      company: {
        id: user.clusterId,
        version: user.version,
        created_at: user.clusterCreatedAt / 1000,
        company_edition: window.dremioConfig?.edition,
      },
      created_at: user.userCreatedAt / 1000,
      name: `${user.firstName} ${user.lastName}`.trim(),
      email: user.email,
      user_id: user.clusterId + (user.email || user.userId),
      edition: window.dremioConfig?.edition,
      widget: {
        activator: "#header-chat-button",
      },
    };
  } catch (e) {
    throw new Error("User session is not available");
  }
};
