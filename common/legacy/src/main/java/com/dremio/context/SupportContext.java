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
package com.dremio.context;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;

/** Support context. */
public class SupportContext implements SerializableContext {
  public static final RequestContext.Key<SupportContext> CTX_KEY =
      RequestContext.newKey("support_ctx_key");

  private static final String ROLES_DELIMITER = ",";

  private static final String SUPPORT_TICKET_HEADER_KEY = "x-dremio-support-ticket-key";
  private static final String SUPPORT_EMAIL_HEADER_KEY = "x-dremio-support-email-key";
  private static final String SUPPORT_ROLES_HEADER_KEY = "x-dremio-support-roles-key";

  // Note: This refers to the UserID field held within the UserContext,
  // but this constant only appears if the SupportContext is set.
  public static final String SUPPORT_USER_ID = "$Dremio-Support-Super-Admin-User$";

  public static final String SUPPORT_USER_NAME = "support@dremio.com";

  public enum SupportRole {
    BASIC_SUPPORT_ROLE("basic-support"),
    BILLING_ROLE("billing"),
    ORG_DELETE_ROLE("org-delete"),
    CONSISTENCY_FIXER_ROLE("consistency-fixer"),
    DEBUG_ROLE("debug-role"),
    SUPPORT_KEY_MANAGEMENT_ROLE("support-key-management"),
    SECRET_ROTATION_ROLE("secret-rotation");

    private final String value;

    SupportRole(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static SupportRole fromValue(String value) {
      for (SupportRole role : values()) {
        if (role.value.equals(value)) {
          return role;
        }
      }
      return null;
    }
  }

  private final String ticket;
  private final String email;
  private final String[] roles;

  public SupportContext(String ticket, String email, String[] roles) {
    this.ticket = ticket;
    this.email = email;
    this.roles = Arrays.copyOf(roles, roles.length);
  }

  public String getTicket() {
    return ticket;
  }

  public String getEmail() {
    return email;
  }

  public String[] getRoles() {
    return Arrays.copyOf(roles, roles.length);
  }

  public static boolean isSupportUserWithBasicSupportRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.BASIC_SUPPORT_ROLE);
  }

  public static boolean isSupportUserWithSupportKeyManagementRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.SUPPORT_KEY_MANAGEMENT_ROLE);
  }

  public static boolean isSupportUserWithBillingRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.BILLING_ROLE);
  }

  public static boolean isSupportUserWithOrgDeleteRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.ORG_DELETE_ROLE);
  }

  public static boolean isSupportUserWithConsistencyFixerRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.CONSISTENCY_FIXER_ROLE);
  }

  public static boolean isSupportUserWithDebugRole() {
    return isSupportUser() && isSupportUserHasRole(SupportRole.DEBUG_ROLE);
  }

  public static boolean doesSupportUserHaveRole(SupportContext supportContext, SupportRole role) {
    return supportContext.roles.length > 0
        && Arrays.stream(supportContext.roles).anyMatch(r -> r.equals(role.value));
  }

  public static boolean isSupportUser() {
    return RequestContext.current().get(SupportContext.CTX_KEY) != null
        && RequestContext.current().get(UserContext.CTX_KEY) != null
        && SupportContext.SUPPORT_USER_ID.equals(
            RequestContext.current().get(UserContext.CTX_KEY).getUserId());
  }

  public static boolean isSupportUserHasRole(SupportRole role) {
    // assumes that the user is support user
    return doesSupportUserHaveRole(RequestContext.current().get(SupportContext.CTX_KEY), role);
  }

  @Override
  public void serialize(ImmutableMap.Builder<String, String> builder) {
    builder.put(SUPPORT_TICKET_HEADER_KEY, ticket);
    builder.put(SUPPORT_EMAIL_HEADER_KEY, email);
    builder.put(SUPPORT_ROLES_HEADER_KEY, serializeSupportRoles(roles));
  }

  private static String serializeSupportRoles(String[] rolesArr) {
    return rolesArr != null ? String.join(ROLES_DELIMITER, rolesArr) : "";
  }

  public static class Transformer implements SerializableContextTransformer {
    @Override
    public RequestContext deserialize(final Map<String, String> headers, RequestContext builder) {
      if (headers.containsKey(SUPPORT_TICKET_HEADER_KEY)
          && headers.containsKey(SUPPORT_EMAIL_HEADER_KEY)
          && headers.containsKey(SUPPORT_ROLES_HEADER_KEY)) {
        return builder.with(
            SupportContext.CTX_KEY,
            new SupportContext(
                headers.get(SUPPORT_TICKET_HEADER_KEY),
                headers.get(SUPPORT_EMAIL_HEADER_KEY),
                deserializeSupportRoles(headers.get(SUPPORT_ROLES_HEADER_KEY))));
      }

      return builder;
    }
  }

  private static String[] deserializeSupportRoles(String rolesStr) {
    return rolesStr != null ? rolesStr.split(ROLES_DELIMITER) : new String[0];
  }
}
