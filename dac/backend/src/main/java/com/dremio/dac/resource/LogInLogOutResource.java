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
package com.dremio.dac.resource;

import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.config.DremioConfig;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.usergroup.SessionPermissions;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserService;
import com.google.common.base.Strings;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jwt.JWT;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;

/**
 * API for user log in and log out.
 */
@RestResource
@Path("/login")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LogInLogOutResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogInLogOutResource.class);

  private final DremioConfig dremioConfig;
  private final SabotContext dContext;
  private final UserService userService;
  private final SupportService support;
  private final TokenManager tokenManager;
  private final OptionManager projectOptionManager;

  @Inject
  public LogInLogOutResource(
      DremioConfig dremioConfig,
      SabotContext dContext,
      UserService userService,
      SupportService support,
      TokenManager tokenManager,
      ProjectOptionManager projectOptionManager
  ) {
    this.dremioConfig = dremioConfig;
    this.dContext = dContext;
    this.userService = userService;
    this.support = support;
    this.tokenManager = tokenManager;
    this.projectOptionManager = projectOptionManager;
  }

  @POST
  public Response login(UserLogin userLogin, @Context HttpServletRequest request) {
    try {
      if (userLogin == null || Strings.isNullOrEmpty(userLogin.getUserName()) || Strings.isNullOrEmpty(userLogin.getPassword())) {
        throw new IllegalArgumentException("user name or password cannot be null or empty");
      }

      URI callbackUri = new URI(dremioConfig.getString(DremioConfig.SSO_CALLBACK_URI));
      final String userName = validateUser(callbackUri, userLogin.getUserName());

      User userConfig;
      try {
        userConfig = userService.getUser(userName);
      } catch (UserNotFoundException e) {
        final User u2 = SimpleUser.newBuilder().setUserName(userName).setEmail("example@example.wrong.domain").build();
        userConfig = userService.createUser(u2, "Letmein123"); // @WDP
      }

      final String clientAddress = request.getRemoteAddr();

      // Get a token for this session
      final TokenDetails tokenDetails = tokenManager.createToken(userConfig.getUserName(), clientAddress);

      // Make sure the logged-in user has a home space. If not create one.
      try {
        final NamespaceService ns = dContext.getNamespaceService(SystemUser.SYSTEM_USERNAME);
        CatalogServiceHelper.ensureUserHasHomespace(ns, userConfig.getUserName());
      } catch (NamespaceException ex) {
        logger.error("Failed to make sure the user has home space setup.", ex);
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new GenericErrorMessage(ex.getMessage())).build();
      }

      SessionPermissions perms = new SessionPermissions(
        projectOptionManager.getOption(SupportService.USERS_UPLOAD),
        projectOptionManager.getOption(SupportService.USERS_DOWNLOAD),
        projectOptionManager.getOption(SupportService.USERS_EMAIL),
        projectOptionManager.getOption(SupportService.USERS_CHAT),
        true
      );

      return Response.ok(
          new UserLoginSession(
              tokenDetails.token,
              userConfig.getUserName(),
              userConfig.getFirstName(),
              userConfig.getLastName(),
              tokenDetails.expiresAt,
              userConfig.getEmail(),
              userConfig.getUID().getId(),
              true,
              userConfig.getCreatedAt(),
              support.getClusterId().getIdentity(),
              support.getClusterId().getCreated(),
              DremioVersionInfo.getVersion(),
              perms
              )
          ).build();
    } catch (IllegalArgumentException | URISyntaxException | ParseException | IOException | BadJOSEException | JOSEException e) {
      logger.error("Encountered an issue while authenticating {}", userLogin.getUserName(), e);
      return Response.status(UNAUTHORIZED).entity(new GenericErrorMessage(e.getMessage())).build();
    }
  }

  private String validateUser(URI callback, String code) throws ParseException, IOException, URISyntaxException, BadJOSEException, JOSEException {
    AuthorizationCode authorizationCode = new AuthorizationCode(code);
    AuthorizationGrant codeGrant = new AuthorizationCodeGrant(authorizationCode, callback);

    ClientID clientID = new ClientID(dremioConfig.getString(DremioConfig.SSO_CLIENT_ID));
    Secret clientSecret = new Secret(dremioConfig.getString(DremioConfig.SSO_CLIENT_SECRET));
    ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);

    URI tokenEndpoint = new URI(dremioConfig.getString(DremioConfig.SSO_TOKEN_ENDPOINT));

    TokenRequest tokenRequest = new TokenRequest(tokenEndpoint, clientAuth, codeGrant);

    TokenResponse tokenResponse = OIDCTokenResponseParser.parse(tokenRequest.toHTTPRequest().send());

//        if (! response.indicatesSuccess()) {
    // We got an error response...
//            TokenErrorResponse errorResponse = response.toErrorResponse();
//        }

//        OIDCTokenResponse successResponse = (OIDCTokenResponse)tokenResponse.toSuccessResponse();

// Get the ID and access token, the server may also return a refresh token
    JWT idToken = tokenResponse.toSuccessResponse().getTokens().toOIDCTokens().getIDToken();

    // Create validator for signed ID tokens
    IDTokenValidator validator = new IDTokenValidator(new Issuer(dremioConfig.getString(DremioConfig.SSO_ISSUER)),
      clientID, JWSAlgorithm.RS256, new URL(dremioConfig.getString(DremioConfig.SSO_JWK_SET_URI)));

    IDTokenClaimsSet claims = validator.validate(idToken, null);

    return claims.getStringClaim("name");
  }

  @DELETE
  public void logout(@HeaderParam(HttpHeaders.AUTHORIZATION) String authHeader) {
    tokenManager.invalidateToken(TokenUtils.getToken(authHeader));
  }

  @GET
  @Secured
  /**
   * This method serves for following purposes:
   * 1) Check if current user is still authenticated
   * 2) If any user exists. See {@link com.dremio.dac.server.NoUserFilter}
   */
  public boolean isUserAuthorized() {
    return true;
  }
}
