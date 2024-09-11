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
package com.dremio.service.users;

import static java.lang.String.format;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.KVUtil;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.Service;
import com.dremio.service.users.events.UserDeletionEvent;
import com.dremio.service.users.events.UserServiceEvent;
import com.dremio.service.users.events.UserServiceEventSubscriber;
import com.dremio.service.users.events.UserServiceEventTopic;
import com.dremio.service.users.events.UserServiceEvents;
import com.dremio.service.users.events.UserServiceEventsImpl;
import com.dremio.service.users.proto.UID;
import com.dremio.service.users.proto.UserAuth;
import com.dremio.service.users.proto.UserConfig;
import com.dremio.service.users.proto.UserInfo;
import com.dremio.service.users.proto.UserType;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.protostuff.ByteString;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.inject.Inject;
import javax.inject.Provider;

/** User group service with local kv store. */
public class SimpleUserService implements UserService, Service {
  public static final String LOWER_CASE_INDICES = "lowerCaseIndices";

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SimpleUserService.class);

  private final Function<UserInfo, User> infoConfigTransformer =
      new Function<UserInfo, User>() {
        @Override
        public User apply(UserInfo input) {
          return fromUserConfig(input.getConfig());
        }
      };

  @VisibleForTesting public static final String USER_STORE = "userGroup";

  private static final SecretKeyFactory secretKey = UserServiceUtils.buildSecretKey();
  private final Supplier<LegacyIndexedStore<UID, UserInfo>> userStore;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final UserServiceEvents userServiceEvents;
  private final boolean isMaster;

  // when we call hasAnyUser() we cache the result in here so we never hit the kvStore once the
  // value is true
  private final AtomicBoolean anyUserFound = new AtomicBoolean();

  @Inject
  public SimpleUserService(Provider<LegacyKVStoreProvider> kvStoreProvider, boolean isMaster) {
    this.userStore =
        Suppliers.memoize(() -> kvStoreProvider.get().getStore(UserGroupStoreBuilder.class));
    this.kvStoreProvider = kvStoreProvider;
    this.userServiceEvents = new UserServiceEventsImpl();
    this.isMaster = isMaster;
  }

  public SimpleUserService(Provider<LegacyKVStoreProvider> kvStoreProvider) {
    this(kvStoreProvider, true);
  }

  @Override
  public void start() throws Exception {
    if (isMaster) {
      final ConfigurationStore configurationStore = new ConfigurationStore(kvStoreProvider.get());
      final Optional<ConfigurationEntry> entry =
          Optional.ofNullable(configurationStore.get(LOWER_CASE_INDICES));
      if (!entry.isPresent()) {
        this.addIndexKeyId();
        final ConfigurationEntry newEntry = new ConfigurationEntry();
        // store the timestamp we finished
        newEntry.setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis())));
        configurationStore.put(LOWER_CASE_INDICES, newEntry);
      }
    }
  }

  protected UserInfo findUserByUserName(String userName) {
    final LegacyFindByCondition condition =
        new LegacyFindByCondition()
            .setCondition(
                SearchQueryUtils.newTermQuery(
                    UserIndexKeys.NAME_LOWERCASE, userName.toLowerCase(Locale.ROOT)))
            .setLimit(1);

    final List<UserInfo> userInfos =
        Lists.newArrayList(KVUtil.values(userStore.get().find(condition)));
    return userInfos.size() == 0 ? null : userInfos.get(0);
  }

  @Override
  public User getUser(String userName) throws UserNotFoundException {
    if (SystemUser.isSystemUserName(userName)) {
      return SystemUser.SYSTEM_USER;
    }

    final UserInfo userInfo = findUserByUserName(userName);
    if (userInfo == null) {
      throw new UserNotFoundException(userName);
    }
    return fromUserConfig(userInfo.getConfig());
  }

  @Override
  public User getUser(UID uid) throws UserNotFoundException {
    if (SystemUser.isSystemUID(uid)) {
      return SystemUser.SYSTEM_USER;
    }

    final UserInfo userInfo = userStore.get().get(uid);
    if (userInfo == null) {
      throw new UserNotFoundException(uid);
    }
    return fromUserConfig(userInfo.getConfig());
  }

  @Override
  public User createUser(final User userConfig, final String authKey)
      throws IOException, IllegalArgumentException {
    final String userName = userConfig.getUserName();
    if (findUserByUserName(userName) != null) {
      throw new UserAlreadyExistException(userName);
    }
    validateUsername(userName);
    validatePassword(authKey);
    UserConfig newUser =
        toUserConfig(userConfig)
            .setUid(new UID(UUID.randomUUID().toString()))
            .setCreatedAt(System.currentTimeMillis())
            .setModifiedAt(userConfig.getCreatedAt())
            .setTag(null);
    UserInfo userInfo = new UserInfo();
    userInfo.setConfig(newUser);
    userInfo.setAuth(buildUserAuth(newUser.getUid(), authKey));
    userStore.get().put(newUser.getUid(), userInfo);

    // Return the new state
    return fromUserConfig(newUser);
  }

  /**
   * Merge existing info about user with new one, except ModifiedAt
   *
   * @param newUser a new User in the request
   * @param originalUserConfig original UserConfig saved in the kvstore
   * @return a new UserConfig that merge the newUser and originalUserConfig
   */
  private UserConfig merge(User newUser, UserConfig originalUserConfig) {
    UserConfig updatedUserConfig = toUserConfig(newUser);
    // fields that cannot be updated
    updatedUserConfig
        .setUid(originalUserConfig.getUid())
        .setCreatedAt(originalUserConfig.getCreatedAt());

    if (newUser instanceof SimpleUser) {
      // SimpleUser does not handle type, so maintain the already set type
      updatedUserConfig.setType(originalUserConfig.getType());
    }
    // if fields are blank in the request, use the original info
    if (updatedUserConfig.getEmail() == null) {
      updatedUserConfig.setEmail(originalUserConfig.getEmail());
    }
    if (updatedUserConfig.getFirstName() == null) {
      updatedUserConfig.setFirstName(originalUserConfig.getFirstName());
    }
    if (updatedUserConfig.getLastName() == null) {
      updatedUserConfig.setLastName(originalUserConfig.getLastName());
    }
    if (updatedUserConfig.getUserName() == null) {
      updatedUserConfig.setUserName(originalUserConfig.getUserName());
    }
    if (updatedUserConfig.getGroupMembershipsList() == null) {
      updatedUserConfig.setGroupMembershipsList(originalUserConfig.getGroupMembershipsList());
    }
    if (updatedUserConfig.getTag() == null) {
      updatedUserConfig.setTag(originalUserConfig.getTag());
    }
    if (updatedUserConfig.getExternalId() == null) {
      updatedUserConfig.setExternalId(originalUserConfig.getExternalId());
    }

    // cannot change new user
    return updatedUserConfig;
  }

  @Override
  public User updateUser(final User userGroup, final String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    final UserInfo oldUserInfo = findUserByUserName(userGroup.getUserName());
    return doUpdateUser(oldUserInfo, userGroup, authKey);
  }

  @Override
  public User updateUserById(final User userGroup, final String authKey)
      throws IllegalArgumentException, UserNotFoundException {
    Preconditions.checkNotNull(userGroup.getUID());
    final UserInfo oldUserInfo = userStore.get().get(userGroup.getUID());
    return doUpdateUser(oldUserInfo, userGroup, authKey);
  }

  /**
   * Given existing oldUserInfo, update it with fields from newUser. If a non-null authKey is
   * provided, treat that as an attempted password change.
   */
  protected User doUpdateUser(UserInfo oldUserInfo, User newUser, String authKey)
      throws UserNotFoundException {
    // Handle exception if user was not found
    final String userName = newUser.getUserName();
    if (oldUserInfo == null) {
      if (Strings.isNullOrEmpty(userName)) {
        throw new UserNotFoundException(newUser.getUID());
      } else {
        throw new UserNotFoundException(userName);
      }
    }

    // reject userName changes
    if (!Strings.isNullOrEmpty(userName)
        && !oldUserInfo.getConfig().getUserName().equals(userName)) {
      throw new IllegalArgumentException("Updating username is not supported.");
    }

    // Start creating an updated user config
    final UserConfig updatedUserConfig = merge(newUser, oldUserInfo.getConfig());
    updatedUserConfig.setModifiedAt(System.currentTimeMillis());
    UserInfo newUserInfo = new UserInfo();
    newUserInfo.setConfig(updatedUserConfig);

    if (authKey != null) {
      if (newUserInfo.getConfig().getType() == UserType.REMOTE) {
        throw new IllegalArgumentException("Cannot set password for an external user.");
      }
      validatePassword(authKey);
      newUserInfo.setAuth(buildUserAuth(oldUserInfo.getConfig().getUid(), authKey));
    } else {
      if (newUserInfo.getConfig().getType() == UserType.LOCAL && oldUserInfo.getAuth() == null) {
        throw new IllegalArgumentException("Must set password for local users");
      }
      newUserInfo.setAuth(oldUserInfo.getAuth());
    }
    userStore.get().put(updatedUserConfig.getUid(), newUserInfo);

    // Return the new state
    return fromUserConfig(updatedUserConfig);
  }

  @Override
  public User updateUserName(
      final String oldUserName,
      final String newUserName,
      final User userGroup,
      final String authKey)
      throws IOException, IllegalArgumentException, UserNotFoundException {
    final UserInfo oldUserInfo = findUserByUserName(oldUserName);
    if (oldUserInfo == null) {
      throw new UserNotFoundException(oldUserName);
    }
    final UserInfo newUserInfo = findUserByUserName(newUserName);
    // rename can change the case of the username
    if (newUserInfo != null
        && !newUserInfo.getConfig().getUid().equals(oldUserInfo.getConfig().getUid())) {
      throw UserException.validationError()
          .message("User '%s' already exists.", newUserName)
          .build(logger);
    }
    validateUsername(newUserName);

    UserConfig userConfig = merge(userGroup, oldUserInfo.getConfig());
    if (!userConfig.getUserName().equals(newUserName)) {
      throw new IllegalArgumentException(
          "Usernames do not match " + newUserName + " , " + userConfig.getUserName());
    }
    userConfig.setModifiedAt(System.currentTimeMillis());

    UserInfo info = new UserInfo();
    info.setConfig(userConfig);

    if (authKey != null) {
      validatePassword(authKey);
      info.setAuth(buildUserAuth(userConfig.getUid(), authKey));
    } else {
      // use previous password
      info.setAuth(oldUserInfo.getAuth());
    }

    userStore.get().put(info.getConfig().getUid(), info);

    // Return the new state
    return fromUserConfig(userConfig);
  }

  @Override
  public AuthResult authenticate(String userName, String password) throws UserLoginException {
    final UserInfo userInfo = findUserByUserName(userName);
    if (userInfo == null) {
      logger.debug("UserInfo not found for user: {}", userName);
      throw new UserLoginException(userName, UserServiceUtils.USER_AUTHENTICATION_ERROR_MESSAGE);
    }
    if (!userInfo.getConfig().getActive()) {
      logger.error("User: {} Inactive", userName);
      throw new UserLoginException(userName, UserServiceUtils.USER_AUTHENTICATION_ERROR_MESSAGE);
    }
    if (userInfo.getConfig().getType() != UserType.LOCAL) {
      logger.error("User: {} is not LOCAL", userName);
      throw new UserLoginException(userName, UserServiceUtils.USER_AUTHENTICATION_ERROR_MESSAGE);
    }
    try {
      UserAuth userAuth = userInfo.getAuth();
      final byte[] authKey = buildUserAuthKey(password, userAuth.getPrefix().toByteArray());
      if (!UserServiceUtils.slowEquals(authKey, userAuth.getAuthKey().toByteArray())) {
        throw new UserLoginException(userName, UserServiceUtils.USER_AUTHENTICATION_ERROR_MESSAGE);
      }

      return AuthResult.builder()
          .setUserName(userInfo.getConfig().getUserName())
          .setUserId(userInfo.getConfig().getUid().getId())
          .build();
    } catch (InvalidKeySpecException ikse) {
      throw new UserLoginException(userName, "Invalid user credentials");
    }
  }

  @Override
  public Iterable<? extends User> getAllUsers(Integer limit) throws IOException {
    Iterable<? extends User> configs =
        Iterables.transform(KVUtil.values(userStore.get().find()), infoConfigTransformer);
    if (limit == null) {
      return configs;
    } else {
      return Iterables.limit(configs, limit);
    }
  }

  @Override
  public boolean hasAnyUser() throws IOException {
    if (anyUserFound.get()) {
      return true;
    }
    boolean userFound = getAllUsers(1).iterator().hasNext();
    anyUserFound.set(userFound);
    return userFound;
  }

  @Override
  public Iterable<? extends User> searchUsers(
      String searchTerm, String sortColumn, SortOrder order, Integer limit) throws IOException {
    final SearchQuery query;
    if (Strings.isNullOrEmpty(searchTerm)) {
      query = SearchQueryUtils.newMatchAllQuery();
    } else {
      query =
          SearchQueryUtils.or(
              SearchQueryUtils.newContainsTerm(
                  UserIndexKeys.NAME_LOWERCASE, searchTerm.toLowerCase(Locale.ROOT)),
              SearchQueryUtils.newContainsTerm(
                  UserIndexKeys.FIRST_NAME_LOWERCASE, searchTerm.toLowerCase(Locale.ROOT)),
              SearchQueryUtils.newContainsTerm(
                  UserIndexKeys.LAST_NAME_LOWERCASE, searchTerm.toLowerCase(Locale.ROOT)),
              SearchQueryUtils.newContainsTerm(
                  UserIndexKeys.EMAIL_LOWERCASE, searchTerm.toLowerCase(Locale.ROOT)));
    }
    return searchUsers(query, sortColumn, order, 0, limit);
  }

  @Override
  public Iterable<? extends User> searchUsers(
      SearchQuery searchQuery,
      String sortColumn,
      SortOrder sortOrder,
      Integer startIndex,
      Integer pageSize) {
    LegacyFindByCondition condition = new LegacyFindByCondition();
    if (searchQuery == null) {
      condition.setCondition(SearchQueryUtils.newMatchAllQuery());
    } else {
      condition.setCondition(searchQuery);
    }
    condition.addSorting(buildSorter(sortColumn, sortOrder));
    if (startIndex != null) {
      condition.setOffset(startIndex);
    }
    if (pageSize != null) {
      condition.setPageSize(pageSize);
      condition.setLimit(pageSize);
    }

    return Lists.newArrayList(KVUtil.values(userStore.get().find(condition))).stream()
        .map(infoConfigTransformer)
        .collect(Collectors.toList());
  }

  @Override
  public Integer getNumUsers(SearchQuery searchQuery) {
    if (searchQuery == null) {
      searchQuery = SearchQueryUtils.newMatchAllQuery();
    }
    return userStore.get().getCounts(searchQuery).get(0);
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void subscribe(
      UserServiceEventTopic userServiceEventTopic, UserServiceEventSubscriber subscriber) {
    this.userServiceEvents.subscribe(userServiceEventTopic, subscriber);
  }

  @Override
  public void publish(UserServiceEvent event) {
    this.userServiceEvents.publish(event);
  }

  private static final class UserConverter implements DocumentConverter<UID, UserInfo> {
    private Integer version = 0;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(DocumentWriter writer, UID key, UserInfo userInfo) {
      UserConfig userConfig = userInfo.getConfig();
      writer.write(UserIndexKeys.UID, userConfig.getUid().getId());
      writer.write(UserIndexKeys.NAME, userConfig.getUserName());

      writer.write(UserIndexKeys.FIRST_NAME, userConfig.getFirstName());
      writer.write(UserIndexKeys.LAST_NAME, userConfig.getLastName());
      writer.write(UserIndexKeys.EMAIL, userConfig.getEmail());

      if (!Strings.isNullOrEmpty(userConfig.getExternalId())) {
        writer.write(UserIndexKeys.EXTERNAL_ID, userConfig.getExternalId());
      }

      if (userConfig.getUserName() != null) {
        writer.write(
            UserIndexKeys.NAME_LOWERCASE, userConfig.getUserName().toLowerCase(Locale.ROOT));
      }
      if (userConfig.getFirstName() != null) {
        writer.write(
            UserIndexKeys.FIRST_NAME_LOWERCASE, userConfig.getFirstName().toLowerCase(Locale.ROOT));
      }
      if (userConfig.getLastName() != null) {
        writer.write(
            UserIndexKeys.LAST_NAME_LOWERCASE, userConfig.getLastName().toLowerCase(Locale.ROOT));
      }
      if (userConfig.getEmail() != null) {
        writer.write(UserIndexKeys.EMAIL_LOWERCASE, userConfig.getEmail().toLowerCase(Locale.ROOT));
      }
    }
  }

  private static SearchFieldSorting buildSorter(final String sortColumn, final SortOrder order) {

    if (Strings.isNullOrEmpty(sortColumn)) {
      return UserServiceUtils.DEFAULT_SORTER;
    }

    final IndexKey key = UserIndexKeys.MAPPING.getKey(sortColumn);

    if (key == null) {
      throw UserException.functionError()
          .message("Unable to sort by field %s", sortColumn)
          .build(logger);
    }

    if (order == null) {
      return key.toSortField(SortOrder.ASCENDING);
    } else {
      return key.toSortField(order);
    }
  }

  @Override
  public void deleteUser(final String userName, String version)
      throws UserNotFoundException, IOException {
    final UserInfo info = findUserByUserName(userName);
    if (info != null) {
      deleteUser(info.getConfig().getUid(), version);
    } else {
      throw new UserNotFoundException(userName);
    }
  }

  @Override
  public void deleteUser(final UID uid) throws UserNotFoundException, IOException {
    deleteUser(uid, null);
  }

  @Override
  public void deleteUser(final UID uid, String version) throws UserNotFoundException, IOException {
    final UserInfo userInfo = userStore.get().get(uid);
    if (userInfo == null) {
      throw new UserNotFoundException(uid);
    }

    if (version == null) {
      version = userInfo.getConfig().getTag();
    }

    userStore.get().delete(userInfo.getConfig().getUid(), version);
    publish(new UserDeletionEvent(userInfo.getConfig().getUid().getId()));

    if (!getAllUsers(1).iterator().hasNext()) {
      anyUserFound.set(false);
    }
  }

  protected UserAuth buildUserAuth(final UID uid, final String authKey)
      throws IllegalArgumentException {
    final UserAuth userAuth = new UserAuth();
    final SecureRandom random = new SecureRandom();
    final byte[] prefix = new byte[16];
    // create salt
    random.nextBytes(prefix);
    userAuth.setUid(uid);
    userAuth.setPrefix(ByteString.copyFrom(prefix));
    final PBEKeySpec spec = new PBEKeySpec(authKey.toCharArray(), prefix, 65536, 128);
    try {
      userAuth.setAuthKey(ByteString.copyFrom(secretKey.generateSecret(spec).getEncoded()));
    } catch (InvalidKeySpecException ikse) {
      throw new IllegalArgumentException(ikse.toString());
    }
    return userAuth;
  }

  private byte[] buildUserAuthKey(final String authKey, final byte[] prefix)
      throws InvalidKeySpecException {
    final PBEKeySpec spec = new PBEKeySpec(authKey.toCharArray(), prefix, 65536, 128);
    return secretKey.generateSecret(spec).getEncoded();
  }

  /**
   * Used only by command line for set-password
   *
   * @param userName username of user whose password is being reset
   * @param password password
   * @throws IllegalArgumentException if user does not exist or password doesn't fit minimum
   *     requirements
   */
  public void setPassword(String userName, String password) throws IllegalArgumentException {
    validatePassword(password);

    UserInfo info = findUserByUserName(userName);
    if (info == null) {
      throw new IllegalArgumentException(format("user %s does not exist", userName));
    }

    info.setAuth(buildUserAuth(info.getConfig().getUid(), password));
    userStore.get().put(info.getConfig().getUid(), info);
  }

  @VisibleForTesting
  public static void validatePassword(String input) throws IllegalArgumentException {
    if (!UserServiceUtils.validatePassword(input)) {
      throw UserException.validationError()
          .message(
              "Invalid password: must be at least 8 letters long, must contain at least one number and one letter")
          .build(logger);
    }
  }

  public static void validateUsername(String input) {
    if (!UserServiceUtils.validateUsername(input)) {
      throw UserException.validationError()
          .message("Invalid username: can not contain quotes or colons.")
          .build(logger);
    }
  }

  protected UserConfig toUserConfig(User user) {
    return new UserConfig()
        .setUid(user.getUID())
        .setUserName(user.getUserName())
        .setFirstName(user.getFirstName())
        .setLastName(user.getLastName())
        .setEmail(user.getEmail())
        .setCreatedAt(user.getCreatedAt())
        .setModifiedAt(user.getModifiedAt())
        .setTag(user.getVersion())
        .setActive(user.isActive());
  }

  protected User fromUserConfig(UserConfig userConfig) {
    return SimpleUser.newBuilder()
        .setUID(userConfig.getUid())
        .setUserName(userConfig.getUserName())
        .setFirstName(userConfig.getFirstName())
        .setLastName(userConfig.getLastName())
        .setEmail(userConfig.getEmail())
        .setCreatedAt(userConfig.getCreatedAt())
        .setModifiedAt(userConfig.getModifiedAt())
        .setVersion(userConfig.getTag())
        .setActive(userConfig.getActive())
        .build();
  }

  protected LegacyIndexedStore<UID, UserInfo> getUserStore() {
    return userStore.get();
  }

  @VisibleForTesting
  public void clearHasAnyUser() {
    anyUserFound.set(false);
  }

  /** Add lower-case indices for users */
  private void addIndexKeyId() {
    userStore
        .get()
        .find()
        .forEach(document -> userStore.get().put(document.getKey(), document.getValue()));
  }

  private static final class UserVersionExtractor implements VersionExtractor<UserInfo> {
    @Override
    public String getTag(UserInfo value) {
      return value.getConfig().getTag();
    }

    @Override
    public void setTag(UserInfo value, String tag) {
      value.getConfig().setTag(tag);
    }
  }

  /** Class for creating kvstore. */
  public static class UserGroupStoreBuilder
      implements LegacyIndexedStoreCreationFunction<UID, UserInfo> {

    @Override
    public LegacyIndexedStore<UID, UserInfo> build(LegacyStoreBuildingFactory factory) {
      return factory
          .<UID, UserInfo>newStore()
          .name(USER_STORE)
          .keyFormat(Format.wrapped(UID.class, UID::getId, UID::new, Format.ofString()))
          .valueFormat(Format.ofProtostuff(UserInfo.class))
          .versionExtractor(UserVersionExtractor.class)
          .buildIndexed(new UserConverter());
    }
  }
}
