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

import java.io.IOException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.inject.Inject;
import javax.inject.Provider;

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
import com.dremio.service.users.proto.UID;
import com.dremio.service.users.proto.UserAuth;
import com.dremio.service.users.proto.UserConfig;
import com.dremio.service.users.proto.UserInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * User group service with local kv store.
 */
public class SimpleUserService implements UserService {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleUserService.class);

  private final Function<UserInfo, User> infoConfigTransformer = new Function<UserInfo, User>(){
    @Override
    public User apply(UserInfo input) {
      return fromUserConfig(input.getConfig());
    }};

  @VisibleForTesting
  public static final String USER_STORE = "userGroup";

  private static final SecretKeyFactory secretKey = UserServiceUtils.buildSecretKey();
  private LegacyIndexedStore<UID, UserInfo> userStore;

  // when we call hasAnyUser() we cache the result in here so we never hit the kvStore once the value is true
  private final AtomicBoolean anyUserFound = new AtomicBoolean();

  @Inject
  public SimpleUserService(Provider<LegacyKVStoreProvider> kvStoreProvider) {
    this.userStore = kvStoreProvider.get().getStore(UserGroupStoreBuilder.class);
  }

  private UserInfo findUserByUserName(String userName) {
    final LegacyFindByCondition condition = new LegacyFindByCondition()
      .setCondition(SearchQueryUtils.newTermQuery(UserIndexKeys.NAME, userName))
      .setLimit(1);

    final List<UserInfo> userInfos = Lists.newArrayList(KVUtil.values(userStore.find(condition)));
    return userInfos.size() == 0 ? null : userInfos.get(0);
  }

  @Override
  public User getUser(String userName) throws UserNotFoundException {
    if (SystemUser.SYSTEM_USER.getUserName().equals(userName)) {
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
    final UserInfo userInfo = userStore.get(uid);
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
      throw UserException.validationError()
        .message("User '%s' already exists.", userName)
        .build(logger);
    }
    validatePassword(authKey);
    UserConfig newUser = toUserConfig(userConfig)
      .setUid(new UID(UUID.randomUUID().toString()))
      .setCreatedAt(System.currentTimeMillis())
      .setModifiedAt(userConfig.getCreatedAt())
      .setTag(null);
    UserInfo userInfo = new UserInfo();
    userInfo.setConfig(newUser);
    userInfo.setAuth(buildUserAuth(newUser.getUid(), authKey));
    userStore.put(newUser.getUid(), userInfo);

    // Return the new state
    return fromUserConfig(newUser);
  }

  // Merge existing info about user with new one, except ModifiedAt
  private void merge(UserConfig newConfig, UserConfig oldConfig) {
    newConfig.setUid(oldConfig.getUid());
    if (newConfig.getCreatedAt() == null) {
      newConfig.setCreatedAt(oldConfig.getCreatedAt());
    }
    if (newConfig.getEmail() == null) {
      newConfig.setEmail(oldConfig.getEmail());
    }
    if (newConfig.getFirstName() == null) {
      newConfig.setFirstName(oldConfig.getFirstName());
    }
    if (newConfig.getLastName() == null) {
      newConfig.setLastName(oldConfig.getLastName());
    }
    if (newConfig.getUserName() == null) {
      newConfig.setUserName(oldConfig.getUserName());
    }
    if (newConfig.getGroupMembershipsList() == null) {
      newConfig.setGroupMembershipsList(oldConfig.getGroupMembershipsList());
    }
    if (newConfig.getTag() == null) {
      newConfig.setTag(oldConfig.getTag());
    }
  }

  @Override
  public User updateUser(final User userGroup, final String authKey)
    throws IOException, IllegalArgumentException, UserNotFoundException {
    UserConfig userConfig = toUserConfig(userGroup);
    final String userName = userConfig.getUserName();
    final UserInfo oldUserInfo = findUserByUserName(userName);
    if (oldUserInfo == null) {
      throw new UserNotFoundException(userName);
    }
    merge(userConfig, oldUserInfo.getConfig());
    userConfig.setModifiedAt(System.currentTimeMillis());

    UserInfo newUserInfo = new UserInfo();
    newUserInfo.setConfig(userConfig);

    if(authKey != null){
      validatePassword(authKey);
      newUserInfo.setAuth(buildUserAuth(oldUserInfo.getConfig().getUid(), authKey));
    } else {
      newUserInfo.setAuth(oldUserInfo.getAuth());
    }
    userStore.put(userConfig.getUid(), newUserInfo);

    // Return the new state
    return fromUserConfig(userConfig);
  }

  @Override
  public User updateUserName(final String oldUserName, final String newUserName, final User userGroup, final String authKey)
    throws IOException, IllegalArgumentException, UserNotFoundException {
    final UserInfo oldUserInfo = findUserByUserName(oldUserName);
    if (oldUserInfo == null) {
      throw new UserNotFoundException(oldUserName);
    }
    if (findUserByUserName(newUserName) != null) {
      throw UserException.validationError()
        .message("User '%s' already exists.", newUserName)
        .build(logger);
    }
    UserConfig userConfig = toUserConfig(userGroup);
    if (!userConfig.getUserName().equals(newUserName)) {
      throw new IllegalArgumentException("Usernames do not match " + newUserName + " , " + userConfig.getUserName());
    }
    merge(userConfig, oldUserInfo.getConfig());
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

    userStore.put(info.getConfig().getUid(), info);

    // Return the new state
    return fromUserConfig(userConfig);
  }

  @Override
  public void authenticate(String userName, String password) throws UserLoginException {
    final UserInfo userInfo = findUserByUserName(userName);
    if (userInfo == null) {
      throw new UserLoginException(userName, "Invalid user credentials");
    }
    try {
      UserAuth userAuth = userInfo.getAuth();
      final byte[] authKey = buildUserAuthKey(password, userAuth.getPrefix().toByteArray());
      if (!UserServiceUtils.slowEquals(authKey, userAuth.getAuthKey().toByteArray())) {
        throw new UserLoginException(userName, "Invalid user credentials");
      }
    } catch (InvalidKeySpecException ikse) {
      throw new UserLoginException(userName, "Invalid user credentials");
    }
  }

  @Override
  public Iterable<? extends User> getAllUsers(Integer limit) throws IOException{
    Iterable<? extends User> configs = Iterables.transform(KVUtil.values(userStore.find()), infoConfigTransformer);
    if(limit == null){
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
  public Iterable<? extends User> searchUsers(String searchTerm, String sortColumn, SortOrder order,
                                              Integer limit) throws IOException {
    limit = limit == null ? 10000 : limit;

    if (searchTerm == null || searchTerm.isEmpty()) {
      return getAllUsers(limit);
    }

    final SearchQuery query = SearchQueryUtils.or(
      SearchQueryUtils.newContainsTerm(UserIndexKeys.NAME, searchTerm),
      SearchQueryUtils.newContainsTerm(UserIndexKeys.FIRST_NAME, searchTerm),
      SearchQueryUtils.newContainsTerm(UserIndexKeys.LAST_NAME, searchTerm),
      SearchQueryUtils.newContainsTerm(UserIndexKeys.EMAIL, searchTerm));

    final LegacyFindByCondition conditon = new LegacyFindByCondition()
      .setCondition(query)
      .setLimit(limit)
      .addSorting(buildSorter(sortColumn, order));

    return Lists.transform(Lists.newArrayList(KVUtil.values(userStore.find(conditon))), infoConfigTransformer);
  }

  private static final class UserConverter implements DocumentConverter<UID, UserInfo> {
    @Override
    public void convert(DocumentWriter writer, UID key, UserInfo userInfo) {
      UserConfig userConfig = userInfo.getConfig();
      writer.write(UserIndexKeys.UID, userConfig.getUid().getId());
      writer.write(UserIndexKeys.NAME, userConfig.getUserName());

      writer.write(UserIndexKeys.FIRST_NAME, userConfig.getFirstName());
      writer.write(UserIndexKeys.LAST_NAME, userConfig.getLastName());
      writer.write(UserIndexKeys.EMAIL, userConfig.getEmail());
    }
  }

  private static SearchFieldSorting buildSorter(final String sortColumn, final SortOrder order) {

    if(sortColumn == null){
      return UserServiceUtils.DEFAULT_SORTER;
    }

    final IndexKey key = UserIndexKeys.MAPPING.getKey(sortColumn);

    if(key == null){
      throw UserException
        .functionError()
        .message("Unable to sort by field {}",  sortColumn)
        .build(logger);
    }

    return key.toSortField(order);
  }

  @Override
  public void deleteUser(final String userName, String version) throws UserNotFoundException, IOException {
    final UserInfo info = findUserByUserName(userName);
    if (info != null) {
      userStore.delete(info.getConfig().getUid(), version);
      if (!getAllUsers(1).iterator().hasNext()) {
        anyUserFound.set(false);
      }
    } else {
      throw new UserNotFoundException(userName);
    }
  }

  private UserAuth buildUserAuth(final UID uid, final String authKey) throws IllegalArgumentException {
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

  private byte[] buildUserAuthKey(final String authKey, final byte[] prefix) throws InvalidKeySpecException {
    final PBEKeySpec spec = new PBEKeySpec(authKey.toCharArray(), prefix, 65536, 128);
    return secretKey.generateSecret(spec).getEncoded();
  }

  /**
   * Used only by command line for set-password
   * @param userName username of user whose password is being reset
   * @param password password
   * @throws IllegalArgumentException if user does not exist or password doesn't fit minimum requirements
   */
  public void setPassword(String userName, String password) throws IllegalArgumentException {
    validatePassword(password);

    UserInfo info = findUserByUserName(userName);
    if (info == null) {
      throw new IllegalArgumentException(format("user %s does not exist", userName));
    }

    info.setAuth(buildUserAuth(info.getConfig().getUid(), password));
    userStore.put(info.getConfig().getUid(), info);
  }

  @VisibleForTesting
  public static void validatePassword(String input) throws IllegalArgumentException {
    if (!UserServiceUtils.validatePassword(input)) {
      throw UserException.validationError()
        .message("Invalid password: must be at least 8 letters long, must contain at least one number and one letter")
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
      .setTag(user.getVersion());
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
      .build();
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

    @Override
    public Long getVersion(UserInfo value) {
      return value.getConfig().getVersion();
    }

    @Override
    public void setVersion(UserInfo value, Long version) {
      value.getConfig().setVersion(version);
    }

  }

  /**
   * Class for creating kvstore.
   */
  public static class UserGroupStoreBuilder implements LegacyIndexedStoreCreationFunction<UID, UserInfo> {

    @Override
    public LegacyIndexedStore<UID, UserInfo> build(LegacyStoreBuildingFactory factory) {
      return factory.<UID, UserInfo>newStore()
        .name(USER_STORE)
        .keyFormat(Format.wrapped(UID.class, UID::getId, UID::new, Format.ofString()))
        .valueFormat(Format.ofProtostuff(UserInfo.class))
        .versionExtractor(UserVersionExtractor.class)
        .buildIndexed(new UserConverter());
    }

  }

}
