/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.plugins.s3.store.copy;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URLDecoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

import com.dremio.common.collections.Tuple;

/**
 * (Copied from Hadoop 2.8.3, move to using Hadoop version once hadoop version in MapR profile is upgraded to 2.8.3+)
 *
 * Utility methods for S3A code.
 */
public final class S3AUtils {
  private static final Logger logger = LoggerFactory.getLogger(S3AUtils.class);

  static final String CONSTRUCTOR_EXCEPTION = "constructor exception";
  static final String INSTANTIATION_EXCEPTION = "instantiation exception";
  static final String NOT_AWS_PROVIDER = "does not implement AWSCredentialsProvider";
  static final String ABSTRACT_PROVIDER = "is abstract and therefore cannot be created";

  static final String PLUS_WARNING =
      "Secret key contains a special character that should be URL encoded! " +
          "Attempting to resolve...";

  static final String PLUS_UNENCODED = "+";
  static final String PLUS_ENCODED = "%2B";

  /**
   * Create the AWS credentials from the providers and the URI.
   * @param binding Binding URI, may contain user:pass login details
   * @param conf filesystem configuration
   * @return a credentials provider list
   * @throws IOException Problems loading the providers (including reading
   * secrets from credential files).
   */
  public static AWSCredentialProviderList createAWSCredentialProviderSet(
      URI binding, Configuration conf) throws IOException {
    AWSCredentialProviderList credentials = new AWSCredentialProviderList();

    Class<?>[] awsClasses;
    try {
      awsClasses = conf.getClasses(S3Constants.AWS_CREDENTIALS_PROVIDER);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + S3Constants.AWS_CREDENTIALS_PROVIDER +
          ' ' + c, c);
    }
    if (awsClasses.length == 0) {
      Tuple<String, String> creds = getAWSAccessKeys(binding, conf);
      credentials.add(new BasicAWSCredentialsProvider(creds.first, creds.second));
      credentials.add(new EnvironmentVariableCredentialsProvider());
      credentials.add(SharedInstanceProfileCredentialsProvider.getInstance());
    } else {
      for (Class<?> aClass : awsClasses) {
        if (aClass == InstanceProfileCredentialsProvider.class) {
          logger.debug("Found {}, but will use {} instead.", aClass.getName(),
              SharedInstanceProfileCredentialsProvider.class.getName());
          aClass = SharedInstanceProfileCredentialsProvider.class;
        }
        credentials.add(createAWSCredentialProvider(conf, aClass));
      }
    }
    // make sure the logging message strips out any auth details
    logger.debug("For URI {}, using credentials {}", binding, credentials);
    return credentials;
  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    com.amazonaws.auth.AWSCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param conf configuration
   * @param credClass credential class
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  static AWSCredentialsProvider createAWSCredentialProvider(
      Configuration conf, Class<?> credClass) throws IOException {
    AWSCredentialsProvider credentials = null;
    String className = credClass.getName();
    if (!AWSCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IOException("Class " + credClass + " " + NOT_AWS_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IOException("Class " + credClass + " " + ABSTRACT_PROVIDER);
    }
    logger.debug("Credential provider class is {}", className);

    try {
      // new X(conf)
      Constructor cons = getConstructor(credClass, Configuration.class);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance(conf);
        return credentials;
      }

      // X.getInstance()
      Method factory = getFactoryMethod(credClass, AWSCredentialsProvider.class,
          "getInstance");
      if (factory != null) {
        credentials = (AWSCredentialsProvider)factory.invoke(null);
        return credentials;
      }

      // new X()
      cons = getConstructor(credClass);
      if (cons != null) {
        credentials = (AWSCredentialsProvider)cons.newInstance();
        return credentials;
      }

      // no supported constructor or factory method found
      throw new IOException(String.format("%s " + CONSTRUCTOR_EXCEPTION
          + ".  A class specified in %s must provide a public constructor "
          + "accepting Configuration, or a public factory method named "
          + "getInstance that accepts no arguments, or a public default "
          + "constructor.", className, S3Constants.AWS_CREDENTIALS_PROVIDER));
    } catch (ReflectiveOperationException | IllegalArgumentException e) {
      // supported constructor or factory method found, but the call failed
      throw new IOException(className + " " + INSTANTIATION_EXCEPTION +".", e);
    }
  }

  /**
   * Return the access key and secret for S3 API use.
   * Credentials may exist in configuration, within credential providers
   * or indicated in the UserInfo of the name URI param.
   * @param name the URI for which we need the access keys.
   * @param conf the Configuration object to interrogate for keys.
   * @return AWSAccessKeys
   * @throws IOException problems retrieving passwords from KMS.
   */
  public static Tuple<String, String> getAWSAccessKeys(URI name,
      Configuration conf) throws IOException {
    Tuple<String, String> creds = extractLoginDetails(name);
    String accessKey = getPassword(conf, Constants.ACCESS_KEY, creds.first);
    String secretKey = getPassword(conf, Constants.SECRET_KEY, creds.second);
    return Tuple.of(accessKey, secretKey);
  }


  /**
   * Extract the login details from a URI.
   * @param name URI of the filesystem
   * @return a login tuple, possibly empty.
   */
  public static Tuple<String, String> extractLoginDetails(URI name) {
    try {
      String authority = name.getAuthority();
      if (authority == null) {
        return Tuple.of("", "");
      }
      int loginIndex = authority.indexOf('@');
      if (loginIndex < 0) {
        // no login
        return Tuple.of("", "");
      }
      String login = authority.substring(0, loginIndex);
      int loginSplit = login.indexOf(':');
      if (loginSplit > 0) {
        String user = login.substring(0, loginSplit);
        String encodedPassword = login.substring(loginSplit + 1);
        if (encodedPassword.contains(PLUS_UNENCODED)) {
          logger.warn(PLUS_WARNING);
          encodedPassword = encodedPassword.replaceAll("\\" + PLUS_UNENCODED, PLUS_ENCODED);
        }
        String password = URLDecoder.decode(encodedPassword, "UTF-8");
        return Tuple.of(user, password);
      } else if (loginSplit == 0) {
        // there is no user, just a password. In this case, there's no login
        return Tuple.of("", "");
      } else {
        return Tuple.of(login, "");
      }
    } catch (UnsupportedEncodingException e) {
      // this should never happen; translate it if it does.
      throw new RuntimeException(e);
    }
  }

  /**
   * Get a password from a configuration, or, if a value is passed in,
   * pick that up instead.
   * @param conf configuration
   * @param key key to look up
   * @param val current value: if non empty this is used instead of
   * querying the configuration.
   * @return a password or "".
   * @throws IOException on any problem
   */
  static String getPassword(Configuration conf, String key, String val) throws IOException {
    return StringUtils.isEmpty(val) ? lookupPassword(conf, key, "") : val;
  }

  /**
   * Get a password from a configuration/configured credential providers.
   * @param conf configuration
   * @param key key to look up
   * @param defVal value to return if there is no password
   * @return a password or the value in {@code defVal}
   * @throws IOException on any problem
   */
  static String lookupPassword(Configuration conf, String key, String defVal) throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ? new String(pass).trim() : defVal;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }

  /**
   * Returns the public constructor of {@code cl} specified by the list of
   * {@code args} or {@code null} if {@code cl} has no public constructor that
   * matches that specification.
   * @param cl class
   * @param args constructor argument types
   * @return constructor or null
   */
  private static Constructor<?> getConstructor(Class<?> cl, Class<?>... args) {
    try {
      Constructor cons = cl.getDeclaredConstructor(args);
      return Modifier.isPublic(cons.getModifiers()) ? cons : null;
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Returns the public static method of {@code cl} that accepts no arguments
   * and returns {@code returnType} specified by {@code methodName} or
   * {@code null} if {@code cl} has no public static method that matches that
   * specification.
   * @param cl class
   * @param returnType return type
   * @param methodName method name
   * @return method or null
   */
  private static Method getFactoryMethod(Class<?> cl, Class<?> returnType, String methodName) {
    try {
      Method m = cl.getDeclaredMethod(methodName);
      if (Modifier.isPublic(m.getModifiers()) &&
          Modifier.isStatic(m.getModifiers()) &&
          returnType.isAssignableFrom(m.getReturnType())) {
        return m;
      } else {
        return null;
      }
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }
}
