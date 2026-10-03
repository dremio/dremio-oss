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
package com.dremio.plugins.icebergcatalog.store;

import com.dremio.common.exceptions.UserException;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;

/**
 * Maps exceptions raised by an Iceberg REST catalog client into {@link UserException}s with a clear
 * message and a hint.
 *
 * <p>The mapping is generic: it only depends on the Iceberg exception types (which the REST client
 * derives from the HTTP status) and on the server message. Catalog specific knowledge (for example
 * Apache Polaris configuration names) only appears in hint text.
 *
 * <p>Exceptions that callers already translate into Dremio catalog exceptions (for example {@link
 * NoSuchNamespaceException} for folders, or {@code CommitFailedException} which the Iceberg commit
 * path turns into a concurrent modification error) are deliberately not handled here.
 */
final class RestCatalogExceptionMapper {

  static final String DROP_WITH_PURGE_HINT =
      "The catalog refused to drop the entity because dropping requires a purge that the catalog"
          + " does not allow. Ask the catalog administrator to allow drop with purge (for Apache"
          + " Polaris: set the catalog property 'polaris.config.drop-with-purge.enabled' to 'true').";

  static final String MISSING_PRIVILEGE_HINT =
      "Check the privileges granted to the principal configured in the source credentials (for"
          + " Apache Polaris: the grants of the catalog roles assigned to the principal role used in"
          + " the 'scope' property).";

  static final String NOT_AUTHORIZED_HINT =
      "Check the 'credential' (or 'token') catalog credential and the 'oauth2-server-uri' catalog"
          + " property. The source state check replaces a client whose session the catalog no"
          + " longer accepts (for example after the catalog restarted), so retry in a minute.";

  static final String RETRY_HINT =
      "Check the health and logs of the catalog service (and of any proxy in front of it), then"
          + " retry the query.";

  static final String TIMEOUT_HINT =
      "Check that the catalog service is up and responsive, then retry the query. The client"
          + " timeouts can be set with the catalog properties 'rest.client.connection-timeout-ms'"
          + " and 'rest.client.socket-timeout-ms'.";

  static final String UNREACHABLE_HINT =
      "Check the endpoint URI, the network connectivity and that the catalog service is up, then"
          + " retry the query.";

  static final String TLS_HINT =
      "Check that the endpoint scheme (http or https) matches the catalog service and that its"
          + " certificate is trusted by the Dremio JVM (javax.net.ssl.trustStore).";

  static final String RETRIED_NOTE =
      "The client already retried the request up to 'rest.client.max-retries' times (catalog"
          + " property, 5 by default).";

  static final String CONNECTION_RETRIED_NOTE =
      "The client retries most such failures (for example a connection reset or closed by a proxy)"
          + " up to 'rest.client.max-retries' times (catalog property, 5 by default).";

  static final String UNCLASSIFIED_STATUS_NOTE =
      "The HTTP status is not 400, 401, 403, 404, 500 or 503: for example 429 (Too Many Requests)"
          + " when the catalog limits the request rate, or 502/504 from a proxy. 429, 502, 503 and"
          + " 504 are retried up to 'rest.client.max-retries' times (catalog property, 5 by"
          + " default) before the request fails. Wait a moment and retry; if it persists, lower the"
          + " request rate or check the logs of the catalog service and of any proxy in front of"
          + " it.";

  private static final Pattern HTML_TAG = Pattern.compile("<[^<>]{1,200}>");

  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  /** Maximum length of the server-provided part of an error message. */
  private static final int MAX_DETAIL_LENGTH = 300;

  private RestCatalogExceptionMapper() {}

  /**
   * Same as {@link #forbidden(ForbiddenException, String, Object, UnaryOperator)}, no redaction.
   */
  @VisibleForTesting
  static UserException forbidden(ForbiddenException e, String action, Object entity) {
    return forbidden(e, action, entity, UnaryOperator.identity());
  }

  /**
   * Maps an HTTP 403 from the REST catalog to a permission error.
   *
   * @param e the exception thrown by the REST client
   * @param action what Dremio tried to do, e.g. "drop view"
   * @param entity the entity, e.g. "[ns1.v1]"
   * @param redactor masks the source's secret values in server-provided text: the server message is
   *     copied into the error, the job profile and the logs
   */
  static UserException forbidden(
      ForbiddenException e, String action, Object entity, UnaryOperator<String> redactor) {
    String serverMessage = redactedServerMessage(e, redactor);
    String hint = isPurgeRelated(serverMessage) ? DROP_WITH_PURGE_HINT : MISSING_PRIVILEGE_HINT;
    return UserException.permissionError(redactedCause(e, redactor))
        .message(
            "The Iceberg REST catalog denied the request to %s %s: %s %s",
            action, entity, asSentence(serverMessage), hint)
        .addContext("Action", action + " " + entity)
        .buildSilently();
  }

  /**
   * Same as {@link #notAuthorized(NotAuthorizedException, String, Object, UnaryOperator)}, no
   * redaction.
   */
  @VisibleForTesting
  static UserException notAuthorized(NotAuthorizedException e, String action, Object entity) {
    return notAuthorized(e, action, entity, UnaryOperator.identity());
  }

  /**
   * Maps an HTTP 401 from the REST catalog during an operation (the session token was rejected) to
   * a permission error. See {@link #forbidden(ForbiddenException, String, Object, UnaryOperator)}
   * for the parameters.
   */
  static UserException notAuthorized(
      NotAuthorizedException e, String action, Object entity, UnaryOperator<String> redactor) {
    return UserException.permissionError(redactedCause(e, redactor))
        .message(
            "The Iceberg REST catalog rejected the credentials of the request to %s %s: %s %s",
            action, entity, asSentence(redactedServerMessage(e, redactor)), NOT_AUTHORIZED_HINT)
        .addContext("Action", action + " " + entity)
        .buildSilently();
  }

  /** Same as {@link #requestFailed(RESTException, String, Object, UnaryOperator)}, no redaction. */
  @VisibleForTesting
  static UserException requestFailed(RESTException e, String action, Object entity) {
    return requestFailed(e, action, entity, UnaryOperator.identity());
  }

  /**
   * Maps a failed REST catalog request that is not about the request itself to a connection error
   * with the likely cause and retry advice, in the style of the source state messages: HTTP 500
   * ({@link ServiceFailureException}), HTTP 503 after the client retries ({@link
   * ServiceUnavailableException}), timeouts, network and TLS failures, and every HTTP status that
   * the Iceberg REST client does not classify (for example 429, 502 and 504, which it reports as a
   * plain {@link RESTException} without the status code). A request that the catalog rejects as
   * invalid (HTTP 400 {@link BadRequestException}, HTTP 422 {@link UnprocessableEntityException},
   * both {@link RESTException}s too) is a validation error instead. See {@link
   * #forbidden(ForbiddenException, String, Object, UnaryOperator)} for the parameters.
   */
  static UserException requestFailed(
      RESTException e, String action, Object entity, UnaryOperator<String> redactor) {
    if (isRejectedRequest(e)) {
      // The catalog rejected the request itself (HTTP 400 or 422): retrying does not help.
      return rejectedRequest(
          UserException.validationError(redactedCause(e, redactor)), e, action, entity, redactor);
    }
    IOException ioFailure = findCause(e, IOException.class);
    String prefix;
    String hint;
    if (findNetworkCause(e) != null) {
      prefix = "Unable to reach the Iceberg REST catalog for the request to";
      hint = UNREACHABLE_HINT;
    } else if (ioFailure instanceof SSLException) {
      prefix = "The TLS connection to the Iceberg REST catalog failed for the request to";
      hint = TLS_HINT;
    } else if (ioFailure instanceof InterruptedIOException) {
      prefix = "The Iceberg REST catalog did not respond in time to the request to";
      hint = TIMEOUT_HINT;
    } else if (ioFailure != null) {
      prefix = "The connection to the Iceberg REST catalog failed during the request to";
      hint = CONNECTION_RETRIED_NOTE + " " + RETRY_HINT;
    } else if (e instanceof ServiceFailureException) {
      prefix = "The Iceberg REST catalog reported a server error (HTTP 500) for the request to";
      hint = RETRY_HINT;
    } else if (e instanceof ServiceUnavailableException) {
      prefix = "The Iceberg REST catalog is unavailable (HTTP 503) for the request to";
      hint = RETRIED_NOTE + " " + RETRY_HINT;
    } else {
      prefix = "The Iceberg REST catalog returned an unexpected HTTP error for the request to";
      hint = UNCLASSIFIED_STATUS_NOTE;
    }
    String detail = redactedServerMessage(e, redactor);
    if (ioFailure != null) {
      detail = detail + " (" + redactedServerMessage(ioFailure, redactor) + ")";
    }
    return UserException.connectionError(redactedCause(e, redactor))
        .message("%s %s %s: %s %s", prefix, action, entity, asSentence(detail), hint)
        .addContext("Action", action + " " + entity)
        .buildSilently();
  }

  /**
   * Same as {@link #requestFailed(RESTException, String, Object, UnaryOperator)}, except that a
   * request the catalog rejects as invalid (HTTP 400 or 422) is an unsupported operation error
   * instead of a validation error. For drops: {@code DROP TABLE IF EXISTS} reports any validation
   * error as "not found" ({@code DropTableHandler}), which would hide that the catalog refused to
   * drop an existing table.
   */
  static UserException dropFailed(
      RESTException e, String action, Object entity, UnaryOperator<String> redactor) {
    if (isRejectedRequest(e)) {
      return rejectedRequest(
          UserException.unsupportedError(redactedCause(e, redactor)), e, action, entity, redactor);
    }
    return requestFailed(e, action, entity, redactor);
  }

  /** Same as {@link #dropFailed(RESTException, String, Object, UnaryOperator)}, no redaction. */
  @VisibleForTesting
  static UserException dropFailed(RESTException e, String action, Object entity) {
    return dropFailed(e, action, entity, UnaryOperator.identity());
  }

  private static boolean isRejectedRequest(RESTException e) {
    return e instanceof BadRequestException || e instanceof UnprocessableEntityException;
  }

  private static UserException rejectedRequest(
      UserException.Builder builder,
      RESTException e,
      String action,
      Object entity,
      UnaryOperator<String> redactor) {
    return builder
        .message(
            "The Iceberg REST catalog rejected the request to %s %s as invalid (HTTP %d): %s",
            action,
            entity,
            e instanceof BadRequestException ? 400 : 422,
            asSentence(redactedServerMessage(e, redactor)))
        .addContext("Action", action + " " + entity)
        .buildSilently();
  }

  /**
   * The server-provided message of {@code e}, redacted and abbreviated to one line. Redacted before
   * abbreviating (so that a cut cannot leave part of a secret behind) and after.
   */
  static String redactedServerMessage(Throwable e, UnaryOperator<String> redactor) {
    String detail = redactor.apply(abbreviateDetail(redactor.apply(serverMessage(e))));
    // e.g. a message made of HTML markup only
    return detail.isEmpty() ? e.getClass().getSimpleName() : detail;
  }

  /**
   * The exception to pass to a log statement that prints its stack trace: {@code e} itself when no
   * message in its cause chain contains a secret value, otherwise an exception with the redacted
   * message of {@code e} (prefixed with its class name) and its stack trace, without the causes.
   */
  static Throwable redactedForLogging(Throwable e, UnaryOperator<String> redactor) {
    boolean containsSecret = false;
    Throwable t = e;
    for (int depth = 0; t != null && depth < 20 && !containsSecret; depth++) {
      String message = t.getMessage();
      containsSecret = message != null && !message.equals(redactor.apply(message));
      t = t.getCause();
    }
    if (!containsSecret) {
      return e;
    }
    String message = e.getMessage();
    RuntimeException copy =
        new RuntimeException(
            e.getClass().getName() + ": " + (message == null ? "" : redactor.apply(message)));
    copy.setStackTrace(e.getStackTrace());
    return copy;
  }

  /**
   * Makes a server-provided error text fit for a one-line message: drops HTML markup (e.g. the
   * error page of a proxy), collapses whitespace and cuts it to {@link #MAX_DETAIL_LENGTH}
   * characters. Callers redact secrets before (so that a cut cannot leave part of a secret behind)
   * and after.
   */
  static String abbreviateDetail(String detail) {
    String text = HTML_TAG.matcher(detail).replaceAll(" ");
    text = WHITESPACE.matcher(text).replaceAll(" ").trim();
    if (text.length() > MAX_DETAIL_LENGTH) {
      text = text.substring(0, MAX_DETAIL_LENGTH).trim() + "...";
    }
    return text;
  }

  /**
   * The exception to keep as the cause of a mapped error. The cause is printed with the error (job
   * profile, server.log), so when its server message contains a secret value it is replaced by an
   * exception of the same type with the redacted message and the original stack trace.
   */
  private static RuntimeException redactedCause(
      RuntimeException e, UnaryOperator<String> redactor) {
    String message = e.getMessage();
    String redacted = message == null ? null : redactor.apply(message);
    if (Objects.equals(message, redacted)) {
      return e;
    }
    RuntimeException copy;
    if (e instanceof NotAuthorizedException) {
      copy = new NotAuthorizedException("%s", redacted);
    } else if (e instanceof ForbiddenException) {
      copy = new ForbiddenException("%s", redacted);
    } else if (e instanceof ServiceFailureException) {
      copy = new ServiceFailureException(e.getCause(), "%s", redacted);
    } else if (e instanceof ServiceUnavailableException) {
      copy = new ServiceUnavailableException(e.getCause(), "%s", redacted);
    } else {
      copy = new RESTException(e.getCause(), "%s", redacted);
    }
    copy.setStackTrace(e.getStackTrace());
    return copy;
  }

  @Nullable
  private static Throwable findNetworkCause(Throwable failure) {
    Throwable t = failure;
    for (int depth = 0; t != null && depth < 20; depth++) {
      if (t instanceof ConnectException
          || t instanceof UnknownHostException
          || t instanceof NoRouteToHostException) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  @Nullable
  private static <T extends Throwable> T findCause(Throwable failure, Class<T> type) {
    Throwable t = failure;
    for (int depth = 0; t != null && depth < 20; depth++) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
      t = t.getCause();
    }
    return null;
  }

  /** Returns true when the REST catalog reports that a namespace is not empty with an HTTP 400. */
  static boolean isNamespaceNotEmpty(BadRequestException e) {
    String message = e.getMessage();
    return message != null && message.toLowerCase(Locale.ROOT).contains("not empty");
  }

  /**
   * Maps a namespace that cannot be dropped because it still has content to a validation error.
   * Covers both {@link NamespaceNotEmptyException} (HTTP 409, the REST spec) and an HTTP 400 whose
   * message says the namespace is not empty (Apache Polaris 1.x).
   */
  static UserException namespaceNotEmpty(RuntimeException e, Namespace namespace) {
    return UserException.validationError(e)
        .message(
            "Folder [%s] cannot be deleted because it is not empty. Drop the tables, views and"
                + " folders inside it first.",
            namespace)
        .buildSilently();
  }

  /** Maps a missing parent namespace on table creation to a validation error. */
  static UserException parentNamespaceNotFound(
      NoSuchNamespaceException e, TableIdentifier identifier) {
    return UserException.validationError(e)
        .message(
            "Cannot create [%s]: folder [%s] does not exist. Create the folder first.",
            identifier, identifier.namespace())
        .buildSilently();
  }

  /** Maps a missing parent namespace on folder (namespace) creation to a validation error. */
  static UserException parentNamespaceNotFound(NoSuchNamespaceException e, Namespace namespace) {
    Namespace parent =
        namespace.length() > 1
            ? Namespace.of(Arrays.copyOf(namespace.levels(), namespace.length() - 1))
            : Namespace.empty();
    return UserException.validationError(e)
        .message(
            "Cannot create folder [%s]: parent folder [%s] does not exist. Create the parent"
                + " folder first.",
            namespace, parent)
        .buildSilently();
  }

  /** Maps a name conflict on creation to a validation error. */
  static UserException alreadyExists(AlreadyExistsException e, TableIdentifier identifier) {
    return UserException.validationError(e)
        .message("A table or view with given name [%s] already exists.", identifier)
        .buildSilently();
  }

  /**
   * True when the 403 is about purging on drop rather than about a missing privilege. Matches the
   * Apache Polaris message ("Unable to purge entity ... drop-with-purge.enabled") but not privilege
   * names such as {@code DROP_TABLE_WITHOUT_PURGE}.
   */
  @VisibleForTesting
  static boolean isPurgeRelated(String message) {
    String lower = message.toLowerCase(Locale.ROOT);
    return lower.contains("unable to purge")
        || lower.contains("drop-with-purge")
        || lower.contains("drop_with_purge");
  }

  private static String asSentence(String message) {
    char last = message.charAt(message.length() - 1);
    return last == '.' || last == '!' || last == '?' ? message : message + ".";
  }

  private static String serverMessage(Throwable e) {
    String message = e.getMessage();
    return message == null || message.isBlank() ? e.getClass().getSimpleName() : message.trim();
  }
}
