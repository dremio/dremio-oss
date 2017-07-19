/*
 * Copyright Dremio Corporation 2015
 */
package com.dremio.dac.server;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import java.security.AccessControlException;
import java.util.ConcurrentModificationException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;

import org.glassfish.jersey.internal.inject.ExtractorException;
import org.glassfish.jersey.server.ParamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.dac.model.common.DACUnauthorizedException;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.errors.NewDatasetQueryException;
import com.dremio.dac.service.errors.NotFoundException;
import com.dremio.dac.service.errors.ResourceExistsException;
import com.dremio.service.users.UserNotFoundException;

/**
 * Exception display mapper for rest server.
 */
class GenericExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

  private boolean sendStackTraceToClient;

  public GenericExceptionMapper() {
    this(false);
  }

  public GenericExceptionMapper(boolean sendStackTraceToClient) {
    this.sendStackTraceToClient = sendStackTraceToClient;
  }

  @Context
  private UriInfo uriInfo;

  @Context
  private Request request;

  @Override
  public Response toResponse(Throwable throwable) {
    String[] stackTrace = null;
    if (sendStackTraceToClient) {
      stackTrace = GenericErrorMessage.printStackTrace(throwable);
    }

    return handleException(throwable, stackTrace);
  }

  private Response handleException(Throwable throwable, String[] stackTrace) {
      // standard status already decided
    if (throwable instanceof WebApplicationException) {
      WebApplicationException wae = (WebApplicationException) throwable;
      final String errorMessage;
      if (wae instanceof ParamException) {
        errorMessage = getParamExceptionErrorMessage((ParamException) wae);
      } else {
        errorMessage = wae.toString();
      }
      logger.debug("Status {} for {} {} : {}", wae.getResponse().getStatus(), request.getMethod(), uriInfo.getRequestUri(), errorMessage, wae);
      return Response.fromResponse(wae.getResponse())
          .entity(new GenericErrorMessage(errorMessage, stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof NewDatasetQueryException) {
      logger.debug("Initial attempt to preview new query failed {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      NewDatasetQueryException exception = (NewDatasetQueryException) throwable;
      return Response.status(BAD_REQUEST)
        .entity(new ApiErrorModel("NEW_DATASET_QUERY_EXCEPTION", exception.getMessage(), stackTrace, exception.getDetails()))
        .type(APPLICATION_JSON_TYPE)
        .build();
    }

    if (throwable instanceof NotFoundException) {
      logger.debug("Not Found for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      NotFoundException exception = (NotFoundException) throwable;
      return Response.status(NOT_FOUND)
          .entity(new GenericErrorMessage(exception.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof UserNotFoundException) {
      logger.info("Not Found for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      return Response.status(NOT_FOUND)
          .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof AccessControlException) {
      logger.info("Not Found for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      return Response.status(FORBIDDEN)
          .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof IllegalArgumentException) { // TODO: Move to ClientErrorException
       // bad input => 400
      logger.debug("Bad input for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      return Response.status(BAD_REQUEST)
          .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof ConcurrentModificationException) {
      logger.debug("Conflict for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      return Response.status(CONFLICT)
          .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof ClientErrorException) {
      ClientErrorException e = (ClientErrorException) throwable;
      // bad input => 400
     logger.debug("Bad input for " + request.getMethod() + " " + uriInfo.getRequestUri() + " : " + e, e);
     return Response.status(BAD_REQUEST)
         .entity(new ValidationErrorMessage(e.getMessage(), e.getMessages(), stackTrace))
         .type(APPLICATION_JSON_TYPE)
         .build();
    }  // or ServerErrorException

    if (throwable instanceof DACUnauthorizedException) {
      logger.debug("Not Found for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);

      return Response.status(UNAUTHORIZED)
          .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    if (throwable instanceof ResourceExistsException) {
      logger.debug("Conflict for {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
      final ResourceExistsException exception = (ResourceExistsException) throwable;
      return Response.status(CONFLICT)
          .entity(new GenericErrorMessage(exception.getMessage(), stackTrace))
          .type(APPLICATION_JSON_TYPE)
          .build();
    }

    // bug! => 500
    logger.error("Unexpected exception when processing {} {} : {}", request.getMethod(), uriInfo.getRequestUri(), throwable.toString(), throwable);
    return Response.status(INTERNAL_SERVER_ERROR)
        .entity(new GenericErrorMessage(throwable.getMessage(), stackTrace))
        .type(APPLICATION_JSON_TYPE)
        .build();

  }

  private String getParamExceptionErrorMessage(ParamException pe) {
    Throwable cause = pe.getCause();
    if (cause instanceof ExtractorException && cause.getCause() != null) {
      // ExtractorException does not have any extra info
      cause = cause.getCause();
    }

    final String causeMessage =
        (cause != null)
        ? " " + cause.getMessage()
        : "";

    return pe.getDefaultStringValue() != null
        ? String.format("%s: %s %s (default: %s).%s",
            pe.getMessage(), // generic status message
            pe.getParameterType().getSimpleName(),
            pe.getParameterName(), // which param is wrong
            pe.getDefaultStringValue(), // if it has a default
            causeMessage)
        : String.format("%s: %s %s.%s",
            pe.getMessage(), // generic status message
            pe.getParameterType().getSimpleName(),
            pe.getParameterName(), // which param is wrong
            causeMessage);// the underlying problem
  }
}
