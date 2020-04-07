package rest.image_server.exceptions;

import rest.image_server.model.ErrorMessage;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<GenericException> {
      @Override
      public Response toResponse(GenericException e) {
            ErrorMessage errorMessage = new ErrorMessage(e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "https://restfulapi.net/http-status-codes/");
            return Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(errorMessage)
                        .build();
      }
}
