package image_server.exceptions;

import image_server.model.ErrorMessage;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DataNotFoundExceptionMapper implements ExceptionMapper<DataNotFoundException> {
      @Override
      public Response toResponse(DataNotFoundException e) {
            ErrorMessage errorMessage = new ErrorMessage(e.getMessage(), Response.Status.NOT_FOUND.getStatusCode(), "https://restfulapi.net/http-status-codes/");
            return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(errorMessage)
                        .build();
      }
}
