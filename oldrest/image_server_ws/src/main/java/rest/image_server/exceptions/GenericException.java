package rest.image_server.exceptions;

public class GenericException extends RuntimeException {

      private static final long serialVersionUID = -5135559531537368768L;

      public GenericException(String message) {
            super(message);
      }
}
