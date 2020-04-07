package image_server.exceptions;

public class DataNotFoundException extends RuntimeException {

      private static final long serialVersionUID = -4459591120265685974L;

      public DataNotFoundException(String message) {
            super(message);
      }
}
