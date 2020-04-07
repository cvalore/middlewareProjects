package rest.image_server.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {
      //TODO
      //VEDERE SE VA BENE PER COSE CONCORRENTI (SE SI DEVE CONSIDERARE)

      //NO THREAD SAFE
      private static Map<String, User> users = new HashMap<>();
      private static Map<String, List<Image>> imagesByUsers = new HashMap<>();

      public static Map<String, User> getUsers() {
            return users;
      }
      public static Map<String, List<Image>> getImagesByUsers() { return imagesByUsers; }
}
