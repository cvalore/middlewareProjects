package rest.image_server.services;

import rest.image_server.exceptions.DataNotFoundException;
import rest.image_server.model.Database;
import rest.image_server.model.User;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class UserService {
      private Map<String, User> users = Database.getUsers();
      private ImageService imageService = new ImageService();

      public UserService() {
      }

      public List<User> getUsers() {
            return new ArrayList<>(users.values());
      }

      public User getUser(String uuid) {
            User user = users.get(uuid);
            if(user == null) {
                  throw new DataNotFoundException("User with uuid " + uuid + " not found");
            }
            return user;
      }

      public User addUser(User user) {
            String uuid = UUID.randomUUID().toString().split("-")[0];
            user.setUuid(uuid);
            users.put(user.getUuid(), user);
            imageService.addImage(user.getUuid(), null);
            return user;
      }

      public User updateUser(User user) {
            if(user.getUuid().isEmpty())
                  return null;
            users.put(user.getUuid(), user);
            return user;
      }

      public User removeUser(String uuid) {
            /*Delete directory*/
            User user = users.get(uuid);

            if(user != null) {
                  String path = "." + File.separator + "upload_" + uuid + File.separator;
                  File file = new File(path);
                  String[] entries = file.list();
                  for (String s : entries) {
                        File currentFile = new File(file.getPath(), s);
                        currentFile.delete();
                  }
                  file.delete();
            }
            Database.getImagesByUsers().remove(uuid);
            return users.remove(uuid);
      }
}
