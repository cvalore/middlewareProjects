package rest.image_server.resources;

import rest.image_server.exceptions.DataNotFoundException;
import rest.image_server.exceptions.GenericException;
import rest.image_server.model.User;
import rest.image_server.services.UserService;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.util.List;

@Path("/users")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserResource {
      private UserService userService = new UserService();

      @GET
      public List<User> getUsers() {
            return userService.getUsers();
      }

      @POST
      public User addUser(User user, @Context UriInfo uriInfo) {
            User newUser = userService.addUser(user);

            /*Create directory*/
            String path = "." + File.separator + "upload_" + newUser.getUuid() + File.separator;

            File file = new File(path);
            boolean created = file.mkdir();
            if(!created) {
                  throw new GenericException("Cannot create " + path + " folder");
            }

            /*Add links*/
            addLinks(uriInfo, newUser);

            return newUser;
      }

      @GET
      @Path("/{user_uuid}")
      public User getUser(@PathParam("user_uuid") String uuid) {
            return userService.getUser(uuid);
      }

      @PUT
      @Path("/{user_uuid}")
      public User updateUser(@PathParam("user_uuid") String uuid, User user, @Context UriInfo uriInfo) {
            if(userService.getUser(uuid) == null) {
                  throw new DataNotFoundException("User with uuid " + uuid + " not found");
            }
            user.setUuid(userService.getUser(uuid).getUuid());
            User updatedUser = userService.updateUser(user);

            addLinks(uriInfo, updatedUser);

            return updatedUser;
      }

      @DELETE
      @Path("/{user_uuid}")
      public User deleteUser(@PathParam("user_uuid") String uuid) {
            return userService.removeUser(uuid);
      }

      private void addLinks(@Context UriInfo uriInfo, User user) {
            user.addLink(getUriForSelf(uriInfo, user), "self");
            user.addLink(getUriForUsers(uriInfo), "users");
            user.addLink(getUriForImages(uriInfo), "images");
            user.addUploadFolderLink(getUriForUploadFolder(uriInfo, user), "upload_folder");
      }

      private String getUriForSelf(UriInfo uriInfo, User user) {
            return uriInfo
                        .getBaseUriBuilder()
                        .path(UserResource.class)
                        .path(user.getUuid())
                        .build()
                        .toString();
      }

      private String getUriForUsers(UriInfo uriInfo) {
            return uriInfo
                        .getBaseUriBuilder()
                        .path(UserResource.class)
                        .build()
                        .toString();
      }

      private String getUriForUploadFolder(UriInfo uriInfo, User user) {
            return uriInfo
                        .getBaseUriBuilder()
                        .path(ImageResource.class)
                        .path(user.getUuid())
                        .build()
                        .toString();
      }

      private String getUriForImages(UriInfo uriInfo) {
            return uriInfo
                        .getBaseUriBuilder()
                        .path(ImageResource.class)
                        .build()
                        .toString();
      }

}
