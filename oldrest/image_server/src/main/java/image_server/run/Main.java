package image_server.run;

import image_server.model.Database;
import image_server.model.User;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.File;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
      public static final String BASE_URI = "http://localhost:8080/image_server/api/";

      public static HttpServer startServer() {
            final ResourceConfig rc = new ResourceConfig()
                        .packages("image_server")
                        .register(new LoggingFeature(
                                    Logger.getLogger(LoggingFeature.DEFAULT_LOGGER_NAME),
                                    Level.INFO,
                                    LoggingFeature.Verbosity.PAYLOAD_TEXT,
                                    4096))
                        .register(MultiPartFeature.class)
                        .register(JacksonFeature.class)
                        ;
            return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
      }

      public static void main(String[] args) throws Exception {
            final HttpServer server = startServer();
            System.out.printf("Jersey app started, WADL available at " +
                        "%sapplication.wadl\nHit enter to stop it..", BASE_URI);
            System.in.read();
            server.stop();

            //Delete folders
            for(User user : Database.getUsers().values()) {
                  String path = "." + File.separator + "upload_" + user.getUuid() + File.separator;
                  File file = new File(path);
                  String[] entries = file.list();
                  for (String s : entries) {
                        File currentFile = new File(file.getPath(), s);
                        currentFile.delete();
                  }
                  file.delete();
            }
      }
}
