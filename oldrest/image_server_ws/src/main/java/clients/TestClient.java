package clients;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestClient {

    public static void main(String[] args) {
        if(args.length < 1 || args[0].isEmpty()) {
            Logger logger = Logger.getAnonymousLogger();
            logger.log(Level.WARNING, "Insert a URI e.g. \"http://localhost:8080/image_server\"");
            return;
        }

        final String webServiceURI = args[0];

        ClientConfig clientConfig = new ClientConfig().register(JacksonFeature.class).register(MultiPartFeature.class);
        Client client = ClientBuilder.newClient(clientConfig);

        URI serviceURI = UriBuilder.fromUri(webServiceURI).build();
        WebTarget webTarget = client.target(serviceURI);

        System.out.println(
                webTarget.path("api").path("users").request()
                .accept(MediaType.APPLICATION_JSON).get(Response.class).toString());

        System.out.println(
                webTarget.path("api").path("users").request()
                        .accept(MediaType.APPLICATION_JSON).get(String.class));

    }
}
