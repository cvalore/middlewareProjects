package rest.image_server.model;

import javax.json.bind.annotation.JsonbPropertyOrder;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@JsonbPropertyOrder({"title"})
public class Image {
      private String uuid;
      private String title;
      //private String userName;    //not used otherwise when the user changes it thorugh PUT
                                    //all the images need to be changed

      private List<Link> links = new ArrayList<>();

      public Image() {
      }

      public Image(String uuid, String title) {
            this.uuid = uuid;
            this.title = title;
            //this.userName = userName;
      }

      public Image(String title) {
            this.title = title;
      }

      public String getUuid() {
            return uuid;
      }

      public void setUuid(String uuid) {
            this.uuid = uuid;
      }

      public String getTitle() {
            return title;
      }

      public void setTitle(String title) {
            this.title = title;
      }

      public List<Link> getLinks() {
            return links;
      }

      public void setLinks(List<Link> links) {
            this.links = links;
      }

      /*public String getUserName() {
            return userName;
      }*/

      /*public void setUserName(String userName) {
            this.userName = userName;
      }*/

      public void addLink(String url, String rel) {
            Link link = new Link();
            link.setLink(url);
            link.setRel(rel);
            links.add(link);
      }
}
