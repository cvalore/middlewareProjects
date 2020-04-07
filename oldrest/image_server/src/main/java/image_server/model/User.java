package image_server.model;

import javax.json.bind.annotation.JsonbPropertyOrder;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@JsonbPropertyOrder({"name"})
public class User {
      private String uuid;
      private String name;

      private Link uploadFolderLink;
      private List<Link> links = new ArrayList<>();

      public User() {
      }

      public User(String uuid, String name) {
            this.uuid = uuid;
            this.name = name;
      }

      public String getUuid() {
            return uuid;
      }

      public void setUuid(String uuid) {
            this.uuid = uuid;
      }

      public String getName() {
            return name;
      }

      public void setName(String name) {
            this.name = name;
      }

      public List<Link> getLinks() {
            return links;
      }

      public void setLinks(List<Link> links) {
            this.links = links;
      }

      public void addLink(String url, String rel) {
            Link link = new Link();
            link.setLink(url);
            link.setRel(rel);
            links.add(link);
      }

      public Link getUploadFolderLink() {
            return uploadFolderLink;
      }

      public void setUploadFolderLink(Link uploadFolderLink) {
            this.uploadFolderLink = uploadFolderLink;
      }

      public void addUploadFolderLink(String url, String rel) {
            uploadFolderLink = new Link();
            uploadFolderLink.setLink(url);
            uploadFolderLink.setRel(rel);
      }
}
