import java.io.Serializable;

public class SupportClass3a implements Serializable {

      private int index;
      private int lethal;

      public SupportClass3a(int index, int lethal) {
            this.index = index;
            this.lethal = lethal;
      }

      public Integer getIndex() {
            return index;
      }

      public void setIndex(Integer index) {
            this.index = index;
      }

      public int getLethal() {
            return lethal;
      }

      public void setLethal(int lethal) {
            this.lethal = lethal;
      }
}
