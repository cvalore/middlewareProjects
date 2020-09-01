import java.io.Serializable;

public class SupportClass2 implements Serializable {

      private int accidents;
      private int lethals;

      public SupportClass2() {
      }

      public SupportClass2(int accidents, int lethals) {
            this.accidents = accidents;
            this.lethals = lethals;
      }

      public int getAccidents() {
            return accidents;
      }

      public void setAccidents(int accidents) {
            this.accidents = accidents;
      }

      public int getLethals() {
            return lethals;
      }

      public void setLethals(int lethals) {
            this.lethals = lethals;
      }
}
