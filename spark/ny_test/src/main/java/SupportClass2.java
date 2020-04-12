public class SupportClass2 {

      //private String factor;
      private int accidents = 0;
      private int lethals = 0;

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

      public void addToAccidents(int accidentsToAdd) {
            this.accidents += accidentsToAdd;
      }

      public void addToLethals(int lethalToAdd) {
            this.lethals += lethalToAdd;
      }
}
