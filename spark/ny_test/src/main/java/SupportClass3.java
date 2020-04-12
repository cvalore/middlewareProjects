public class SupportClass3 {
      private int [] accidentsPerBoroughPerWeek;
      private Integer lethalsPerBoroughPerWeek;

      public SupportClass3() {
      }

      public SupportClass3(int[] accidentsPerBoroughPerWeek, Integer lethalsPerBoroughPerWeek) {
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
      }

      public int[] getAccidentsPerBoroughPerWeek() {
            return accidentsPerBoroughPerWeek;
      }

      public void setAccidentsPerBoroughPerWeek(int[] accidentsPerBoroughPerWeek) {
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
      }

      public Integer getLethalsPerBoroughPerWeek() {
            return lethalsPerBoroughPerWeek;
      }

      public void setLethalsPerBoroughPerWeek(Integer lethalsPerBoroughPerWeek) {
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
      }

      public void addAccidentsAt(int index, int num) {
            this.accidentsPerBoroughPerWeek[index] += num;
      }

      public void addLethals(int num) {
            this.lethalsPerBoroughPerWeek += num;
      }
}
