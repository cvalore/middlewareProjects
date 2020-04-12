import java.util.concurrent.atomic.AtomicReferenceArray;

public class SupportClass3 {
      private String borough;
      private AtomicReferenceArray<Integer> accidentsPerBoroughPerWeek;
      private Integer lethalsPerBoroughPerWeek;

      public SupportClass3() {
      }

      public SupportClass3(String borough) {
            this.borough = borough;
      }

      public String getBorough() {
            return borough;
      }

      public void setBorough(String borough) {
            this.borough = borough;
      }

      public AtomicReferenceArray<Integer> getAccidentsPerBoroughPerWeek() {
            return accidentsPerBoroughPerWeek;
      }

      public void setAccidentsPerBoroughPerWeek(AtomicReferenceArray<Integer> accidentsPerBoroughPerWeek) {
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
      }

      public Integer getLethalsPerBoroughPerWeek() {
            return lethalsPerBoroughPerWeek;
      }

      public void setLethalsPerBoroughPerWeek(Integer lethalsPerBoroughPerWeek) {
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
      }
}
