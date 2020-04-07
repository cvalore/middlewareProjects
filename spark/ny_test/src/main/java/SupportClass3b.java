import java.util.concurrent.atomic.AtomicReferenceArray;

public class SupportClass3b {
      private AtomicReferenceArray<Integer> accidentsPerBoroughPerWeek;
      private Integer lethalsPerBoroughPerWeek;

      public SupportClass3b() {
      }

      public SupportClass3b(AtomicReferenceArray<Integer> accidentsPerBoroughPerWeek, Integer lethalsPerBoroughPerWeek) {
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
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
