import java.util.ArrayList;
import java.util.List;

public class SupportClass2 {

      private String factor;
      private Long accidents;
      private Long lethals;

      public SupportClass2() {
      }

      public SupportClass2(String factor, Long accidents, Long lethals) {
            this.factor = factor;
            this.accidents = accidents;
            this.lethals = lethals;
      }

      public String getFactor() {
            return factor;
      }

      public void setFactor(String factor) {
            this.factor = factor;
      }

      public Long getAccidents() {
            return accidents;
      }

      public void setAccidents(Long accidents) {
            this.accidents = accidents;
      }

      public Long getLethals() {
            return lethals;
      }

      public void setLethals(Long lethals) {
            this.lethals = lethals;
      }
}
