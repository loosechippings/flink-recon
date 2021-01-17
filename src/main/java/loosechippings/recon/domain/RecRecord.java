package loosechippings.recon.domain;

import java.util.Objects;

/* not using a FLink tuple because tuples can't hold nulls
   and we're expecting null values
 */
public class RecRecord {
    Event event;
    Audit audit;

    public RecRecord() {
    }

    public RecRecord(Event event, Audit audit) {
        this.event = event;
        this.audit = audit;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public Audit getAudit() {
        return audit;
    }

    public void setAudit(Audit audit) {
        this.audit = audit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecRecord recRecord = (RecRecord) o;
        return Objects.equals(event, recRecord.event) &&
                Objects.equals(audit, recRecord.audit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, audit);
    }

    @Override
    public String toString() {
        return "RecRecord{" +
                "event=" + event +
                ", audit=" + audit +
                '}';
    }
}
