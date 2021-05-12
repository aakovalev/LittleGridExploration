import java.io.Serializable;
import java.util.UUID;

public class Book implements Serializable {
    UUID id;

    public Book() {
        this.id = UUID.randomUUID();
    }

    @Override
    public String toString() {
        return "BOOK-" + id.toString().substring(0, 8);
    }

    public UUID getId() {
        return id;
    }
}