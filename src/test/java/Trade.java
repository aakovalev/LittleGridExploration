import com.tangosol.net.cache.KeyAssociation;

import java.io.Serializable;
import java.util.UUID;

public class Trade implements KeyAssociation<UUID>, Serializable {
    UUID id;
    UUID bookId;
    double notional;

    public Trade(UUID bookId, double notional) {
        this.bookId = bookId;
        this.notional = notional;
        this.id = UUID.randomUUID();
    }

    public double getNotional() {
        return notional;
    }

    public UUID getBookId() {
        return bookId;
    }

    @Override
    public UUID getAssociatedKey() {
        return getBookId();
    }
}