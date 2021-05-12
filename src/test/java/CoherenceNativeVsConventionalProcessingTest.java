import com.google.common.base.Stopwatch;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.aggregator.DoubleSum;
import com.tangosol.util.aggregator.GroupAggregator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CoherenceNativeVsConventionalProcessingTest {
    private static final int MAX_BOOKS = 20;
    private static final int MAX_TRADES = 5000;
    private static ClusterMemberGroup memberGroup;
    private static Stopwatch stopwatch;

    private static NamedCache<UUID, Trade> TRADES;
    private static NamedCache<UUID, Book> BOOKS;
    private static Random RND = new Random();

    @BeforeClass
    public static void beforeTests() {
        stopwatch = Stopwatch.createStarted();
        memberGroup = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(5)
                .buildAndConfigureForStorageDisabledClient();
        populateTestData();
    }

    @AfterClass
    public static void afterTests() {
        ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup);
        stopwatch.stop();
        System.out.println("Whole run took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void conventionalProcessing() {
        long timeToProcess = System.currentTimeMillis();
        // calculate totalNotional for whole trade population
        double totalNotional = 0;
        for (Trade trade: TRADES.values()) {
            totalNotional += trade.notional;
        }
        System.out.println("Total notional: " + totalNotional);

        // calculate contribution of each book into totalNotional in percents
        for (Book book: BOOKS.values()) {
            double notional = 0;
            for (Trade trade: TRADES.values()) {
                if (book.id.equals(trade.bookId)) {
                    notional += trade.notional;
                }
            }
            displayBookNotional(book.toString(), notional/totalNotional*100);
        }
        System.out.println("Processed time: " + (System.currentTimeMillis() - timeToProcess) + " ms" );
    }

    @Test
    public void coherenceNativeProcessing() {
        long timeToProcess = System.currentTimeMillis();
        double totalNotional = TRADES.aggregate(new DoubleSum<>("getNotional"));
        System.out.println("Total notional: " + totalNotional);
        GroupAggregator notionalGroupByBookId =
                GroupAggregator.createInstance("getBookId", new DoubleSum<Trade>("getNotional"));
        Map<UUID, Double> results = (Map<UUID, Double>) TRADES.aggregate(notionalGroupByBookId);
        for (UUID bookId: results.keySet()) {
            double relativeBookNotional = results.get(bookId) / totalNotional * 100;
            displayBookNotional(BOOKS.get(bookId).toString(), relativeBookNotional);
        }
        System.out.println("Time to processed: " + (System.currentTimeMillis() - timeToProcess) + " ms");
    }

    private void displayBookNotional(String bookName, double notional) {
        DecimalFormat fmt = new DecimalFormat("#.##");
        System.out.println(bookName + " notional: "
                + fmt.format(notional) + "%");
    }

    private static void populateTestData() {
        System.out.println("Generating test data...");
        populateBooks();
        populateTrades();
        System.out.println("Test data is generated.");
    }

    private static void populateTrades() {
        TRADES = CacheFactory.getCache("trades");
        for (Book book: BOOKS.values()) {
            int tradeCount = Math.abs(RND.nextInt(MAX_TRADES));
            for (int i = 0; i < tradeCount; i++) {
               Trade trade = new Trade(book.id, Math.abs(RND.nextDouble()));
               TRADES.put(trade.id, trade);
            }
        }
    }

    private static void populateBooks() {
        BOOKS = CacheFactory.getCache("books");
        for (int i = 0; i < MAX_BOOKS; i++) {
            Book book = new Book();
            BOOKS.put(book.id, book);
        }
    }
}