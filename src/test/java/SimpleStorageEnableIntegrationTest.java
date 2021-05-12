import com.google.common.base.Stopwatch;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.ClusterMemberGroupUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Simple integration test example.
 */
public final class SimpleStorageEnableIntegrationTest {
    private static ClusterMemberGroup memberGroup;
    private static Stopwatch stopwatch;

    @BeforeClass
    public static void beforeTests() {
        stopwatch = Stopwatch.createStarted();
        memberGroup = ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(2)
                .buildAndConfigureForStorageDisabledClient();
    }

    @AfterClass
    public static void afterTests() {
        ClusterMemberGroupUtils.shutdownCacheFactoryThenClusterMemberGroups(memberGroup);
        stopwatch.stop();
        System.out.println("Whole run took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Test
    public void simpleExampleCoherence() {
        final NamedCache cache = CacheFactory.getCache("test");
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 1; i <= 1000000; i++) {
            Map<Integer, Integer> m = new HashMap<>();
            m.put(i, i);
            cache.putAll(m);
            if (i % 100000 == 0) {
                System.out.println("Coherence performance is " + i/watch.elapsed(TimeUnit.MILLISECONDS)
                        + "/ms");
            }
        }
        assertEquals(1000000, cache.size());
    }

    @Test
    public void simpleExampleHashMap() {
        final Map<Integer, Integer> cache = new HashMap<>();
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 1; i <= 1000000; i++) {
            cache.put(i, i);
            if (i % 100000 == 0) {
                System.out.println("HashMap performance is " + i/watch.elapsed(TimeUnit.MILLISECONDS)
                        + "/ms");
            }
        }
        assertEquals(1000000, cache.size());
    }
}