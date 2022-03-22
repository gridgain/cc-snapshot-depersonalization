package org.gridgain.cc.snapshot.depersonalization;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.configuration.GridGainConfiguration;
import org.gridgain.grid.configuration.SnapshotConfiguration;
import org.gridgain.grid.internal.processors.cache.database.GridSnapshotEx;
import org.gridgain.grid.persistentstore.ListSnapshotParams;
import org.gridgain.grid.persistentstore.RestoreSnapshotParams;
import org.gridgain.grid.persistentstore.SnapshotCreateParams;
import org.gridgain.grid.persistentstore.SnapshotInfo;
import org.gridgain.grid.persistentstore.SnapshotPath;

import static java.util.Collections.singleton;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class SnapshotDepersonalization {
    private static final String DEPERSONALIZATION_OUTPUT_DIR = "DEPERSONALIZATION_OUTPUT_DIRECTORY";
    private static final String DEPERSONALIZATION_INPUT_DIR = "DEPERSONALIZATION_INPUT_DIRECTORY";
    private static final String DEPERSONALIZATION_REPLACEMENT_NEBULA_PRICES = "DEPERSONALIZATION_REPLACEMENT_NEBULA_PRICES";
    private static final String DEPERSONALIZATION_REPLACEMENT_CUSTOMERS = "DEPERSONALIZATION_REPLACEMENT_CUSTOMERS";

    private static final String CUSTOMERS_CACHE = "StripeCustomerMappingCache";
    private static final String NEBULA_CLUSTER_CACHE = "NebulaClusterInfoCache";

    private static final String[] DEPERSONALIZATION_CACHES = new String[] {
        "AccountCache",
        "NotificationConfigurationCache",
        "ResourceMetadataCache",
        "TeamCache"
    };
    private static final String[] MODIFIED_FIELDS = new String[] {
        "email",
        "tag",
        "firstName",
        "lastName",
        "company",
        "phone",
        "fileName",
        "recipients",
        "name"
    };

    private static final Set<Character> VOWELS = new HashSet<>(Arrays.asList('A', 'E', 'I', 'O', 'U'));
    private static final Set<Character> CONSONANTS = new HashSet<>(Arrays.asList('B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'X', 'Z'));

    private static final Random RND = new Random();

    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(48888)
                .setIpFinder(new TcpDiscoveryMulticastIpFinder()
                    .setAddresses(Collections.singleton("localhost:48888"))))
            .setPluginConfigurations(new GridGainConfiguration()
                .setSnapshotConfiguration(new SnapshotConfiguration()
                    .setSnapshotsPath("/tmp")));

        System.out.println("Depersonalization started");

        try (IgniteEx ignite = (IgniteEx)Ignition.start(cfg)) {
            ignite.cluster().state(ACTIVE);

            GridGain gg = ignite.plugin(GridGain.PLUGIN_NAME);
            GridSnapshotEx snapshot = (GridSnapshotEx)gg.snapshot();

            loadSnapshot(snapshot);

            depesonalizeCaches(ignite);
            fixStripeInfo(ignite);

            createNewSnapshot(snapshot);
        }

        System.out.println("Depersonalization finished");
    }

    private static void loadSnapshot(GridSnapshotEx snapshot) {
        System.out.println("Loading an existing snapshot...");

        String dir = IgniteSystemProperties.getString(DEPERSONALIZATION_INPUT_DIR);

        if (dir == null)
            throw new IllegalArgumentException("Not defined input directory. Use a system property " + DEPERSONALIZATION_INPUT_DIR);

        SnapshotPath path = SnapshotPath.file().path(new File(dir)).build();

        SnapshotInfo snap = snapshot.list(new ListSnapshotParams()
                .optionalSearchPaths(singleton(path)))
            .stream()
            .max(Comparator.comparing(SnapshotInfo::snapshotId))
            .orElseThrow(() -> new IllegalArgumentException("Failed to find a snapshot in the directory: " + dir));

        System.out.println("Restoring from snapshot [id=" + snap.snapshotId() + "]...");

        snapshot
            .restore(new RestoreSnapshotParams()
                .snapshotId(snap.snapshotId())
                .forceRestore(true)
                .optionalSearchPaths(singleton(path))
            )
            .get();

        System.out.println("Restored successfully.");
    }

    private static void depesonalizeCaches(IgniteEx ignite) {
        for (String cache : DEPERSONALIZATION_CACHES)
            depesonalizeCache(ignite, cache);
    }

    private static void depesonalizeCache(IgniteEx ignite, String cacheName) {
        IgniteCache<Object, BinaryObject> cache = ignite.cache(cacheName).withKeepBinary();

        if (cache == null)
            return;

        System.out.println("Depersonalizing " + cacheName + "...");

        cache.forEach(entry -> {
            BinaryObject val = entry.getValue();
            BinaryObjectBuilder bldr = val.toBuilder();

            for (String field : MODIFIED_FIELDS) {
                if (val.hasField(field))
                    bldr.setField(field, depesonalizeField((Object)val.field(field)));
            }

            cache.put(entry.getKey(), bldr.build());
        });
    }

    private static Object depesonalizeField(Object val) {
        if (val instanceof Set) {
            return ((Set<String>)val).stream()
                .map(SnapshotDepersonalization::depesonalizeField)
                .collect(Collectors.toSet());
        }

        return depesonalizeField((String)val);
    }

    private static String depesonalizeField(String val) {
        StringBuilder sb = new StringBuilder(val.length());

        val.chars().forEach(ch0 -> {
            char ch = (char)ch0;

            if (Character.isDigit(ch))
                sb.append(1);
            else if (VOWELS.contains(Character.toUpperCase(ch)))
                sb.append('a');
            else if (CONSONANTS.contains(Character.toUpperCase(ch)))
                sb.append('b');
            else
                sb.append(ch);
        });

        return sb.toString();
    }

    private static void fixStripeInfo(IgniteEx ignite) {
        IgniteCache<Object, Object> teamMapping = ignite.cache("TeamStripeSubscriptionMappingCache").withKeepBinary();

        if (teamMapping != null)
            teamMapping.removeAll();

        replaceStripeCustomers(ignite);
        replaceNebulaPrices(ignite);
    }

    private static void replaceStripeCustomers(IgniteEx ignite) {
        String[] customers = readPropertyStrings(DEPERSONALIZATION_REPLACEMENT_CUSTOMERS);

        IgniteCache<Object, BinaryObject> cache = ignite.cache(CUSTOMERS_CACHE).withKeepBinary();

        if (cache == null)
            return;

        System.out.println("Replacing Stripe customers...");

        cache.forEach(entry -> {
            BinaryObject val = entry.getValue();

            val = val.toBuilder()
                .setField("customerId", customers[RND.nextInt(customers.length)])
                .build();

            cache.put(entry.getKey(), val);
        });
    }

    private static void replaceNebulaPrices(IgniteEx ignite) {
        String[] prices = readPropertyStrings(DEPERSONALIZATION_REPLACEMENT_NEBULA_PRICES);

        IgniteCache<Object, BinaryObject> cache = ignite.cache(NEBULA_CLUSTER_CACHE).withKeepBinary();

        if (cache == null)
            return;

        System.out.println("Replacing Nebula prices...");

        cache.forEach(entry -> {
            BinaryObject val = entry.getValue();

            val = val.toBuilder()
                .setField("priceId", prices[RND.nextInt(prices.length)])
                .build();

            cache.put(entry.getKey(), val);
        });
    }

    private static String[] readPropertyStrings(String propName) {
        String s = IgniteSystemProperties.getString(propName);

        if (s == null)
            throw new IllegalArgumentException("Not defined property: " + propName);

        return s.split(",");
    }

    private static void createNewSnapshot(GridSnapshotEx snapshot) {
        System.out.println("Creating a new snapshot...");

        String dir = IgniteSystemProperties.getString(DEPERSONALIZATION_OUTPUT_DIR);

        if (dir == null)
            throw new IllegalArgumentException("Not defined output directory. Use a system property " + DEPERSONALIZATION_OUTPUT_DIR);

        snapshot.createFullSnapshot(
            null,
            new File(dir),
            new SnapshotCreateParams(),
            null
        );
    }
}
