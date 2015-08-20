package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class PercentilesHelperTest {
    @Test
    public void testApproxPercentiles() {
        final PercentilesHelper.ApproximatePercentilesImpl percentiles = new PercentilesHelper.ApproximatePercentilesImpl("test", ImmutableList.<Percentile>of(
            new InternalPercentile(10, 4),
            new InternalPercentile(30, 10.5)));

        assertEquals(percentiles.percentile(0), 0.0);
        assertEquals(percentiles.percentile(5), 2.0);
        assertEquals(percentiles.percentile(10), 4.0);
        assertEquals(percentiles.percentile(15), 5.625);
        assertEquals(percentiles.percentile(30), 10.5);
        assertEquals(percentiles.percentile(100), 10.5);
    }
}
