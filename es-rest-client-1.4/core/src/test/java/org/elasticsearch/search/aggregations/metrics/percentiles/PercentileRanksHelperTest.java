package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PercentileRanksHelperTest {
    @Test
    public void testApproxPercentiles() {
        final PercentileRanksHelper.ApproximatePercentileRanksImpl percentileRanks = new PercentileRanksHelper.ApproximatePercentileRanksImpl("test", ImmutableList.<Percentile>of(
            new InternalPercentile(10, 4),
            new InternalPercentile(30, 10.5)));

        assertEquals(percentileRanks.percent(0), 0.0);
        assertEquals(percentileRanks.percent(3), 0.0);
        assertEquals(percentileRanks.percent(4), 10.0);
        final double percentUnder8 = percentileRanks.percent(8);
        assertTrue(percentUnder8 < 22.31 && percentUnder8 > 22.30, "was "+Double.toString(percentUnder8));
        assertEquals(percentileRanks.percent(10.5), 30.0);
        assertEquals(percentileRanks.percent(50), 30.0);
    }
}
