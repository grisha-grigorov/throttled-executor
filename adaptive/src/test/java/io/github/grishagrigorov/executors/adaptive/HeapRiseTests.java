package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The solo heap-peak arithmetic: the largest rise of heap-in-use above the <em>lowest
 * point seen so far</em> during the run, not above the reading at the start. A garbage
 * collection early in the run (typically triggered by the task's own first allocations)
 * drops heap-in-use below the start reading; measured against the start, everything the
 * task builds afterwards would read as zero. Measured against the low point, it reads as
 * what it is.
 */
class HeapRiseTests {

    private static long riseOver(long... readings) {
        final HeapRise rise = new HeapRise(readings[0]);
        for (int i = 1; i < readings.length; i++) {
            rise.observe(readings[i]);
        }
        return rise.value();
    }

    @Test
    @DisplayName("plain growth: the rise above the start")
    void plainGrowth() {
        assertThat(riseOver(200, 232)).isEqualTo(32);
        assertThat(riseOver(200, 210, 232, 220)).isEqualTo(32);
    }

    @Test
    @DisplayName("a collection early in the run: the rise above the new low, not zero")
    void earlyCollection() {
        assertThat(riseOver(200, 50, 82)).isEqualTo(32);
    }

    @Test
    @DisplayName("a collection late in the run: the peak before it still counts")
    void lateCollection() {
        assertThat(riseOver(200, 232, 82)).isEqualTo(32);
    }

    @Test
    @DisplayName("a collection reclaiming more than the task built, with nothing built afterwards: zero")
    void collectionSwallowsEverything() {
        assertThat(riseOver(200, 82)).isZero();
        assertThat(riseOver(100, 100)).isZero();
    }

    @Test
    @DisplayName("the largest rise wins, wherever it happens")
    void largestRiseWins() {
        assertThat(riseOver(300, 100, 400, 50, 120)).isEqualTo(300);
        assertThat(riseOver(300, 100, 150, 50, 400)).isEqualTo(350);
    }

    @Test
    @DisplayName("negative readings (a failed meter) are ignored")
    void negativeReadingsIgnored() {
        assertThat(riseOver(200, -1, 232)).isEqualTo(32);
        assertThat(riseOver(200, -1)).isZero();
    }

    @Test
    @DisplayName("never negative")
    void neverNegative() {
        assertThat(riseOver(200, 100, 50)).isZero();
    }
}
