package com.port.terminal.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.port.terminal.common.BizException;
import com.port.terminal.common.Dict;
import com.port.terminal.domain.Container;
import com.port.terminal.domain.SlotKey;
import com.port.terminal.domain.YardSlot;
import com.port.terminal.domain.YardZone;
import com.port.terminal.dto.SlotActionRequest;
import com.port.terminal.repository.ContainerRepository;
import com.port.terminal.repository.YardSlotRepository;
import com.port.terminal.repository.YardZoneRepository;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 单箱位并发竞争验收：首次落位与翻倒共用原子占位（目标行悲观锁），
 * 20 尺 / 40 尺抢位与翻倒目标冲突都要求只有一个请求成功；
 * 失败方保留原位置、错误信息带占用箱号，事后箱位矩阵与箱档位置一致。
 */
@SpringBootTest
class YardServiceConcurrencyTest {

    private static final String ZONE = "A1";

    @Autowired
    private YardService yardService;
    @Autowired
    private YardZoneRepository zoneRepository;
    @Autowired
    private YardSlotRepository slotRepository;
    @Autowired
    private ContainerRepository containerRepository;

    @BeforeEach
    void setUp() {
        containerRepository.deleteAll();
        slotRepository.deleteAll();
        zoneRepository.deleteAll();

        YardZone zone = new YardZone();
        zone.setZoneCode(ZONE);
        zone.setBayCount(2);
        zone.setRowCount(6);
        zone.setTierCount(4);
        zone.setContainerTypeLimit("ALL");
        zone.setPowerOutlet(false);
        zone.setZoneType("普通重箱区");
        zoneRepository.save(zone);

        List<YardSlot> slots = new ArrayList<>();
        for (int bay = 1; bay <= 2; bay++) {
            for (int row = 1; row <= 6; row++) {
                for (int tier = 1; tier <= 4; tier++) {
                    YardSlot slot = new YardSlot();
                    slot.setId(new SlotKey(ZONE, bay, row, tier));
                    slot.setStatus("EMPTY");
                    slot.setRefrigerated(false);
                    slots.add(slot);
                }
            }
        }
        slotRepository.saveAll(slots);
    }

    @Test
    void concurrentPlace20Foot_onlyOneOccupiesSlot() throws Exception {
        newContainer("T20A0000001", "20GP");
        newContainer("T20B0000002", "20GP");

        List<Result> results = concurrently(yardService::place,
                new SlotActionRequest("T20A0000001", ZONE, 1, 3, 1),
                new SlotActionRequest("T20B0000002", ZONE, 1, 3, 1));

        Result winner = onlyWinner(results);
        Result loser = loserOf(results);
        assertTrue(loser.error().contains(winner.containerNo()),
                "失败方应看到占用箱号: " + loser.error());

        YardSlot slot = slotAt(ZONE, 1, 3, 1);
        assertEquals("OCCUPIED", slot.getStatus());
        assertEquals(winner.containerNo(), slot.getOccupiedContainerNo());
        assertPosition(winner.containerNo(), ZONE, 1, 3, 1);
        assertNoPosition(loser.containerNo());
        assertMatrixConsistent();
    }

    @Test
    void concurrentPlace40Foot_overlappingPair_noHalfOccupancy() throws Exception {
        newContainer("T40A0000001", "40GP");
        newContainer("T40B0000002", "40GP");

        // A 要 1 贝 3-4 行，B 要 1 贝 4-5 行：共享第 4 行，必然冲突
        List<Result> results = concurrently(yardService::place,
                new SlotActionRequest("T40A0000001", ZONE, 1, 3, 1),
                new SlotActionRequest("T40B0000002", ZONE, 1, 4, 1));

        Result winner = onlyWinner(results);
        Result loser = loserOf(results);
        assertTrue(loser.error().contains(winner.containerNo()),
                "失败方应看到占用箱号: " + loser.error());
        assertNoPosition(loser.containerNo());

        // 赢家相邻两格必须整体归它，被抢的第三格保持空，不留半占用
        Container winnerContainer = containerRepository.findByContainerNo(winner.containerNo()).orElseThrow();
        int anchorRow = winnerContainer.getCurrentRow();
        assertEquals(winner.containerNo(), slotAt(ZONE, 1, anchorRow, 1).getOccupiedContainerNo());
        assertEquals(winner.containerNo(), slotAt(ZONE, 1, anchorRow + 1, 1).getOccupiedContainerNo());
        YardSlot third = slotAt(ZONE, 1, anchorRow == 3 ? 5 : 3, 1);
        assertEquals("EMPTY", third.getStatus());
        assertNull(third.getOccupiedContainerNo());
        assertMatrixConsistent();
    }

    @Test
    void concurrentPlace_mixedSizes_loserLeavesNothingBehind() throws Exception {
        newContainer("T40C0000001", "40GP");
        newContainer("T20C0000002", "20GP");

        // 40 尺要 3-4 行，20 尺单要第 4 行：无论谁赢，另一格都不能被半占
        List<Result> results = concurrently(yardService::place,
                new SlotActionRequest("T40C0000001", ZONE, 1, 3, 1),
                new SlotActionRequest("T20C0000002", ZONE, 1, 4, 1));

        Result winner = onlyWinner(results);
        Result loser = loserOf(results);
        assertTrue(loser.error().contains(winner.containerNo()),
                "失败方应看到占用箱号: " + loser.error());
        assertNoPosition(loser.containerNo());

        Container winnerContainer = containerRepository.findByContainerNo(winner.containerNo()).orElseThrow();
        long occupied = slotRepository.findAll().stream()
                .filter(s -> "OCCUPIED".equals(s.getStatus())).count();
        if (Dict.isFortyFoot(winnerContainer.getType())) {
            assertEquals(2, occupied, "40 尺胜出时相邻两格整体占用");
            assertEquals(winner.containerNo(), slotAt(ZONE, 1, 3, 1).getOccupiedContainerNo());
            assertEquals(winner.containerNo(), slotAt(ZONE, 1, 4, 1).getOccupiedContainerNo());
        } else {
            assertEquals(1, occupied, "20 尺胜出时只占一格，40 尺不得留下半占用");
            assertEquals("EMPTY", slotAt(ZONE, 1, 3, 1).getStatus());
            assertEquals(winner.containerNo(), slotAt(ZONE, 1, 4, 1).getOccupiedContainerNo());
        }
        assertMatrixConsistent();
    }

    @Test
    void concurrentRelocate_sameTarget_loserKeepsOriginalSlot() throws Exception {
        newContainer("T20R0000001", "20GP");
        newContainer("T20R0000002", "20GP");
        yardService.place(new SlotActionRequest("T20R0000001", ZONE, 1, 1, 1));
        yardService.place(new SlotActionRequest("T20R0000002", ZONE, 1, 6, 1));
        Map<String, Integer> originalRow = Map.of("T20R0000001", 1, "T20R0000002", 6);

        List<Result> results = concurrently(yardService::relocate,
                new SlotActionRequest("T20R0000001", ZONE, 2, 3, 1),
                new SlotActionRequest("T20R0000002", ZONE, 2, 3, 1));

        Result winner = onlyWinner(results);
        Result loser = loserOf(results);
        assertTrue(loser.error().contains(winner.containerNo()),
                "失败方应看到占用箱号: " + loser.error());

        // 赢家：新位置生效，旧位置释放
        assertPosition(winner.containerNo(), ZONE, 2, 3, 1);
        assertEquals(winner.containerNo(), slotAt(ZONE, 2, 3, 1).getOccupiedContainerNo());
        YardSlot winnerOld = slotAt(ZONE, 1, originalRow.get(winner.containerNo()), 1);
        assertEquals("EMPTY", winnerOld.getStatus());
        assertNull(winnerOld.getOccupiedContainerNo());

        // 输家：保留原位置，箱档与箱位矩阵都不动
        int loserRow = originalRow.get(loser.containerNo());
        assertPosition(loser.containerNo(), ZONE, 1, loserRow, 1);
        YardSlot loserSlot = slotAt(ZONE, 1, loserRow, 1);
        assertEquals("OCCUPIED", loserSlot.getStatus());
        assertEquals(loser.containerNo(), loserSlot.getOccupiedContainerNo());
        assertMatrixConsistent();
    }

    @Test
    void concurrentRelocate40Foot_sameTargetPair_loserKeepsBothCells() throws Exception {
        newContainer("T40R0000001", "40GP");
        newContainer("T40R0000002", "40GP");
        yardService.place(new SlotActionRequest("T40R0000001", ZONE, 1, 1, 1)); // 占 1-2 行
        yardService.place(new SlotActionRequest("T40R0000002", ZONE, 1, 5, 1)); // 占 5-6 行
        Map<String, Integer> originalRow = Map.of("T40R0000001", 1, "T40R0000002", 5);

        List<Result> results = concurrently(yardService::relocate,
                new SlotActionRequest("T40R0000001", ZONE, 2, 2, 1),
                new SlotActionRequest("T40R0000002", ZONE, 2, 2, 1));

        Result winner = onlyWinner(results);
        Result loser = loserOf(results);
        assertTrue(loser.error().contains(winner.containerNo()),
                "失败方应看到占用箱号: " + loser.error());

        // 赢家：目标两格整体落位，旧两格整体释放
        assertPosition(winner.containerNo(), ZONE, 2, 2, 1);
        assertEquals(winner.containerNo(), slotAt(ZONE, 2, 2, 1).getOccupiedContainerNo());
        assertEquals(winner.containerNo(), slotAt(ZONE, 2, 3, 1).getOccupiedContainerNo());
        int winnerOldRow = originalRow.get(winner.containerNo());
        assertEquals("EMPTY", slotAt(ZONE, 1, winnerOldRow, 1).getStatus());
        assertEquals("EMPTY", slotAt(ZONE, 1, winnerOldRow + 1, 1).getStatus());

        // 输家：原来两格都保留，不能一半释放一半占用
        int loserRow = originalRow.get(loser.containerNo());
        assertPosition(loser.containerNo(), ZONE, 1, loserRow, 1);
        assertEquals(loser.containerNo(), slotAt(ZONE, 1, loserRow, 1).getOccupiedContainerNo());
        assertEquals(loser.containerNo(), slotAt(ZONE, 1, loserRow + 1, 1).getOccupiedContainerNo());
        assertMatrixConsistent();
    }

    // ---------- 并发执行与断言辅助 ----------

    private record Result(String containerNo, String error) {
        boolean ok() {
            return error == null;
        }
    }

    /** 两个请求同一起跑线并发提交，各自独立事务；返回每个请求的结果（箱号 + 冲突信息）。 */
    private List<Result> concurrently(Consumer<SlotActionRequest> action, SlotActionRequest... requests)
            throws InterruptedException {
        int n = requests.length;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Result>> futures = new ArrayList<>();
            for (SlotActionRequest request : requests) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        start.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        action.accept(request);
                        return new Result(request.containerNo(), null);
                    } catch (BizException e) {
                        return new Result(request.containerNo(), e.getMessage());
                    }
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS), "并发线程未能就绪");
            start.countDown();
            List<Result> results = new ArrayList<>();
            for (Future<Result> future : futures) {
                results.add(await(future));
            }
            return results;
        } finally {
            pool.shutdown();
            try {
                assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发任务未在限期内结束");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Result await(Future<Result> future) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("并发任务执行失败", e);
        }
    }

    private static Result onlyWinner(List<Result> results) {
        assertEquals(1, results.stream().filter(Result::ok).count(),
                "单箱位竞争只能有一个请求成功: " + results);
        return results.stream().filter(Result::ok).findFirst().orElseThrow();
    }

    private static Result loserOf(List<Result> results) {
        return results.stream().filter(r -> !r.ok()).findFirst().orElseThrow();
    }

    private Container newContainer(String containerNo, String type) {
        Container container = new Container();
        container.setContainerNo(containerNo);
        container.setType(type);
        container.setLoadStatus("FULL");
        container.setOwner("测试货主");
        container.setShippingCompany("中远海运");
        container.setWeight(new BigDecimal("20.5"));
        container.setDangerous(false);
        container.setPaidInspection(true);
        container.setPickupStatus("AVAILABLE");
        container.setEntryTime(LocalDateTime.now());
        return containerRepository.save(container);
    }

    private YardSlot slotAt(String zone, int bay, int row, int tier) {
        return slotRepository.findExactSlot(zone, bay, row, tier).orElseThrow();
    }

    private void assertPosition(String containerNo, String zone, int bay, int row, int tier) {
        Container container = containerRepository.findByContainerNo(containerNo).orElseThrow();
        assertEquals(zone, container.getCurrentYardZone());
        assertEquals(bay, container.getCurrentBay().intValue());
        assertEquals(row, container.getCurrentRow().intValue());
        assertEquals(tier, container.getCurrentTier().intValue());
    }

    private void assertNoPosition(String containerNo) {
        Container container = containerRepository.findByContainerNo(containerNo).orElseThrow();
        assertNull(container.getCurrentYardZone(), "失败方不应落位: " + containerNo);
    }

    /** 双向一致性：箱档指向的箱位必须记该箱号，被占箱位的箱号必须能反查到箱档位置。 */
    private void assertMatrixConsistent() {
        Map<String, Container> byNo = containerRepository.findAll().stream()
                .collect(Collectors.toMap(Container::getContainerNo, Function.identity()));
        for (Container container : byNo.values()) {
            for (SlotKey key : expectedCells(container)) {
                YardSlot slot = slotRepository.findById(key).orElseThrow();
                assertEquals("OCCUPIED", slot.getStatus(),
                        "箱档位置在矩阵中应为占用: " + container.getContainerNo());
                assertEquals(container.getContainerNo(), slot.getOccupiedContainerNo(),
                        "箱档位置与矩阵占用箱号不一致: " + container.getContainerNo());
            }
        }
        for (YardSlot slot : slotRepository.findAll()) {
            if (!"OCCUPIED".equals(slot.getStatus())) {
                assertNull(slot.getOccupiedContainerNo(), "空箱位不应残留箱号");
                continue;
            }
            Container container = byNo.get(slot.getOccupiedContainerNo());
            assertNotNull(container, "占用箱位找不到对应箱档: " + slot.getOccupiedContainerNo());
            assertTrue(expectedCells(container).contains(slot.getId()),
                    "箱位矩阵与箱档位置不一致: " + slot.getOccupiedContainerNo());
        }
    }

    /** 箱档位置展开为应占用的箱位：20 尺一格，40 尺为锚点行 + 相邻行两格。 */
    private List<SlotKey> expectedCells(Container container) {
        if (container.getCurrentYardZone() == null) {
            return List.of();
        }
        List<SlotKey> keys = new ArrayList<>();
        keys.add(new SlotKey(container.getCurrentYardZone(), container.getCurrentBay(),
                container.getCurrentRow(), container.getCurrentTier()));
        if (Dict.isFortyFoot(container.getType())) {
            keys.add(new SlotKey(container.getCurrentYardZone(), container.getCurrentBay(),
                    container.getCurrentRow() + 1, container.getCurrentTier()));
        }
        return keys;
    }
}
