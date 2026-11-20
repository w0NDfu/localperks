package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.cache.VoucherCacheInvalidationEvent;
import com.hmdp.cache.VoucherCacheService;
import com.hmdp.cache.SeckillActivityCache;
import lombok.extern.slf4j.Slf4j;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.util.List;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    private static final int SECKILL_VOUCHER_TYPE = 1;

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private VoucherCacheService voucherCacheService;
    @Resource
    private ApplicationEventPublisher applicationEventPublisher;
    @Resource
    private SeckillActivityCache seckillActivityCache;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        List<Voucher> vouchers = voucherCacheService.queryByShopId(
                shopId, getBaseMapper()::queryVoucherOfShop);
        return Result.ok(vouchers);
    }

    @Override
    public Result queryMarket() {
        return Result.ok(voucherCacheService.mergeLiveStock(getBaseMapper().queryMarket()));
    }

    @Override
    @Transactional
    public boolean save(Voucher voucher) {
        boolean saved = super.save(voucher);
        if (saved && voucher.getShopId() != null) {
            applicationEventPublisher.publishEvent(new VoucherCacheInvalidationEvent(voucher.getShopId()));
        }
        return saved;
    }

    @Override
    @Transactional
    public boolean updateById(Voucher voucher) {
        Long shopId = voucher.getShopId();
        if (shopId == null && voucher.getId() != null) {
            Voucher existing = getById(voucher.getId());
            shopId = existing == null ? null : existing.getShopId();
        }
        boolean updated = super.updateById(voucher);
        if (updated && shopId != null) {
            applicationEventPublisher.publishEvent(new VoucherCacheInvalidationEvent(shopId));
        }
        return updated;
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        if (voucher.getStock() == null || voucher.getStock() < 0 || voucher.getBeginTime() == null
                || voucher.getEndTime() == null || !voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("秒杀库存和活动时间不合法");
        }
        voucher.setType(SECKILL_VOUCHER_TYPE);
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        // Publish only after SQL commits. A failed warmup is retried by the background scanner.
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try { seckillActivityCache.warm(voucher.getId()); }
                catch (Exception error) { log.warn("Activity saved; Redis preheat will retry, voucherId={}", voucher.getId(), error); }
            }
        });
    }
}
