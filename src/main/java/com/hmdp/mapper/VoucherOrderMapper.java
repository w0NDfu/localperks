package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.hmdp.dto.VoucherWalletEntry;
import org.apache.ibatis.annotations.Param;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    IPage<VoucherWalletEntry> queryWallet(IPage<VoucherWalletEntry> page, @Param("userId") Long userId,
                                        @Param("ownedOnly") boolean ownedOnly, @Param("topic") String topic,
                                        @Param("receipts") java.util.List<VoucherWalletEntry> receipts);
}
