import type { Voucher } from '@/types/domain'

const yuan = new Intl.NumberFormat('zh-CN', { style: 'currency', currency: 'CNY' })

export function formatVoucherPrice(value?: number, minorUnits = true) {
  return value == null ? '—' : yuan.format(minorUnits ? value / 100 : value)
}

export function formatVoucherTime(value?: string | null) {
  return value ? value.replace('T', ' ').slice(0, 16) : '—'
}

export function voucherAvailability(voucher: Voucher, now: number) {
  if (voucher.type !== 1) return { label: '普通券', canClaim: false }
  if (!voucher.beginTime || !voucher.endTime) return { label: '活动未配置', canClaim: false }
  if (now < new Date(voucher.beginTime).getTime()) return { label: '即将开抢', canClaim: false }
  if (now >= new Date(voucher.endTime).getTime()) return { label: '活动已结束', canClaim: false }
  if (voucher.stock == null) return { label: '库存待同步', canClaim: false }
  if (voucher.stock <= 0) return { label: '已抢完', canClaim: false }
  return { label: '立即抢券', canClaim: true }
}
