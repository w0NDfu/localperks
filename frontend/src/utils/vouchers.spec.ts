import { describe, expect, it } from 'vitest'
import { formatVoucherPrice, voucherAvailability } from './vouchers'

describe('voucher display and claimability', () => {
  const now = new Date('2026-11-05T12:00:00').getTime()
  const offer = { type: 1, stock: 3, beginTime: '2026-11-01T00:00:00', endTime: '2026-12-01T00:00:00' }

  it('formats seckill prices as cents while preserving legacy regular prices', () => {
    expect(formatVoucherPrice(1900)).toContain('19.00')
    expect(formatVoucherPrice(22, false)).toContain('22.00')
  })

  it('allows claiming only in the active window', () => {
    expect(voucherAvailability(offer, now).canClaim).toBe(true)
    expect(voucherAvailability(offer, new Date(offer.endTime).getTime()).canClaim).toBe(false)
    expect(voucherAvailability(offer, new Date(offer.beginTime).getTime() - 1).canClaim).toBe(false)
  })

  it('distinguishes unavailable stock from a sellout', () => {
    expect(voucherAvailability({ ...offer, stock: undefined }, now).label).toBe('库存待同步')
    expect(voucherAvailability({ ...offer, stock: 0 }, now).label).toBe('已抢完')
  })

  it('does not offer seckill on a regular voucher', () => {
    expect(voucherAvailability({ ...offer, type: 0 }, now).canClaim).toBe(false)
  })
})
