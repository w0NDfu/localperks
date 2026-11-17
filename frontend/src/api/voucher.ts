import { request } from './http'
import type { SeckillOrderSubmitResult, Voucher, VoucherOrderStatusResult, VoucherWallet } from '@/types/domain'
export const voucherApi = {
  market: () => request<Voucher[]>({ url: '/voucher/market' }),
  mine: (page = 1, ownedOnly = false) => request<VoucherWallet>({ url: '/voucher-order/mine', params: { page, ownedOnly } }),
  list: (shopId: number) => request<Voucher[]>({ url: `/voucher/list/${shopId}` }), create: (data: Voucher) => request<number>({ url: '/voucher', method: 'post', data }),
  createSeckill: (data: Voucher) => request<number>({ url: '/voucher/seckill', method: 'post', data }), seckill: (id: number) => request<SeckillOrderSubmitResult>({ url: `/voucher-order/seckill/${id}`, method: 'post' }),
  status: (id: string) => request<VoucherOrderStatusResult>({ url: `/voucher-order/status/${encodeURIComponent(id)}`, timeout: 5000 }),
  mockPay: (id: string, success: boolean) => request<string>({ url: `/voucher-order/mock-pay/${encodeURIComponent(id)}/${success}`, method: 'post' }),
}
