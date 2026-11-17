import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import ElementPlus, { ElSelect } from 'element-plus'
import ShopsView from './ShopsView.vue'

const { api, route } = vi.hoisted(() => ({
  api: { types: vi.fn(), byName: vi.fn(), byType: vi.fn() },
  route: { query: {} as Record<string, string> },
}))

vi.mock('@/api/shop', () => ({ shopApi: api }))
vi.mock('vue-router', () => ({ useRoute: () => route, useRouter: () => ({ push: vi.fn() }) }))

describe('shop browsing', () => {
  let wrapper: VueWrapper | undefined
  const shops = [
    { id: 1, name: 'First shop', typeId: 1 },
    { id: 2, name: 'Second shop', typeId: 1 },
  ]

  beforeEach(() => {
    vi.clearAllMocks()
    route.query = {}
    api.types.mockResolvedValue([{ id: 1, name: 'Food', sort: 1 }])
    api.byName.mockResolvedValue(shops)
    api.byType.mockResolvedValue([shops[0]])
  })

  afterEach(() => wrapper?.unmount())

  async function openPage() {
    wrapper = mount(ShopsView, { global: { plugins: [ElementPlus] } })
    await flushPromises()
    return wrapper
  }

  it('shows all shops immediately without requiring a category', async () => {
    const page = await openPage()
    expect(api.byName).toHaveBeenCalledWith('', 1)
    expect(api.byType).not.toHaveBeenCalled()
    expect(page.text()).toContain('First shop')
    expect(page.text()).toContain('Second shop')
  })

  it('preserves a category selected through the page URL', async () => {
    route.query = { typeId: '1' }
    const page = await openPage()
    expect(api.byType).toHaveBeenCalledWith({ typeId: 1, current: 1, x: undefined, y: undefined })
    expect(api.byName).not.toHaveBeenCalled()
    expect(page.text()).toContain('First shop')
    expect(page.text()).not.toContain('Second shop')
  })

  it('returns to all shops when the category is cleared', async () => {
    route.query = { typeId: '1' }
    const page = await openPage()
    const select = page.findComponent(ElSelect)
    select.vm.$emit('update:modelValue', undefined)
    select.vm.$emit('change', undefined)
    await flushPromises()
    expect(api.byName).toHaveBeenCalledWith('', 1)
    expect(page.text()).toContain('Second shop')
  })

  it('uses ten shops per page when browsing all shops', async () => {
    api.byName.mockResolvedValue(Array.from({ length: 5 }, (_, index) => ({ id: index + 1, name: `Shop ${index}` })))
    const page = await openPage()
    const nextButton = page.findAll('button').find(button => button.text() === '下一页')
    expect(nextButton?.attributes('disabled')).toBeDefined()
  })
})
