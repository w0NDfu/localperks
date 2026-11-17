import groovy.json.JsonSlurper

String step = vars.get('step')
String kind = vars.get('journey')
String label = ['search':'shop.search', 'detail':'shop.detail', 'vouchers':'voucher.list',
    'submit':'seckill.submit', 'duplicate':'seckill.duplicate', 'poll':'order.poll',
    'action':kind == 'cancel' ? 'order.cancel' : 'order.pay',
    'wallet':'order.wallet', 'final_status':'order.final_status'][step]
vars.put('outcome', 'ok')
try {
    if (prev.responseCode == '429') {
        vars.put('outcome', 'rate_limited')
        prev.setSampleLabel(label + '.rate_limited')
        // A real rejection is retained separately; never counted as a successful business operation.
        prev.setSuccessful(false)
        // Rejection of an intentional retry must not abandon the previously accepted order.
        if (step == 'duplicate' && vars.get('orderId')) vars.put('step', 'poll')
        else vars.put('done', 'true')
        return
    }
    if (prev.responseCode != '200') throw new IllegalStateException('HTTP_' + prev.responseCode)
    def body = new JsonSlurper().parseText(prev.responseDataAsString)
    if (step == 'submit' && body.success == false && body.errorMsg == '库存不足') {
        vars.put('outcome', 'sold_out')
        prev.setSampleLabel(label + '.sold_out')
        prev.setSuccessful(true)
        vars.put('done', 'true')
        return
    }
    if (body.success != true) throw new IllegalStateException('BUSINESS_' + body.errorMsg)
    switch (step) {
        case 'search':
            if (!(body.data instanceof List) || body.data.isEmpty()) throw new IllegalStateException('EMPTY_SEARCH')
            vars.put('step', 'detail'); break
        case 'detail':
            if (body.data?.id.toString() != vars.get('shopId')) throw new IllegalStateException('WRONG_SHOP')
            vars.put('step', 'vouchers'); break
        case 'vouchers':
            if (!(body.data instanceof List)) throw new IllegalStateException('BAD_VOUCHER_LIST')
            if (kind == 'browse') vars.put('done', 'true')
            else vars.put('step', 'submit')
            break
        case 'submit':
            if (!(body.data?.orderId instanceof String) || !(body.data.orderId ==~ /[0-9]+/)) throw new IllegalStateException('BAD_ORDER_ID')
            vars.put('orderId', body.data.orderId)
            vars.put('order_started', Long.toString(prev.startTime))
            vars.put('outcome', body.data.requestState == 'UNKNOWN' ? 'unknown' : 'accepted')
            vars.put('step', vars.get('want_duplicate') == 'true' ? 'duplicate' : 'poll')
            break
        case 'duplicate':
            if (body.data?.orderId?.toString() != vars.get('orderId')) throw new IllegalStateException('DUPLICATE_CHANGED_ID')
            vars.put('outcome', 'same_order')
            vars.put('step', 'poll'); break
        case 'poll':
            int status = body.data.statusCode as int
            if (status == 12) {
                vars.put('visible_ms', Long.toString(prev.endTime - Long.parseLong(vars.get('order_started'))))
                vars.put('outcome', 'visible')
                vars.put('step', kind == 'abandon' ? 'wallet' : 'action')
            } else if (status == 15) throw new IllegalStateException('ORDER_FAILED')
            else if (prev.endTime - Long.parseLong(vars.get('order_started')) > 30000) throw new IllegalStateException('ORDER_VISIBLE_TIMEOUT')
            else vars.put('outcome', 'pending')
            break
        case 'action': vars.put('step', 'wallet'); break
        case 'wallet':
            if (!(body.data?.records instanceof List) || !body.data.records.any { it.orderId?.toString() == vars.get('orderId') }) throw new IllegalStateException('ORDER_NOT_IN_WALLET')
            vars.put('step', 'final_status'); break
        case 'final_status':
            int wanted = kind == 'cancel' ? 14 : kind == 'abandon' ? 12 : 13
            if ((body.data.statusCode as int) != wanted) throw new IllegalStateException('WRONG_FINAL_STATUS')
            vars.put('done', 'true'); break
    }
    prev.setSuccessful(true)
    prev.setSampleLabel(label + '.' + vars.get('outcome'))
} catch (Exception error) {
    vars.put('outcome', 'unexpected')
    vars.put('done', 'true')
    prev.setSuccessful(false)
    prev.setSampleLabel(label + '.unexpected')
    prev.setResponseMessage('ASSERT_' + error.message + '; original=' + prev.getResponseMessage())
}
finally {
    if (vars.get('done') == 'true') vars.put('session_ms', Long.toString(prev.endTime - Long.parseLong(vars.get('session_started'))))
}
