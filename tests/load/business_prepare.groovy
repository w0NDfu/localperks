import groovy.json.JsonOutput
import java.net.URLEncoder

if (vars.get('journey') == null) {
    long uid = Long.parseLong(vars.get('uid'))
    int bucket = (int)(uid % 100)
    String journey = bucket < 90 ? 'browse' : bucket < 96 ? 'pay' : bucket < 98 ? 'cancel' : bucket == 98 ? 'abandon' : 'scarce'
    vars.put('journey', journey)
    vars.put('step', 'search')
    vars.put('done', 'false')
    vars.put('session_started', Long.toString(System.currentTimeMillis()))
    def random = new Random(uid * 31 + Long.parseLong(props.get('phaseSeed')))
    vars.putObject('rng', random)
    long shop = 100000L + (random.nextInt(100) < 80 ? random.nextInt(200) : 200 + random.nextInt(19800))
    if (journey != 'browse') {
        int choice = journey == 'scarce' ? 8 : random.nextInt(100) < 80 ? random.nextInt(2) : 2 + random.nextInt(6)
        shop = 100000L + choice
        vars.put('voucherId', Integer.toString(Integer.parseInt(props.get('voucherBase')) + choice))
    }
    vars.put('shopId', Long.toString(shop))
    vars.put('clientIp', '198.18.' + (Math.floorDiv(uid, 250L) % 250) + '.' + (uid % 250 + 1))
    vars.put('want_duplicate', uid % 5 == 0 ? 'true' : 'false')
}
vars.put('visible_ms', '')
vars.put('session_ms', '')
vars.put('outcome', '')
String step = vars.get('step')
def rng = vars.getObject('rng')
vars.put('request_method', 'GET')
vars.put('request_body', '')
vars.put('pause_ms', step == 'search' ? '0' : Integer.toString(100 + rng.nextInt(301)))
switch (step) {
    case 'search':
        vars.put('request_path', rng.nextInt(100) < 70
            ? '/shop/of/type?typeId=' + (1 + rng.nextInt(10)) + '&current=' + (1 + rng.nextInt(3))
            : '/shop/of/name?name=MixShop&current=' + (1 + rng.nextInt(3)))
        break
    case 'detail': vars.put('request_path', '/shop/' + vars.get('shopId')); break
    case 'vouchers': vars.put('request_path', '/voucher/list/' + vars.get('shopId')); break
    case 'submit':
    case 'duplicate':
        vars.put('request_method', 'POST')
        vars.put('request_path', '/voucher-order/seckill/' + vars.get('voucherId'))
        vars.put('pause_ms', step == 'duplicate' ? '100' : '200')
        break
    case 'poll':
    case 'final_status':
        vars.put('request_path', '/voucher-order/status/' + vars.get('orderId'))
        vars.put('pause_ms', step == 'poll' ? '250' : '100')
        break
    case 'action':
        vars.put('request_method', 'POST')
        vars.put('request_path', vars.get('journey') == 'cancel'
            ? '/voucher-order/cancel/' + vars.get('orderId')
            : '/voucher-order/mock-pay/' + vars.get('orderId') + '/true')
        vars.put('pause_ms', Integer.toString(500 + rng.nextInt(1001)))
        break
    case 'wallet': vars.put('request_path', '/voucher-order/mine?page=1'); break
    default: throw new IllegalStateException('Unexpected journey step: ' + step)
}
