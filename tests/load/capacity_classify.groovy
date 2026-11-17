import groovy.json.JsonSlurper
String outcome = 'unexpected'
try {
    if (prev.responseCode == '429') outcome = 'backpressure'
    else if (prev.responseCode == '200') {
        def body = new JsonSlurper().parseText(prev.responseDataAsString)
        if (vars.get('bucket') in ['ramp','late'] && body.success == true) outcome = 'ready'
        else if (body.success == false && body.errorMsg == '库存不足') outcome = 'sold_out'
        else if (body.success == true && body.data?.orderId instanceof String && body.data.orderId ==~ /[0-9]+/) {
            vars.put('orderId', body.data.orderId)
            outcome = body.data.requestState == 'UNKNOWN' ? 'unknown' : body.data.requestState == 'FAILED' ? 'failed' : 'accepted'
        }
    }
} catch (Exception ignored) { }
vars.put('outcome', outcome)
prev.setSampleLabel(vars.get('bucket') + '.' + outcome)
prev.setSuccessful(outcome in ['ready', 'accepted', 'sold_out'])
