import groovy.json.JsonOutput
import org.apache.jmeter.threads.JMeterVariables
import org.apache.jmeter.samplers.SampleResult

def prepare = new GroovyShell().parse(new File('tests/load/business_prepare.groovy'))
def validate = new GroovyShell().parse(new File('tests/load/business_assert.groovy'))
def props = new Properties()
props.putAll([phaseSeed:'5107', voucherBase:'98400'])
for (long uid : [10101L,10190L,10195L,10196L,10198L,10199L]) {
    def vars = new JMeterVariables()
    vars.put('uid',uid.toString())
    int steps = 0
    while(vars.get('done') != 'true') {
        prepare.binding = new Binding([vars:vars,props:props])
        prepare.run()
        assert vars.get('request_path').startsWith('/')
        def step = vars.get('step')
        def data = step == 'search' ? [[id:100000]] : step == 'detail' ? [id:vars.get('shopId').toLong()] :
            step == 'vouchers' ? [] : step in ['submit','duplicate'] ? [orderId:'123456',requestState:'ACCEPTED'] :
            step == 'wallet' ? [records:[[orderId:'123456']]] : step == 'final_status' ?
            [statusCode:vars.get('journey') == 'cancel' ? 14 : vars.get('journey') == 'abandon' ? 12 : 13] : [statusCode:12]
        def prev = new SampleResult()
        prev.setStampAndTime(System.currentTimeMillis(),1)
        prev.setResponseCode('200')
        prev.setResponseData(JsonOutput.toJson([success:true,data:data]),'UTF-8')
        validate.binding = new Binding([vars:vars,prev:prev])
        validate.run()
        assert prev.isSuccessful(): prev.getResponseMessage()
        assert ++steps < 15
    }
    println('Validated '+vars.get('journey')+' journey for '+uid+' in '+steps+' requests')
}
