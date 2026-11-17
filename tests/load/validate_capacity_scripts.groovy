import groovy.json.JsonOutput
import org.apache.jmeter.threads.JMeterVariables
import org.apache.jmeter.samplers.SampleResult

def parsed = new GroovyShell().parse(new File('tests/load/capacity_classify.groovy'))
def cases = [
    [code:'200',bucket:'steady',body:[success:true,data:[orderId:'123',requestState:'ACCEPTED']],wanted:'accepted'],
    [code:'200',bucket:'steady',body:[success:true,data:[orderId:'123',requestState:'UNKNOWN']],wanted:'unknown'],
    [code:'200',bucket:'steady',body:[success:false,errorMsg:'库存不足'],wanted:'sold_out'],
    [code:'429',bucket:'steady',body:[:],wanted:'backpressure'],
    [code:'500',bucket:'steady',body:[:],wanted:'unexpected'],
    [code:'200',bucket:'steady',body:[success:false,errorMsg:'other error'],wanted:'unexpected'],
    [code:'200',bucket:'ramp',body:[success:true,data:[]],wanted:'ready'],
]
for (item in cases) {
    def vars = new JMeterVariables();vars.put('bucket',item.bucket)
    def sample = new SampleResult();sample.setResponseCode(item.code)
    sample.setResponseData(JsonOutput.toJson(item.body),'UTF-8')
    parsed.binding = new Binding([vars:vars,prev:sample]);parsed.run()
    assert vars.get('outcome') == item.wanted
    assert sample.isSuccessful() == (item.wanted in ['ready','accepted','sold_out'])
}
println('Verified accepted, unknown, sold-out, backpressure, HTTP failure and business failure classifications')
