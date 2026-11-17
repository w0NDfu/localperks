"""A declared 32-order setup phase warms the injector compiler and drains before measurement."""
import copy
import csv
import xml.etree.ElementTree as ET


def remove_pairs(tree, predicate):
    for child in list(tree):
        if child not in list(tree):
            continue
        if predicate(child):
            children = list(tree)
            index = children.index(child)
            tree.remove(child)
            if index + 1 < len(children) and children[index + 1].tag == 'hashTree':
                tree.remove(children[index + 1])
        elif child.tag == 'hashTree':
            remove_pairs(child, predicate)


def make_warm_plan(run):
    root = ET.parse(run.plan)
    plan_tree = root.getroot().find('hashTree/hashTree')
    group_tree = list(plan_tree)[1]
    for processor in group_tree.iter('JSR223PostProcessor'):
        script = processor.find("stringProp[@name='script']")
        script.text = script.text.replace("prev.setSampleLabel('seckill.' + outcome)",
            "prev.setSampleLabel((vars.get('p6Warmup') == 'true' ? 'warmup.' : 'seckill.') + outcome)")
        script.text += """
if (vars.get('p6Warmup') == 'true') {
    if (outcome != 'accepted') {
        ctx.getEngine().stopTest(true)
        throw new IllegalStateException('P6 warmup acceptance failed')
    }
    boolean visible = false
    for (int attempt = 0; attempt < 50 && !visible; attempt++) {
        def connection = new URL('http://127.0.0.1:' + props.get('port') + '/voucher-order/status/' + vars.get('orderId')).openConnection()
        connection.connectTimeout = 1000
        connection.readTimeout = 2000
        connection.setRequestProperty('Authorization', vars.get('token'))
        try {
            def status = connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
            visible = status.success == true && status.data.statusCode == 12
        } finally { connection.disconnect() }
        if (!visible) Thread.sleep(100)
    }
    if (!visible) {
        ctx.getEngine().stopTest(true)
        throw new IllegalStateException('P6 warmup orders did not drain')
    }
}
"""
    setup = ET.fromstring('''<SetupThreadGroup guiclass="SetupThreadGroupGui" testclass="SetupThreadGroup" testname="Declared warmup 32 orders" enabled="true">
      <stringProp name="ThreadGroup.on_sample_error">stoptest</stringProp>
      <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController">
        <boolProp name="LoopController.continue_forever">false</boolProp><stringProp name="LoopController.loops">4</stringProp>
      </elementProp>
      <stringProp name="ThreadGroup.num_threads">8</stringProp><stringProp name="ThreadGroup.ramp_time">1</stringProp>
      <boolProp name="ThreadGroup.scheduler">false</boolProp>
    </SetupThreadGroup>''')
    setup_tree = copy.deepcopy(group_tree)
    remove_pairs(setup_tree, lambda item: item.tag == 'SyncTimer' or
                 item.tag == 'HTTPSamplerProxy' and item.get('testname') == 'preconnect')
    for path in setup_tree.findall(".//stringProp[@name='HTTPSampler.path']"):
        path.text = '/voucher-order/seckill/98200'
    for data in setup_tree.iter('CSVDataSet'):
        data.find("stringProp[@name='filename']").text = str(run.path / 'temp/warmup.csv')
    for tree in setup_tree.iter('hashTree'):
        if any(item.tag == 'HeaderManager' for item in tree):
            processor = ET.fromstring('''<JSR223PreProcessor guiclass="TestBeanGUI" testclass="JSR223PreProcessor" testname="Warmup scope" enabled="true">
              <stringProp name="scriptLanguage">groovy</stringProp><stringProp name="cacheKey">true</stringProp>
              <stringProp name="script">vars.put('p6Warmup','true')</stringProp>
            </JSR223PreProcessor>''')
            tree.insert(0, ET.Element('hashTree'))
            tree.insert(0, processor)
    plan_tree.insert(0, setup_tree)
    plan_tree.insert(0, setup)
    run.plan = run.path / 'p6-measured.jmx'
    root.write(run.plan, encoding='utf-8', xml_declaration=True)


async def seed_warmup(run):
    await run.coupon(98200, 40)
    with (run.path / 'temp/warmup.csv').open('w', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerows((user, run.tokens[user], '127.0.0.1') for user in range(10001, 10033))
