import sparkplug_client as spc
import time
from pathlib import Path

BROKER_ADDRESS = '192.168.20.16'
BROKER_PORT = 1883
CLIENT_ID = 'test-client'

client = spc.Client(CLIENT_ID)
print(f'spc.Client.on_connect: {spc.Client.on_connect}')
      
client.set_id('test-group', 'test-node')

birth_certificate_path = Path(__file__).parent / 'birth_certificate.json'
client.set_birth_certificate(spc.BirthCertificate.from_file(birth_certificate_path))
client.username_pw_set('test-user','password')

client.connect(BROKER_ADDRESS, BROKER_PORT)

client.state.set_node_metric('solar_power', 54.34)
client.state.set_node_metric('solar_power', 55.56)
client.state.set_device_metric('battery', 'voltage', 798.81)

client.publish_changes()
           
time.sleep(5)

client.state.set_node_metric('solar_power', 55.56)
client.state.set_device_metric('battery', 'voltage', 798.81)

client.publish_changes()

client.disconnect()