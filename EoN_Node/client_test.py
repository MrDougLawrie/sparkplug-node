import sparkplug_client as spc
import time
from pathlib import Path
import random

BROKER_ADDRESS = '192.168.20.16'
BROKER_PORT = 1883
CLIENT_ID = 'test-client'

client = spc.Client(CLIENT_ID)
print(f'spc.Client.on_message: {spc.Client.on_message}')
      
client.set_id('test-group', 'test-node')

birth_certificate_path = Path(__file__).parent / 'birth_certificate.json'
client.set_birth_certificate(spc.BirthCertificate.from_file(birth_certificate_path))
client.username_pw_set('test-user','password')

client.connect(BROKER_ADDRESS, BROKER_PORT)
time.sleep(1)
client.state.set_node_metric('solar_power', 54.34)
client.state.set_node_metric('solar_power', 55.56)
client.state.set_node_metric('ip_address', '192.168.20.8')
client.state.set_node_metric('inverter_status', True)
client.state.set_device_metric('battery', 'voltage', 798.81)

client.publish_changes()
for _ in range(100):
    client.state.set_node_metric('solar_power', random.gauss(60, 5))
    client.state.set_device_metric('battery', 'voltage', random.gauss(800, 1))
    client.publish_changes()
    time.sleep(0.2)

print('Disconnecting')
client.disconnect()