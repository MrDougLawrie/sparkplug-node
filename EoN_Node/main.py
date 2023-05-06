## Import
# MQTT
import sparkplug_client as spc

# Utility
from pathlib import Path
import random
import time
import datetime
import math

## Initialise MQTT
BROKER_ADDRESS = '192.168.20.16'
BROKER_PORT = 1883
CLIENT_ID = 'test-client'

BIRTH_CERTIFICATE_FILNAME = 'vpp_bc.json'


class SolarBess(spc.Client):
    def __init__(self, client_id):
        super().__init__(client_id)
        self.irradiance_nominal = 1_000 # W/m2
        self.pv_power_nominal = 20_000 # W
        self.pv_voltage_nominal = 48 # VDC
        self.ac_voltage_nominal = 3_000 # VAC
        self.bess_nominal_voltage = 600 #VDC

        self.last_time_secs = 0
        self.time_secs = 0
        self.start_time = 0

    def start(self):
        now = datetime.datetime.now()
        self.time_secs = now.second + 60*now.minute + 3600*now.hour
        self.last_time_secs = self.time_secs
        self.start_time = self.time_secs

    def sim_loop(self):
        # Time
        now = datetime.datetime.now()
        self.time_secs = now.microsecond*1e-6 + now.second + 60*now.minute + 3600*now.hour
        dt = self.time_secs - self.last_time_secs

        self.state.set_node_metric('uptime', int(1000*(self.time_secs - self.start_time)))
        
        self.set_irradiance(noise=1)
        self.set_temperature(noise=0.01)

        # End Loop
        self.last_time_secs = self.time_secs

    def set_irradiance(self, dawn=6.0, dusk=18.0, max=1000.0, noise=0.0):
        hours_since_midnight = self.time_secs/3600 % 24
        if hours_since_midnight < dawn or hours_since_midnight > dusk:
            self.state.set_node_metric('irradiance', 0)
        else:
            A = -4*max/(dusk - dawn)**2
            i = A*(hours_since_midnight - dawn)*(hours_since_midnight - dusk) + random.normalvariate(0, noise)
            self.state.set_node_metric('irradiance', i)

    def set_temperature(self, minimum=10.0, maximum=25.0, noise=0.0):
        hours_since_midnight = self.time_secs/3600 % 24
        A = (maximum - minimum)/2
        avg = (maximum + minimum)/2
        MAX_TIME = 15
        t = avg + A*math.cos(2*math.pi/24*hours_since_midnight - MAX_TIME) + random.normalvariate(0, noise)
        self.state.set_node_metric('ambient_temperature', t)

    def handle_node_metric_event(self, event: spc.MetricChangeEvent):
        if event.metric_name == 'manual_mode':
            self.state.set_node_metric('poc_sp_mode', not event.new_value)
            self.state.set_node_metric('mode_string', 'Manual' if event.new_value else 'PoC SP Control')

    def handle_device_event(self, event: spc.DeviceChangeEvent):
        if event.device_id == '':
            pass


client = SolarBess(CLIENT_ID)
client.set_id('VPP', 'Site1')

birth_certificate_path = Path(__file__).parent / 'solar_bess.json'
client.set_birth_certificate(spc.BirthCertificate.from_file(birth_certificate_path))
client.username_pw_set('test-user','password')

client.start()
## Main Loop
while True:
    ## Check Connection
    if not client.connected:
        client.connect(BROKER_ADDRESS, BROKER_PORT)
        time.sleep(0.5)

    print(datetime.datetime.now())
    client.sim_loop()

    events = client.inbound_events()
    for event in events:
        print(event)
        if isinstance(event, spc.MetricChangeEvent):
            client.handle_node_metric_event(event)
        elif isinstance(event, spc.DeviceChangeEvent):
            client.handle_device_event(event)

    ## Publish Changes
    client.publish_changes()

    time.sleep(abs(random.normalvariate(0.6, 0.2)))
    
    # ## Client Loop - await inputs
    # # Result is a list of events such as NCMD, DCMD, Node Control
    # result = client.inbound_events()
    
    # ## Process inputs
    # if result:
    #     continue