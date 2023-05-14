## Import
# MQTT
import sparkplug_client as spc

# Utility
from pathlib import Path
import random
import time
import datetime
import math
import socket

## MQTT CONSTANTS
BROKER_ADDRESS = '192.168.0.2'
BROKER_PORT = 1883
CLIENT_ID = 'test-client'

BIRTH_CERTIFICATE_FILENAME = 'solar_bess.json'


class SolarBess(spc.Client):
    def __init__(self, client_id):
        super().__init__(client_id)
        self.pv_irradiance_nominal = 1_000 # W/m2
        self.pv_temperature_nominal = 25 # C
        self.pv_cell_temperature_nominal = 40 # C
        self.pv_power_nominal = 20_000 # W
        self.pv_voltage_nominal = 798 # VDC
        self.pv_efficiency = 0.15
        self.set_pv_capacity(self.pv_power_nominal)
        self.pv_power_temperature_coefficient = 0.0032 # /C
        self.pv_voltage_temperature_coefficient = 0.0028 # /C
        self.pv_pcs_efficiency = 0.98

        self.ac_voltage_nominal = 3_000 # VAC
        
        self.bess_nominal_voltage = 600 #VDC
        self.bess_capacity = 60_000
        self.bess_stored_energy = 30_000 # Wh
        self.bess_efficiency = 0.98 #
        self.bess_pcs_efficiency = 0.97
        self.bess_soc_low_low = 10 # %
        self.bess_soc_low = 20 # %
        self.bess_soc_high = 80 # %
        self.bess_soc_high_high = 100 # %

        self.poc_efficiency = 0.99
        self.last_poc_error = 0 # for PI control

        self.last_time_secs = 0
        self.time_secs = 0
        self.start_time = 0

        self.mode_was_manual = True # revert to manual mode if starting in local

        self.handle_ncmd = self.handle_node_metric_change_event
        self.handle_dcmd = self.handle_device_change_event

    def start(self):
        now = datetime.datetime.now()
        self.time_secs = now.second + 60*now.minute + 3600*now.hour
        self.last_time_secs = self.time_secs
        self.start_time = self.time_secs
        self.state.set_node_metric('hostname', socket.gethostname())
        self.local_falling_edge = self.state.node_metrics['local_mode']
        self.state.set_device_metric('bess', 'battery/capacity', self.bess_capacity)

    def set_pv_capacity(self, capacity):
        self.pv_area = capacity/(self.pv_irradiance_nominal*self.pv_efficiency)
        self.pv_power_nominal = capacity

    def sim_loop(self):
        # Time
        now = datetime.datetime.now()
        self.time_secs = now.microsecond*1e-6 + now.second + 60*now.minute + 3600*now.hour
        dt = self.time_secs - self.last_time_secs

        self.state.set_node_metric('uptime', int(1000*(self.time_secs - self.start_time)))
        self.state.set_node_metric('clock', str(datetime.datetime.now()))

        # Weather
        self.set_irradiance(noise=1)
        self.set_temperature(noise=0.01)
        self.set_pv_output()
        self.set_bess_soc(dt)
        
        # Local operator enables Local control every 10 minutes
        if now.minute % 10 == 0:
            self.local_falling_edge = True
            self.force_local_mode()
        else:
            if self.local_falling_edge:
                self.local_falling_edge = False
                self.exit_local_mode()

        # Control and Dynamics
        if self.state.node_metrics['local_mode'].value:
            self.set_bess_output_local(dt)
        elif self.state.node_metrics['manual_mode'].value:
            self.mode_was_manual = True
            self.set_bess_output_manual(dt)
            poc_p = self.state.get_device_metric_value('poc', 'ac/active_power')
            self.state.set_device_metric('poc', 'power_setpoint', poc_p)
            self.state.set_device_metric('poc', 'power_setpoint_readback', poc_p)
        elif self.state.node_metrics['poc_sp_mode'].value:
            self.mode_was_manual = False
            kc = self.state.get_device_metric_value('poc', 'control_constants/kc')
            tau_i = self.state.get_device_metric_value('poc', 'control_constants/tau_i')
            self.set_bess_output_poc_control(dt, kc, tau_i)
            
        self.set_poc_output()

        # End Loop
        self.last_time_secs = self.time_secs

    def force_local_mode(self):
        self.set_mode_local()
    
    def exit_local_mode(self):
        if self.mode_was_manual:
            self.set_mode_manual()
        else:
            self.set_mode_poc_sp_control()

    def set_mode_local(self):
        self.state.set_node_metric('local_mode', True)
        self.state.set_node_metric('manual_mode', False)
        self.state.set_node_metric('poc_sp_mode', False)
        self.state.set_node_metric('mode_string', 'Local')

    def set_mode_manual(self):
        self.state.set_node_metric('local_mode', False)
        self.state.set_node_metric('manual_mode', True)
        self.state.set_node_metric('poc_sp_mode', False)
        self.state.set_node_metric('mode_string', 'Manual')
    
    def set_mode_poc_sp_control(self):
        self.state.set_node_metric('local_mode', False)
        self.state.set_node_metric('manual_mode', False)
        self.state.set_node_metric('poc_sp_mode', True)
        self.state.set_node_metric('mode_string', 'PoC SP Control')

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
        MAX_TIME = 14
        t = avg + A*math.cos(2*math.pi/24*hours_since_midnight - MAX_TIME) + random.normalvariate(0, noise)
        self.state.set_node_metric('ambient_temperature', t)

    def set_pv_output(self):
        # Cell temperature increases over ambient with irradiance
        t = self.state.node_metrics['ambient_temperature'].value
        NOCT = self.pv_cell_temperature_nominal
        ir = self.state.node_metrics['irradiance'].value
        I_NOM = self.pv_irradiance_nominal
        
        cell_temp = t + (NOCT - 25)*ir/I_NOM

        # PV DC Active Power Output
        A = self.pv_area
        H = self.pv_efficiency
        Tcp = self.pv_power_temperature_coefficient
        Tstc = self.pv_temperature_nominal

        p = ir*A*H*(1 - Tcp*(cell_temp - Tstc))
        self.state.set_device_metric('pv', 'dc/power', p)
        p_active = self.pv_pcs_efficiency*p
        self.state.set_device_metric('pv', 'ac/active_power', p_active)

        # PV Voltage
        V_NOM = self.pv_voltage_nominal
        Tcv = self.pv_voltage_temperature_coefficient
        
        v = V_NOM*(1 - Tcv*(cell_temp - Tstc))
        self.state.set_device_metric('pv', 'dc/voltage', v)
        vac = self.ac_voltage_nominal + random.normalvariate(v - V_NOM, 0.000_1*self.ac_voltage_nominal)
        self.state.set_device_metric('pv', 'ac/voltage', vac)

        # PV Current
        i = p/v
        self.state.set_device_metric('pv', 'dc/current', i)
        iac = p_active/vac
        self.state.set_device_metric('pv', 'ac/current', iac)

        # Reactive Power
        self.state.set_device_metric('pv','ac/reactive_power', 0) 

        # Frequency
        f = random.normalvariate(50, 0.005)
        self.state.set_device_metric('pv','ac/frequency', f) 

    def set_bess_output_local(self, dt: float):
        noise = 0.000_3
        p = self.state.get_device_metric_value('bess', 'dc/power')
        p += random.normalvariate(0, noise*p)
        self.state.set_device_metric('bess', 'dc/power', p)
        p_ac = p*self.bess_pcs_efficiency
        self.state.set_device_metric('bess', 'ac/active_power', p_ac)

        v_dc = random.normalvariate(self.bess_nominal_voltage, noise*self.bess_nominal_voltage)
        self.state.set_device_metric('bess', 'dc/voltage', v_dc)
        self.state.set_device_metric('bess', 'dc/current', p/v_dc)

        v_ac = random.normalvariate(self.ac_voltage_nominal, noise*self.ac_voltage_nominal)
        self.state.set_device_metric('bess', 'ac/voltage', v_ac)
        self.state.set_device_metric('bess', 'ac/current', p_ac/v_ac)

        f = random.normalvariate(50, 0.005)
        self.state.set_device_metric('bess', 'ac/frequency', f)

    def set_bess_output_manual(self, dt: float):
        p = self.state.get_device_metric_value('bess', 'dc/power')
        p_sp = self.state.get_device_metric_value('bess', 'power_setpoint')
        ramp_rate = self.state.get_device_metric_value('bess', 'ramp_rate')
        # p_ac = self.state.get_device_metric_value('bess', 'ac/active_power')

        p_sp_actual = self.bess_power_setpoint_clamp(p_sp)
        self.state.set_device_metric('bess', 'power_setpoint_readback', p_sp_actual)

        noise = 0.000_5
        # p = self.bess_ac_p_control(p_sp_actual, p, )
        if p < p_sp_actual:
            if p + dt*ramp_rate > p_sp_actual:
                p = p_sp_actual + random.normalvariate(0, noise*p)
            else:
                p += dt*ramp_rate
        elif p > p_sp_actual:
            if p - dt*ramp_rate < p_sp_actual:
                p = p_sp_actual + random.normalvariate(0, noise*p)
            else:
                p -= dt*ramp_rate
        elif p == 0:
            pass
        else:
            p = p_sp_actual + random.normalvariate(0, noise*p)

        self.state.set_device_metric('bess', 'dc/power', p)
        p_ac = p*self.bess_pcs_efficiency
        self.state.set_device_metric('bess', 'ac/active_power', p_ac)

        v_dc = random.normalvariate(self.bess_nominal_voltage, noise*self.bess_nominal_voltage)
        self.state.set_device_metric('bess', 'dc/voltage', v_dc)
        self.state.set_device_metric('bess', 'dc/current', p/v_dc)

        v_ac = random.normalvariate(self.ac_voltage_nominal, noise*self.ac_voltage_nominal)
        self.state.set_device_metric('bess', 'ac/voltage', v_ac)
        self.state.set_device_metric('bess', 'ac/current', p_ac/v_ac)

        f = random.normalvariate(50, 0.005)
        self.state.set_device_metric('bess', 'ac/frequency', f)

    def set_bess_output_poc_control(self, dt: float, kc = 1, tau_i = 100):
        # POC
        poc_sp = self.state.get_device_metric_value('poc', 'power_setpoint')
        self.state.set_device_metric('poc', 'power_setpoint_readback', poc_sp)
        poc_p_ac = self.state.get_device_metric_value('poc', 'ac/active_power')
        # BESS
        bess_p_dc_sp = self.state.get_device_metric_value('bess', 'power_setpoint')
        bess_ramp_rate = self.state.get_device_metric_value('bess', 'ramp_rate')
        
        # PI Control
        error = poc_sp - poc_p_ac
        delta_bess_p_dc_sp = kc*(error - self.last_poc_error + dt/tau_i*error)
        self.last_poc_error = error

        # anti-windup
        if delta_bess_p_dc_sp > dt*bess_ramp_rate:
            delta_bess_p_dc_sp = dt*bess_ramp_rate
        elif delta_bess_p_dc_sp < -dt*bess_ramp_rate:
            delta_bess_p_dc_sp = -dt*bess_ramp_rate

        bess_p_dc_sp += delta_bess_p_dc_sp

        self.state.set_device_metric('bess', 'power_setpoint', bess_p_dc_sp)
        # Ramp battery to DC setpoint
        self.set_bess_output_manual(dt)

    def set_bess_soc(self, dt):
        p = self.state.devices['bess'].metrics['dc/power'].value
        e_max = self.state.devices['bess'].metrics['battery/capacity'].value
        # Convert from J to Wh in energy calculations
        if p > 0: # Discharging
            self.bess_stored_energy -= dt*p*(2 - self.bess_efficiency)/3_600
        elif p < 0: # Charging
            self.bess_stored_energy -= dt*p*self.bess_efficiency/3_600
        
        if self.bess_stored_energy > e_max:
            self.bess_stored_energy = e_max
        elif self.bess_stored_energy < 0:
            self.bess_stored_energy = 0

        soc = self.bess_stored_energy/e_max*100
        self.state.set_device_metric('bess', 'battery/soc', soc)

    def bess_power_setpoint_clamp(self, sp):
        discharge_max = self.state.get_device_metric_value('bess', 'dc/power_max')
        charge_max = -discharge_max
        soc = self.state.get_device_metric_value('bess', 'battery/soc')

        if soc < self.bess_soc_low_low:
            discharge_max = 0
        elif soc < self.bess_soc_low:
            discharge_max = discharge_max*(soc - self.bess_soc_low_low)/(self.bess_soc_low - self.bess_soc_low_low)
        elif soc > self.bess_soc_high and soc < self.bess_soc_high_high:
            charge_max = charge_max*(1 - (soc - self.bess_soc_high)/(self.bess_soc_high_high - self.bess_soc_high))
        elif soc > self.bess_soc_high_high:
            charge_max = 0

        if sp > discharge_max:
            sp = discharge_max
        elif sp < charge_max:
            sp = charge_max
        
        return sp
    
    @staticmethod
    def bess_ac_p_control(p_sp, p_dc, p_ac, ramp_rate, dt, kc = 10):
        p_dc_sp = kc*(p_sp - p_ac)
        
        if p_dc < p_dc_sp:
            if p_dc + dt*ramp_rate > p_dc_sp:
                p_dc = p_dc_sp
            else:
                p_dc += dt*ramp_rate
        elif p_dc > p_dc_sp:
            if p_dc - dt*ramp_rate < p_dc_sp:
                p_dc = p_dc_sp
            else:
                p_dc -= dt*ramp_rate
        elif p_dc == 0:
            pass
        else:
            p_dc = p_dc_sp
        
        return p_dc

    def set_poc_output(self):
        bess_p = self.state.get_device_metric_value('bess', 'ac/active_power')
        pv_p = self.state.get_device_metric_value('pv', 'ac/active_power')
        p = self.poc_efficiency*(bess_p + pv_p)
        self.state.set_device_metric('poc', 'ac/active_power', p)

        v_pv = self.state.get_device_metric_value('pv', 'ac/voltage')
        v_bess = self.state.get_device_metric_value('bess', 'ac/voltage')
        v_poc = (v_pv + v_bess)/2
        self.state.set_device_metric('poc', 'ac/voltage', v_poc)
        self.state.set_device_metric('poc', 'ac/current', v_poc)

    def handle_node_metric_change_event(self, event: spc.MetricChangeEvent):
        # Returns True to allow the NCMD to result in a node metric change and False otherwise.

        local_mode = self.state.get_node_metric_value('local_mode')
        if event.metric_name == 'disconnect':
            self.disconnect()

        elif event.metric_name == 'manual_mode':
            if not local_mode:
                self.state.set_node_metric('poc_sp_mode', not event.new_value)
                self.state.set_node_metric('mode_string', 'Manual' if event.new_value else 'PoC SP Control')
            return not local_mode # If local mode is True, do not allow state change

        elif event.metric_name == 'poc_sp_mode':
            if not local_mode:
                self.state.set_node_metric('manual_mode', not event.new_value)
                self.state.set_node_metric('mode_string', 'PoC SP Control' if event.new_value else 'Manual')
            return not local_mode # If local mode is True, do not allow state change
        
        return True

    def handle_device_change_event(self, event: spc.DeviceChangeEvent):
        # Returns True to allow the DCMD to result in device metric changes and False otherwise.

        if event.device_id == 'bess':
            for metric_change in event.metric_changes:
                if metric_change.metric_name == 'power_setpoint':
                    # Only allow change in manual mode
                    return self.state.get_node_metric_value('manual_mode')
        elif event.device_id == 'poc':
            for metric_change in event.metric_changes:
                if metric_change.metric_name == 'power_setpoint':
                    # Only allow change in point of connection control mode
                    return self.state.get_node_metric_value('poc_sp_mode')

def get_sim_client(client_id: str):
    client = SolarBess(client_id)
    client.set_id('VPP', client_id)
    birth_certificate_path = Path(__file__).parent / BIRTH_CERTIFICATE_FILENAME
    client.set_birth_certificate(spc.BirthCertificate.from_file(birth_certificate_path))
    client.username_pw_set('test-user','password')
    return client

clients = [get_sim_client(f'Site {i}') for i in range(1,5)]

for client in clients:
    client.start()
    client.connect(BROKER_ADDRESS, BROKER_PORT)
    time.sleep(0.2)
## Main Loop
while any(client.connected for client in clients):
    for client in clients:
        client.sim_loop()

        ## Publish Changes
        client.publish_changes()

    time.sleep(0.5)