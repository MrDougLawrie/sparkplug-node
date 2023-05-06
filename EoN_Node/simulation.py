import time
import numpy as np

class RealTimeSimulation():
    def __init__(self, *args, **kwargs):
        print(f'Initiating {type(self)}')
        self.children = {}
        self.children_outputs = {}
        self.rtf = 1
        self.max_step_secs = 1

        for key, value in kwargs.items():

            if key == 'children':
                if type(value) is not dict:
                    raise TypeError(f'Children must be a dictionary of RealTimeSimulation, not {type(value)}')
                for child_name, child in value.items():
                    if not isinstance(child, RealTimeSimulation):
                        raise TypeError(f'Simulation children must be an instance RealTimeSimulation or another subclass, not {type(child)}')
                self.children = value

            elif key == 'rtf':
                if type(value) is not float:
                    raise TypeError(f'rtf (real time factor) must of type float, not {type(value)}')
                self.rtf = value

            elif key == 'max_step_secs':
                if type(value) is not float:
                    raise TypeError(f'max_step_secs must of type float, not {type(value)}')
                self.max_step_secs = value

            else:
                raise KeyError(f'Invalid keyword argument {key} while initiating {type(self)}')

        self.sync_child_props()

    def add_children(self, children: dict):
        self.children.update(children)

    def sync_child_props(self):
        for child in self.children.values():
            child.rtf = self.rtf
            child.max_step_secs = self.max_step_secs
            child.sync_child_props()

    def start(self, t: float=None):
        self.start_time = t or time.time() # equivilant to x = t if t else time.time()
        self.last_sim_time = 0
        self.last_real_time = self.start_time
        for child in self.children.values():
            child.start(self.start_time)
        print(f'{self} started at {self.start_time}')

    def sim_loop(self, t, dt, inputs: dict = None, *args, **kwargs) -> dict:
        print('sim_loop', self)
        return {}

    def loop(self, sim_time: float = None, sim_dt: float = None):

        if not (sim_time and sim_dt):
            real_dt = time.time() - self.last_real_time
            sim_dt = real_dt*self.rtf
            self.sim_time = self.last_sim_time + sim_dt
        else:
            self.sim_time = sim_time

        if sim_dt > self.max_step_secs:
            for t in np.arange(self.last_sim_time, self.sim_time, self.max_step_secs):
                for child in self.children.values():
                    child.loop(t, self.max_step_secs)
                self.sim_loop(t, self.max_step_secs)
        else:
            for child in self.children.values():
                child.loop(self.sim_time, sim_dt)
            self.sim_loop(self.sim_time, sim_dt)

    def __repr__(self):
        return f"{type(self).__name__}"

class SolarSystem(RealTimeSimulation):
    # Inputs:
    # Outputs: 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Specifications
        self.efficiency = 0.15       # 
        self.area = 20    # m^2

        # External Metrics
        self.irradiance = 0     # m/s
        self.ambient_temperature = 25    # degrees C

        # Internal Metrics
        self.p = 0  # active power at PoC W
    

class Bess(RealTimeSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class SolarBatterySystem(RealTimeSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.children = [SolarSystem(), Bess()]

class VirtualPowerPlant(RealTimeSimulation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

def main():
    sim = VirtualPowerPlant(children = {f'PVBESS{i}': SolarBatterySystem() for i in range(3)})
    sim.start()
    sim.loop()

if __name__ == '__main__':
    main()