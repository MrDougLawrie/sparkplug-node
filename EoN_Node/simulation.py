

class WindFarm():
    def __init__(self, *args, **kwargs):
        self.t_prev = 0
        for name, value in kwargs:
            if name == '':
                pass

            elif name == '':
                pass

            else:
                raise NameError(f'Invalid keyword argument {name} when creating WindFarm')
            
    def loop(self, **kwargs):
        for name, value in kwargs:
            if name == 't':
                t = value
                dt = t - self.t_prev
            elif name == 'dt':
                dt = value
                t = self.t_prev + dt
