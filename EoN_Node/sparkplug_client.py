import json
import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import sparkplug_b_pb2
from typing import List, Tuple
import copy
import time

NAMESPACE = 'spBv1.0'
NUMERIC_SPARKPLUG_METRIC_TYPES = {
    sparkplug.MetricDataType.Float,
    sparkplug.MetricDataType.Double,
    sparkplug.MetricDataType.Int8,
    sparkplug.MetricDataType.UInt8,
    sparkplug.MetricDataType.Int16,
    sparkplug.MetricDataType.UInt16,
    sparkplug.MetricDataType.Int32,
    sparkplug.MetricDataType.UInt32,
    sparkplug.MetricDataType.Int64,
    sparkplug.MetricDataType.UInt64,
}

NUMERIC_SPARKPLUG_PARAMETER_TYPES = {
    sparkplug.ParameterDataType.Float,
    sparkplug.ParameterDataType.Double,
    sparkplug.ParameterDataType.Int8,
    sparkplug.ParameterDataType.UInt8,
    sparkplug.ParameterDataType.Int16,
    sparkplug.ParameterDataType.UInt16,
    sparkplug.ParameterDataType.Int32,
    sparkplug.ParameterDataType.UInt32,
    sparkplug.ParameterDataType.Int64,
    sparkplug.ParameterDataType.UInt64,
}

def sparkplug_type_from_str(datatype_str: str) -> 'sparkplug.MetricDataType':
    """
    Gets the Sparkplug data type that corresponds to the given string.

    Args:
        datatype_str (str): The string representation of the data type.

    Returns:
        sparkplug.MetricDataType: The corresponding Sparkplug data type.

    Raises:
        ValueError: If the given data type string is invalid.
    """
    datatype_str = datatype_str.lower()
    if datatype_str == "bool":
        return sparkplug.MetricDataType.Boolean
    elif datatype_str == "string":
        return sparkplug.MetricDataType.String
    elif datatype_str == "float":
        return sparkplug.MetricDataType.Float
    elif datatype_str == "double":
        return sparkplug.MetricDataType.Double
    elif datatype_str == "int8":
        return sparkplug.MetricDataType.Int8
    elif datatype_str == "uint8":
        return sparkplug.MetricDataType.UInt8
    elif datatype_str == "int16":
        return sparkplug.MetricDataType.Int16
    elif datatype_str == "uint16":
        return sparkplug.MetricDataType.UInt16
    elif datatype_str == "int32":
        return sparkplug.MetricDataType.Int32
    elif datatype_str == "uint32":
        return sparkplug.MetricDataType.UInt32
    elif datatype_str == "int64":
        return sparkplug.MetricDataType.Int64
    elif datatype_str == "uint64":
        return sparkplug.MetricDataType.UInt64
    else:
        raise ValueError(f"Invalid datatype: {datatype_str}")

def sparkplug_param_type_from_str(datatype_str: str) -> 'sparkplug.ParameterDataType':
    """
    Gets the Sparkplug data type that corresponds to the given string.

    Args:
        datatype_str (str): The string representation of the data type.

    Returns:
        sparkplug.MetricDataType: The corresponding Sparkplug data type.

    Raises:
        ValueError: If the given data type string is invalid.
    """
    datatype_str = datatype_str.lower()
    if datatype_str == "bool":
        return sparkplug.ParameterDataType.Boolean
    elif datatype_str == "string":
        return sparkplug.ParameterDataType.String
    elif datatype_str == "float":
        return sparkplug.ParameterDataType.Float
    elif datatype_str == "double":
        return sparkplug.ParameterDataType.Double
    elif datatype_str == "int8":
        return sparkplug.ParameterDataType.Int8
    elif datatype_str == "uint8":
        return sparkplug.ParameterDataType.UInt8
    elif datatype_str == "int16":
        return sparkplug.ParameterDataType.Int16
    elif datatype_str == "uint16":
        return sparkplug.ParameterDataType.UInt16
    elif datatype_str == "int32":
        return sparkplug.ParameterDataType.Int32
    elif datatype_str == "uint32":
        return sparkplug.ParameterDataType.UInt32
    elif datatype_str == "int64":
        return sparkplug.ParameterDataType.Int64
    elif datatype_str == "uint64":
        return sparkplug.ParameterDataType.UInt64
    else:
        raise ValueError(f"Invalid datatype: {datatype_str}")

class Property():
    def __init__(self, name: str, datatype_str: str, value=None):
        self.name = name
        self.datatype_str = datatype_str
        self.datatype_sp = sparkplug_param_type_from_str(datatype_str)
        self.value = value
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        elif isinstance(other, type(self.value)):
            return self.value == other
        else:
            return False
    
    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __repr__(self):
        return f"Property('{self.name}', '{self.datatype_str}', {self.value})"

class Metric(): 
    def __init__(self, name: str, datatype_str: str, value=None, properties: dict = dict()):
        self.name = name
        self.datatype_str = datatype_str
        self.datatype_sp = sparkplug_type_from_str(datatype_str)
        if value:
            self.value = value
        else:
            self.value = self.default_from_sparkplug_type(self.datatype_sp)
        self.properties = [Property(name, prop.get('datatype', None), prop.get('value', None)) for name, prop in properties.items()]
        print(f'Created {self}')
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        elif isinstance(other, type(self.value)):
            return self.value == other
        else:
            return False
    
    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)
    
    def __repr__(self):
        return f"Metric(name: '{self.name}', datatype_str='{self.datatype_str}', value={self.value}, properties={self.properties})"

    @staticmethod
    def default_from_sparkplug_type(datatype: sparkplug.MetricDataType):
        """
        Returns the default value for a given sparkplug data type.

        Args:
        data_type (sparkplug.MetricDataType): The data type to return the default value for.

        Returns:
        The default value for the specified data type.
        """
        if datatype == sparkplug.MetricDataType.Boolean:
            return False
        elif datatype == sparkplug.MetricDataType.String:
            return ""
        elif datatype in NUMERIC_SPARKPLUG_METRIC_TYPES:
            return 0
        else:
            raise ValueError(f"Invalid datatype: {datatype}")

class Device():
    def __init__(self, name: str, metrics: dict[str, Metric]):
        self.name = name
        self.metrics = metrics

class MetricChangeEvent:
    def __init__(self, metric_name: str, metric: Metric, prev_value=None):
        if type(metric_name) is not str:
            raise TypeError(f'Error creating MetricChangeEvent. metric_name must be of type str, not {type(metric_name)}')
        if type(metric) is not Metric:
            raise TypeError(f'Error creating MetricChangeEvent for {metric_name}. metric must be of type Metric, not {type(metric)}')
        self.metric_name = metric_name
        self.new_value = metric.value
        self.datatype_sp = metric.datatype_sp
        if self.new_value is None:
            print(f'WARNING: new_value=None when creating MetricChangeEvent for {self.metric_name}')
    
    def __repr__(self):
        return f"MetricChangeEvent 'metric_name': {self.metric_name} 'new_value': {self.new_value}, 'datatype_sp': {self.datatype_sp}"

class DeviceChangeEvent:
    def __init__(self, device_id: str, metric_changes: list[MetricChangeEvent]):
        if type(device_id) is not str:
            raise TypeError(f'Error creating DeviceChangeEvent. device_id must be of type str, not {type(device_id)}')
        if type(metric_changes) is not list:
            raise TypeError(f'Error creating DeviceChangeEvent. device_id must be a lit of MetricChangeEvent, not {type(metric_changes)}')
        self.device_id = device_id
        self.metric_changes = metric_changes
    
    def __repr__(self):
        return f"DeviceChangeEvent('{self.device_id}', {self.metric_changes})"

class BirthCertificate:
    """
    Represents a Sparkplug birth certificate that contains information about the metrics and devices in a node.
    """
    
    def __init__(self, node_metrics: dict, devices: dict):
        """
        Initializes a new instance of the BirthCertificate class.

        Args:
            node_metrics (dict): A dictionary with the structure node_metrics[metric_name]['datatype', 'value']
            devices (dict): A dictionary with the structure devices[device_name][metric_name]['datatype', 'value']
        """
        #TODO check the structure of node metrics and device metrics 
        self.node_metrics = node_metrics
        self.devices = devices
    
    @staticmethod
    def from_file(filename: str) -> 'BirthCertificate':
        """
        Creates a new instance of the BirthCertificate class from a birth certificate JSON file.

        Args:
            filename (str): The path of the Sparkplug birth certificate file.

        Returns:
            BirthCertificate: A new instance of the BirthCertificate class.
        """
        with open(filename, 'r') as f:
            birth_certificate = json.load(f)
            node_metrics = birth_certificate['node']['metrics']
            devices = birth_certificate['node']['devices']
            return BirthCertificate(node_metrics, devices)

class State():

    def __init__(self, birth_certificate: BirthCertificate):
        """
        Creates a new instance of the `State` class with metrics initialized to their default values.
        
        Args:
        birth_certificate (BirthCertificate): An instance of the `BirthCertificate` class.
        """
        # self.node_metrics = {metric_name: self.get_default_value(data_type) 
        #     for metric_name, metric in birth_certificate.node_metrics.items()}
        self.node_metrics = {metric_name: Metric(metric_name, metric['datatype'], metric.get('value', None), metric.get('properties', {})) for metric_name, metric in birth_certificate.node_metrics.items()}
        
        self.devices = {}
        for device_name, device in birth_certificate.devices.items():
            device_metrics = {metric_name: Metric(metric_name, metric['datatype'], metric['value'], metric.get('properties', {})) for metric_name, metric in device['metrics'].items()}
            self.devices[device_name] = Device(device_name, device_metrics)

        self._old_node_metrics = copy.deepcopy(self.node_metrics)
        self._old_devices = {device_name: copy.deepcopy(device) for device_name, device in self.devices.items()}

    def set_node_metric(self, metric_name: str, value):
        """
        Sets the value of a node metric.

        Args:
        metric_name (str): The name of the metric.
        value (Any): The new value for the metric.

        Raises:
        ValueError: If the metric is not found in the node metrics.
        """
        if metric_name in self.node_metrics:
            self.node_metrics[metric_name].value = value
        else:
            raise ValueError(f"Metric {metric_name} not found in node metrics")

    def set_device_metric(self, device_name: str, metric_name: str, value):
        """
        Sets the value of a metric for a specified device.

        Args:
        device_name (str): The name of the device.
        metric_name (str): The name of the metric.
        value (Any): The new value for the metric.

        Raises:
        ValueError: If the device is not found in the devices or if the metric is not found for the specified device.
        """
        if device_name not in self.devices:
            raise ValueError(f"Device {device_name} not found in devices")
        if metric_name not in self.devices[device_name].metrics:
            raise ValueError(f"Metric {metric_name} not found in device {device_name} metrics")
        self.devices[device_name].metrics[metric_name].value = value

    def get_changes(self) -> Tuple[List[MetricChangeEvent], List[DeviceChangeEvent]]:
        """
        Compares the current state of the metrics to the previous state and returns the changes.

        Returns:
        A tuple containing the changes to the node metrics and device metrics respectively.
        """
        node_changes = [MetricChangeEvent(metric_name, metric) for metric_name, metric in self.node_metrics.items() if self._old_node_metrics[metric_name] != metric]
        self._old_node_metrics = copy.deepcopy(self.node_metrics)

        device_changes = []
        for device_name, device in self.devices.items():
            device_metric_changes = [MetricChangeEvent(metric_name, metric) for metric_name, metric in device.metrics.items() if self._old_devices[device_name].metrics[metric_name] != metric]
            if device_metric_changes:
                device_changes.append(DeviceChangeEvent(device_name, device_metric_changes))
            self._old_devices[device_name] = copy.deepcopy(device)

        return node_changes, device_changes
    
    def get_node_metric_value(self, metric_name: str):
        #TODO add context to key not found error
        return self.node_metrics[metric_name].value
    
    def get_device_metric_value(self, device_name: str, metric_name: str):
        #TODO add context to key not found error
        return self.devices[device_name].metrics[metric_name].value

class Client(mqtt.Client):
    def __init__(self, client_id):
        super().__init__(client_id)
        self.state = None
        self.event_buffer = []
        self.birth_certificate = None
        self.connected = False
        self.on_connect = self.sp_on_connect
        self.on_message = self.sp_on_message
        self.on_subscribe = self.sp_on_subscribe
        self.payload_seq = 0
        self.bd_seq = None
        self.handle_ncmd = self._handle_ncmd_default
        self.handle_dcmd = self._handle_dcmd_default


    @staticmethod
    def sp_on_connect(client, userdata, flags, rc):
        print('Connected!')
        print(f'flags: {flags}')
        print(f'rc: {rc}')
        client.subscribe(f"{NAMESPACE}/{client.group_id}/NCMD/{client.node_id}/#")
        client.subscribe(f"{NAMESPACE}/{client.group_id}/DCMD/{client.node_id}/#")
        client.connected = True
        client._publish_birth()

    @staticmethod
    def sp_on_subscribe(client, userdata, mid, granted_qos):
        pass
        # print(f'Subscribed to "{"something"}"')

    def set_birth_certificate(self, birth_certificate: BirthCertificate): 
        # Save birth certificate as a property
        self.birth_certificate = birth_certificate
        
        # Initialise state
        self.state = State(self.birth_certificate)

    def set_id(self, group_id, node_id):
        # Set the group id and node id for this client
        self.group_id = group_id
        self.node_id = node_id
        self.will_set(
            f'{NAMESPACE}/{group_id}/NDEATH/{node_id}',
            bytearray(sparkplug.getNodeDeathPayload().SerializeToString())
            )

    def connect(self, host, port, keep_alive=60):
        # Initiate and maintain MQTT connection. Subscribe to NCMD and DCMD sparkplug topics according to group 
        # and node ids

        self.broker_address = host
        self.broker_port = port
        self.keep_alive_time = keep_alive
        super().connect(self.broker_address, self.broker_port, self.keep_alive_time)
        self.loop_start()

    def disconnect(self):
        death_topic = f'{NAMESPACE}/{self.group_id}/NDEATH/{self.node_id}'
        payload = sparkplug_b_pb2.Payload()
        sparkplug.addMetric(payload, "bdSeq", None, sparkplug.MetricDataType.Int64, 0)

        death_payload_bytes = bytearray(payload.SerializeToString())
        self.publish(death_topic, death_payload_bytes)
        super().disconnect()
        self.connected = False
        print("Disconnected and sent NDEATH")

    def _handle_ncmd_default(client, event):
        # Placeholder for user handler
        return True
    
    def _handle_dcmd_default(client, event):
        # Placeholder for user handler
        return True

    def _handle_ncmd(self, inbound_payload):
        for metric in inbound_payload.metrics:
            if metric.name == "Node Control/Next Server":
                # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                # disconnect from the current MQTT server and connect to the next MQTT server in the
                # list of available servers.  This is used for clients that have a pool of MQTT servers
                # to connect to.
                print( "'Node Control/Next Server' is not implemented")
            elif metric.name == "Node Control/Rebirth":
                # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                # application if it receives an NDATA or DDATA with a metric that was not published in the
                # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                # its original NBIRTH and DBIRTH messages.
                self.set_seq(0)
                self._publish_birth()
            elif metric.name == "Node Control/Reboot":

                self._publish_birth()
            elif metric.name in self.birth_certificate.node_metrics:
                
                # Get value
                new_value = self.metric_value_from_type(metric, self.state.node_metrics[metric.name].datatype_sp)
                
                # Appnd to metric change event bufffer so that user can get new events from .inbound_events()
                metric_copy = copy.deepcopy(self.state.node_metrics[metric.name])
                metric_copy.value = new_value
                event = MetricChangeEvent(metric.name, metric_copy)
                # self.event_buffer.append(event)
                
                # Run user command handler to check whether to update state
                if not self.handle_ncmd(event):
                    continue

                # Update state
                self.state.set_node_metric(metric.name, new_value)
                print(f'Set {self.group_id}/{self.node_id}/{metric.name} to {new_value}')

            else:
                # Invalid metric
                print(f"Received NCMD with invalid metric {metric.name}. Ignoring.")

    def _handle_dcmd(self, device_name, inbound_payload):
        if device_name not in self.state.devices:
            print(f"Received DCMD with invalid device id {device_name}. Ignoring.")
            return
        
        metric_changes = []
        for metric in inbound_payload.metrics:
            if metric.name not in self.state.devices[device_name].metrics:
                print(f"Received DCMD with invalid metric {metric.name}. Ignoring this metric.")
            
            # Get value
            new_value = self.metric_value_from_type(metric, self.state.devices[device_name].metrics[metric.name].datatype_sp)

            # Appnd to metric change event buffer so that user can get new events from .inbound_events()
            metric_copy = copy.deepcopy(self.state.devices[device_name].metrics[metric.name])
            metric_copy.value = new_value
            metric_changes.append(MetricChangeEvent(metric.name, metric_copy))

        if metric_changes:
            event = DeviceChangeEvent(device_name, metric_changes)
            # self.event_buffer.append(event)

            # Run user command handler to check whether to update state
            if self.handle_dcmd(event):
                # Update state
                for change in event.metric_changes:
                    self.state.set_device_metric(device_name, change.metric_name, change.new_value)
                    print(f'Set {self.group_id}/{self.node_id}/{device_name}/{change.metric_name} to {change.new_value}')

    @staticmethod
    def sp_on_message(client, userdata, msg):
        print("Message arrived: " + msg.topic)
        tokens = msg.topic.split("/")

        if tokens[0] == "spBv1.0" and tokens[1] == client.group_id and tokens[3] == client.node_id:
            inbound_payload = sparkplug_b_pb2.Payload()
            inbound_payload.ParseFromString(msg.payload)
            if tokens[2] == 'NCMD': 
                client._handle_ncmd(inbound_payload)
            elif tokens[2] == 'DCMD':
                device_name = tokens[4]
                client._handle_dcmd(device_name, inbound_payload)

        client.publish_changes()


    def _publish_node_birth(self):
        # Create the node birth payload
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(round(time.time() * 1000))
        payload.seq = 0
        self.set_seq(payload.seq)
        # TODO track bdSeq over multiple birth-death cycles 
        sparkplug.addMetric(payload, "bdSeq", None, sparkplug.MetricDataType.Int64, 0)
        

        # Add node control metrics to payload
        sparkplug.addMetric(payload, "Node Control/Next Server", None, sparkplug.MetricDataType.Boolean, False)
        sparkplug.addMetric(payload, "Node Control/Rebirth", None, sparkplug.MetricDataType.Boolean, False)
        sparkplug.addMetric(payload, "Node Control/Reboot", None, sparkplug.MetricDataType.Boolean, False)

        # Add metrics from state to payload
        for metric_name, metric in self.state.node_metrics.items():
            sp_metric = sparkplug.addMetric(payload, metric_name, None, metric.datatype_sp, metric.value)
            
            # Add properties to metric
            if metric.properties:
                sp_metric.properties.keys.extend([prop.name for prop in metric.properties])
                for prop in metric.properties:
                    sp_prop = sp_metric.properties.values.add()
                    sp_prop.type = prop.datatype_sp
                    # This is dumb
                    if sp_prop.type == sparkplug.ParameterDataType.Boolean:
                        sp_prop.boolean_value = prop.value
                    elif sp_prop.type == sparkplug.ParameterDataType.String:
                        sp_prop.string_value = prop.value
                    else:
                        sp_prop.int_value = prop.value

        # Publish the node birth certificate
        byteArray = bytearray(payload.SerializeToString())
        self.publish(f"spBv1.0/{self.group_id}/NBIRTH/{self.node_id}", byteArray, 0, False)
        print(f'Published NBIRTH payload.seq = {payload.seq}')

    def _publish_device_birth(self, device_name: str):
        # Get the payload
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(round(time.time() * 1000))
        payload.seq = self.next_seq()

        # Add metrics from state for the given device
        for metric_name, metric in self.state.devices[device_name].metrics.items():
            sp_metric = sparkplug.addMetric(payload, metric_name, None, metric.datatype_sp, metric.value)

            # Add properties to metric
            if metric.properties:
                sp_metric.properties.keys.extend([prop.name for prop in metric.properties])
                for prop in metric.properties:
                    sp_prop = sp_metric.properties.values.add()
                    sp_prop.type = prop.datatype_sp
                    # This is dumb
                    if sp_prop.type == sparkplug.ParameterDataType.Boolean:
                        sp_prop.boolean_value = prop.value
                    elif sp_prop.type == sparkplug.ParameterDataType.String:
                        sp_prop.string_value = prop.value
                    else:
                        sp_prop.int_value = prop.value

        # Publish the device birth certificate
        byteArray = bytearray(payload.SerializeToString())
        self.publish(f"spBv1.0/{self.group_id}/DBIRTH/{self.node_id}/{device_name}", byteArray, 0, False)
        print(f'Published DBIRTH payload.seq = {payload.seq}')

    
    def _publish_birth(self):
        self._publish_node_birth()
        for device_name in self.birth_certificate.devices:
            self._publish_device_birth(device_name)

    def publish_changes(self):
        # Publish changes in state since last call
        node_changes, device_changes = self.state.get_changes()

        if node_changes:
            self.publish_node_changes(node_changes)

        if device_changes:
            self.publish_device_changes(device_changes)
        
    def publish_node_changes(self, node_changes: List[MetricChangeEvent]):
        payload = sparkplug.Payload()
        payload.seq = self.next_seq()

        for change in node_changes:
            sparkplug.addMetric(payload, change.metric_name, None, change.datatype_sp, change.new_value)
        topic = f'{NAMESPACE}/{self.group_id}/NDATA/{self.node_id}'
        self.publish(topic, bytearray(payload.SerializeToString()))

    def publish_device_changes(self, device_changes: List[DeviceChangeEvent]):
        for device_change in device_changes:
            payload = sparkplug.getDdataPayload()
            payload.seq = self.next_seq()

            device_name = device_change.device_id
            for metric_change in device_change.metric_changes:
                sparkplug.addMetric(payload, metric_change.metric_name, None, metric_change.datatype_sp, metric_change.new_value)
            topic = f'{NAMESPACE}/{self.group_id}/DDATA/{self.node_id}/{device_name}'

            self.publish(topic, bytearray(payload.SerializeToString()))

    def inbound_events(self, clear_buffer=True):
        events = self.event_buffer
        if clear_buffer:
            self.event_buffer = []
        return events

    def set_seq(self, new_seq):
        self.payload_seq = new_seq

    def next_seq(self):
        self.payload_seq += 1
        if self.payload_seq == 256:
            self.payload_seq = 0
        return self.payload_seq

    def _next_bd_seq(self):
        self.bd_seq += 1
        if self.bd_seq == 256:
            self.bd_seq = 0
        return self.bd_seq

    @staticmethod
    def metric_value_from_type(metric, data_type: sparkplug.MetricDataType):
        if data_type == sparkplug.MetricDataType.Boolean:
            return metric.boolean_value
        elif data_type == sparkplug.MetricDataType.Int8:
            return metric.int_value
        elif data_type == sparkplug.MetricDataType.UInt8:
            return metric.uint_value
        elif data_type == sparkplug.MetricDataType.Int16:
            return metric.int_value
        elif data_type == sparkplug.MetricDataType.UInt16:
            return metric.uint_value
        elif data_type == sparkplug.MetricDataType.Int32:
            return metric.int_value
        elif data_type == sparkplug.MetricDataType.UInt32:
            return metric.uint_value
        elif data_type == sparkplug.MetricDataType.Int64:
            return metric.int_value
        elif data_type == sparkplug.MetricDataType.UInt64:
            return metric.long_value
        elif data_type == sparkplug.MetricDataType.Float:
            return metric.float_value
        elif data_type == sparkplug.MetricDataType.Double:
            return metric.float_val
        elif data_type == sparkplug.MetricDataType.String:
            return metric.string_value
        else:
            return None