import sys
sys.path.insert(0, "../core/")
import json
import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import sparkplug_b_pb2

class State():

    def __init__(self, birth_certificate):
        """
        Creates a new instance of the `State` class with metrics initialized to their default values.
        
        Args:
        birth_certificate (BirthCertificate): An instance of the `BirthCertificate` class.
        """
        self.node_metrics = {metric_name: self.get_default_value(data_type) 
            for metric_name, data_type in birth_certificate.node_metrics.items()}
        self.devices = {}
        for device_name, device_metrics in birth_certificate.devices.items():
            self.devices[device_name] = {}
            for metric_name, data_type in device_metrics.items():
                self.devices[device_name][metric_name] = self.get_default_value(data_type)
        self.old_node_metrics = self.node_metrics.copy()
        self.old_devices = {device_name: device_metrics.copy() for device_name, device_metrics in self.devices.items()}

    def get_default_value(self, data_type: sparkplug.MetricDataType):
        """
        Returns the default value for a given sparkplug data type.

        Args:
        data_type (sparkplug.MetricDataType): The data type to return the default value for.

        Returns:
        The default value for the specified data type.
        """
        if data_type == sparkplug.MetricDataType.Boolean:
            return False
        elif data_type == sparkplug.MetricDataType.String:
            return ""
        else:
            return 0

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
            self.node_metrics[metric_name] = value
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
        if metric_name not in self.devices[device_name]:
            raise ValueError(f"Metric {metric_name} not found in device {device_name} metrics")
        self.devices[device_name][metric_name] = value

    def get_changes(self):
        """
        Compares the current state of the metrics to the previous state and returns the changes.

        Returns:
        A tuple containing the changes to the node metrics and device metrics respectively.
        """
        node_changes = {}
        device_changes = {}
        node_changes = [MetricChangeEvent(metric_name, value) for metric_name, value in self.node_metrics.items() if self.old_node_metrics[metric_name] != value]
        self.old_node_metrics = self.node_metrics.copy()

        device_changes = []
        for device_name, device_metrics in self.devices.items():
            device_metric_changes = [MetricChangeEvent(metric_name, value) for metric_name, value in device_metrics.items() if self.old_devices[device_name][metric_name] != value]
            device_changes += DeviceChangeEvent(device_name, device_metric_changes)
            self.old_devices[device_name] = device_metrics.copy()

        return node_changes, device_changes


class BirthCertificate:
    """
    Represents a Sparkplug birth certificate that contains information about the metrics and devices in a node.
    """

    def __init__(self, node_metrics: dict, devices: dict):
        """
        Initializes a new instance of the BirthCertificate class.

        Args:
            node_metrics (dict): A dictionary that maps metric names to their data types for the node.
            devices (dict): A dictionary that maps device names to a dictionary of their metrics and their data types.
        """
        self.node_metrics = node_metrics
        self.devices = devices
    
    @staticmethod
    def from_file(filename: str) -> 'BirthCertificate':
        """
        Creates a new instance of the BirthCertificate class from a Sparkplug birth certificate file.

        Args:
            filename (str): The path of the Sparkplug birth certificate file.

        Returns:
            BirthCertificate: A new instance of the BirthCertificate class.
        """
        with open(filename, 'r') as f:
            birth_certificate = json.load(f)
            node_metrics = {metric['name']: BirthCertificate.get_metric_datatype(metric['datatype']) for metric in birth_certificate['node']['metrics']}
            devices = {device['name']: {metric['name']: BirthCertificate.get_metric_datatype(metric['datatype']) for metric in device['metrics']} for device in birth_certificate['devices']}
            return BirthCertificate(node_metrics, devices)

    @staticmethod
    def get_metric_datatype(datatype_str: str) -> 'sparkplug.MetricDataType':
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

class MetricChangeEvent:
    def __init__(self, metric_name: str, new_value):
        self.metric_name = metric_name
        self.new_value = new_value

class DeviceChangeEvent:
    def __init__(self, device_id: str, metric_changes: list[MetricChangeEvent]):
        self.device_id = device_id
        self.metric_changes = metric_changes

class Client(mqtt.Client):
    def __init__(self, host, port, keep_alive):
        super().__init__()
        self.state = None
        self.event_buffer = []
        self.birth_certificate = None
        self.group_id = ''
        self.node_id = ''
        self.broker_address = host
        self.broker_port = port
        self.keep_alive_time = keep_alive
        self.connected = False
        self.on_connect = self._on_connect
        self.on_message = self._on_message

    def set_birth_certificate(self, birth_certificate: BirthCertificate):
        # Save birth certificate as a property
        self.birth_certificate = birth_certificate
        
        # Initialise state
        self.state = State(self.birth_certificate)

    def set_id(self, group_id, node_id):
        # Set the group id and node id for this client
        self.group_id = group_id
        self.node_id = node_id

    def connect(self):
        # Initiate and maintain MQTT connection. Subscribe to NCMD and DCMD sparkplug topics according to group 
        # and node ids
        self.connect(self.broker_address, self.broker_port, self.keep_alive_time)

    def _on_connect(self, userdata, flags, rc):
        self.subscribe("spBv1.0/" + self.group_id + "/" + self.node_id + "/DCMD/#")
        self.subscribe("spBv1.0/" + self.group_id + "/" + self.node_id + "/NCMD/#")
        self.connected = True
        self._publish_birth()

    def _handle_ncmd(self, inbound_payload):
        for metric in inbound_payload.metrics:
            if metric.name == "Node Control/Next Server":
                # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                # disconnect from the current MQTT server and connect to the next MQTT server in the
                # list of available servers.  This is used for clients that have a pool of MQTT servers
                # to connect to.
                print( "'Node Control/Next Server' is not implemented in this example")
            elif metric.name == "Node Control/Rebirth":
                # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                # application if it receives an NDATA or DDATA with a metric that was not published in the
                # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                # its original NBIRTH and DBIRTH messages.
                self._publish_birth()
            elif metric.name == "Node Control/Reboot":

                self._publish_birth()
            elif metric.name in self.birth_certificate.node_metrics:
                # Get value
                new_value = self.metric_value_from_type(metric, self.birth_certificate.node_metrics[metric.name])
                        
                # Update state
                self.state.set_node_metric(metric.name, new_value)

                # Appnd to metric change event buffer so that user can get new events from .inbound_events()
                self.event_buffer.append(MetricChangeEvent(metric.name, new_value))
            else:
                # Invalid metric
                print(f"Received NCMD with invalid metric {metric.name}. Ignoring.")


    def _handle_dcmd(self, tokens, inbound_payload):
        device_name = tokens[4]
        if device_name not in self.state.devices:
            print(f"Received DCMD with invalid device id {device_name}. Ignoring.")
            return
        
        metric_changes = []
        for metric in inbound_payload.metrics:
            if metric not in self.birth_certificate.devices[device_name]:
                print(f"Received DCMD with invalid metric {metric.name}. Ignoring this metric.")
            
            # Get value
            new_value = self.metric_value_from_type(metric, self.birth_certificate.node_metrics[metric.name])

            # Update state
            self.state.set_node_metric(metric.name, new_value)
            
            # Appnd to metric change event buffer so that user can get new events from .inbound_events()
            metric_changes.append(MetricChangeEvent(device_name, metric.name, new_value))
        if metric_changes:
            self.event_buffer.append(DeviceChangeEvent(device_name, metric_changes))


    def _on_message(self, userdata, msg):
        print("Message arrived: " + msg.topic)
        tokens = msg.topic.split("/")

        if tokens[0] == "spBv1.0" and tokens[1] == self.group_id and tokens[3] == self.node_id:
            inbound_payload = sparkplug_b_pb2.Payload()
            inbound_payload.ParseFromString(msg.payload)
            if tokens[2] == 'NCMD': 
                self._handle_ncmd(inbound_payload)
            elif tokens[2] == 'DCMD':
                self._handle_dcmd(tokens, inbound_payload)

        self.publish_changes()


    def _publish_node_birth(self):
        print("Publishing Node Birth")

        # Create the node birth payload
        payload = sparkplug.Payload()

        # Add node control metrics
        sparkplug.addMetric(payload, "Node Control/Next Server", None, sparkplug.MetricDataType.Boolean, False)
        sparkplug.addMetric(payload, "Node Control/Rebirth", None, sparkplug.MetricDataType.Boolean, False)
        sparkplug.addMetric(payload, "Node Control/Reboot", None, sparkplug.MetricDataType.Boolean)

        # Add metrics from birth certificate
        for metric_name, metric_data_type in self.birth_certificate.metrics.items():
            sparkplug.addMetric(payload, metric_name, None, metric_data_type, 0)

        # Publish the node birth certificate
        byteArray = bytearray(payload.SerializeToString())
        self.publish(f"spBv1.0/{self.group_id}/NBIRTH/{self.node_id}", byteArray, 0, False)

    def _publish_device_birth(self, device_name: str):
        print(f"Publishing Birth Certificate for {device_name}")

        # Get the payload
        payload = sparkplug.getDeviceBirthPayload()

        # Add metrics from birth certificate for the given device
        device_metrics = self.birth_certificate.devices.get(device_name, {})
        for metric_name, metric_data_type in device_metrics.items():
            sparkplug.addMetric(payload, metric_name, None, metric_data_type, 0)

        # Publish the device birth certificate
        byteArray = bytearray(payload.SerializeToString())
        self.publish(f"spBv1.0/{self.group_id}/DBIRTH/{self.node_id}/{device_name}", byteArray, 0, False)
    
    def _publish_birth(self):
        self._publish_node_birth()
        for device_name in self.birth_certificate.device_metrics:
            self._publish_device_birth(device_name)

    def publish_changes(self):
        # Publish changes in state since last call
        node_changes, device_changes = self.state.get_changes()
    
        payload = sparkplug.Payload()
        for change in node_changes:
            data_type = self.birth_certificate.node_metrics[change.metric_name]
            sparkplug.addMetric(payload, change.metric_name, None, data_type, change.new_value)
        topic = "spBv1.0/" + self.group_id + "/NDATA" + self.node_id
        self.publish(topic, payload.SerializeToString())

        for device_change in device_changes:
            payload = sparkplug.Payload()
            device_name = device_change.device_id
            for metric_change in device_change.metric_changes:
                metric_name = metric_change.metric_name
                metric_value = metric_change.new_value
                data_type = self.birth_certificate.device_metrics[device_name][metric_name]
                sparkplug.addMetric(payload, metric_name, None, data_type, metric_value)
            topic = "spBv1.0/" + self.group_id + "/DDATA" + self.node_id + "/" + device_name

            self.publish(topic, payload.SerializeToString())
    
    def inbound_events(self, clear_buffer=True):
        events = self.event_buffer
        if clear_buffer:
            self.event_buffer = []
        return events

    @staticmethod
    def metric_value_from_type(metric, data_type: sparkplug.MetricDataType):
        if data_type == sparkplug.MetricDataType.Boolean:
            return metric.bool_value
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
            return metric.uint_value
        elif data_type == sparkplug.MetricDataType.Float32:
            return metric.float_value
        elif data_type == sparkplug.MetricDataType.Double64:
            return metric.double_value
        elif data_type == sparkplug.MetricDataType.String:
            return metric.string_value
        else:
            return None