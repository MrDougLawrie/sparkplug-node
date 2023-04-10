#
#/********************************************************************************
# MU MQTT/Sparkplug with Ignition Demonstration
# Doug Lawrie
# ********************************************************************************/
import sys
sys.path.insert(0, "../core/")
#print(sys.path)

import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import time
import random
import string

import socket

from sparkplug_b import *

import copy
# I/O
import RPi.GPIO as gpio

# GPIO SETUP
BUTTON_PIN = 18
gpio.setmode(gpio.BCM)
gpio.setup(BUTTON_PIN, gpio.IN, pull_up_down=gpio.PUD_UP)

# Application Variables
serverUrl = "192.168.20.15"
myGroupId = "Sparkplug B Devices"
myNodeName = "Raspberry Pi 1"
myDeviceName = "ESP32"
publishPeriod = 5000
myUsername = "test_user"
myPassword = "password"

######################################################################
# The callback for when the client receives a CONNACK response from the server.
######################################################################
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected with result code "+str(rc))
    else:
        print("Failed to connect with result code "+str(rc))
        sys.exit()

    global myGroupId
    global myNodeName

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("spBv1.0/" + myGroupId + "/NCMD/" + myNodeName + "/#")
    client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName + "/#")
######################################################################

######################################################################
# The callback for when a PUBLISH message is received from the server.
######################################################################
def on_message(client, userdata, msg):
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and (tokens[2] == "NCMD" or tokens[2] == "DCMD") and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metrics:
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
                publishBirth()
            elif metric.name == "Node Control/Reboot":
                # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                # This can be used for devices that need a full application reset via a soft reboot.
                # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                # messages.
                publishBirth()
            elif metric.name == "output/Device Metric2":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Int16 because of how we declated it in the DBIRTH
                newValue = metric.int_value
                print( "CMD message for output/Device Metric2 - New Value: {}".format(newValue))

                # Create the DDATA payload
                payload = sparkplug.getDdataPayload()
                addMetric(payload, None, None, MetricDataType.Int16, newValue)

                # Publish a message data
                byteArray = bytearray(payload.SerializeToString())
                client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)
            elif metric.name == "output/Device Metric3":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Boolean because of how we declated it in the DBIRTH
                newValue = metric.boolean_value
                print( "CMD message for output/Device Metric3 - New Value: %r" % newValue)

                # Create the DDATA payload
                payload = sparkplug.getDdataPayload()
                addMetric(payload, None, None, MetricDataType.Boolean, newValue)

                # Publish a message data
                byteArray = bytearray(payload.SerializeToString())
                client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)
            else:
                print( "Unknown command: " + metric.name)
    else:
        print( "Unknown command...")

    print( "Done publishing")
######################################################################

######################################################################
# Publish the BIRTH certificates
######################################################################
def publishBirth():
    publishNodeBirth()
    publishDeviceBirth()
######################################################################

######################################################################
# Publish the NBIRTH certificate
######################################################################
def publishNodeBirth():
    print( "Publishing Node Birth")

    # Create the node birth payload
    payload = sparkplug.getNodeBirthPayload()

    # Set up the Node Controls
    addMetric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    addMetric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

    # Add some regular node metrics
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    addMetric(payload, "hostname", None, MetricDataType.String, hostname)
    addMetric(payload, "ip_address", None, MetricDataType.String, ip_address)
    addMetric(payload, "Node Metric1", None, MetricDataType.Boolean, True)
    addNullMetric(payload, "Node Metric3", None, MetricDataType.Int32)

    # Create a DataSet (012 - 345) two rows with Int8, Int16, and Int32 contents and headers Int8s, Int16s, Int32s and add it to the payload
    columns = ["Int8s", "Int16s", "Int32s"]
    types = [DataSetDataType.Int8, DataSetDataType.Int16, DataSetDataType.Int32]
    dataset = initDatasetMetric(payload, "DataSet", None, columns, types)
    row = dataset.rows.add()
    element = row.elements.add();
    element.int_value = 0
    element = row.elements.add();
    element.int_value = 1
    element = row.elements.add();
    element.int_value = 2
    row = dataset.rows.add()
    element = row.elements.add();
    element.int_value = 3
    element = row.elements.add();
    element.int_value = 4
    element = row.elements.add();
    element.int_value = 5

    # Add a metric with a custom property
    metric = addMetric(payload, "AmbientTemperature", None, MetricDataType.Int16, 22)
    metric.properties.keys.extend(["EngUnit"])
    propertyValue = metric.properties.values.add()
    propertyValue.type = ParameterDataType.String
    propertyValue.string_value = "°C"

    # Create the UDT definition value which includes two UDT members and a single parameter and add it to the payload
    template = initTemplateMetric(payload, "_types_/Custom_Motor", None, None)
    templateParameter = template.parameters.add()
    templateParameter.name = "Index"
    templateParameter.type = ParameterDataType.String
    templateParameter.string_value = "0"
    addMetric(template, "RPMs", None, MetricDataType.Int32, 0)
    addMetric(template, "AMPs", None, MetricDataType.Int32, 0)

    # Publish the node birth certificate
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)
######################################################################

######################################################################
# Publish the DBIRTH certificate
######################################################################
def publishDeviceBirth():
    print( "Publishing Device Birth")

    # Get the payload
    payload = sparkplug.getDeviceBirthPayload()

    # Add some device metrics
    addMetric(payload, "input/string_metric", None, MetricDataType.String, '')
    addMetric(payload, "input/boolean_metric", None, MetricDataType.Boolean, True)
    # addMetric(payload, "output/Device Metric2", None, MetricDataType.Int16, 16)
    # addMetric(payload, "output/Device Metric3", None, MetricDataType.Boolean, True)
    # addMetric(payload, "DateTime Metric", None, MetricDataType.DateTime, long(time.time() * 1000))

    # Create the UDT definition value which includes two UDT members and a single parameter and add it to the payload
    # template = initTemplateMetric(payload, "My_Custom_Motor", None, "Custom_Motor")
    # templateParameter = template.parameters.add()
    # templateParameter.name = "Index"
    # templateParameter.type = ParameterDataType.String
    # templateParameter.string_value = "1"
    # addMetric(template, "RPMs", None, MetricDataType.Int32, 3000)
    # addMetric(template, "AMPs", None, MetricDataType.Int32, 1200)

    # Publish the initial data with the Device BIRTH certificate
    totalByteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/DBIRTH/" + myNodeName + "/" + myDeviceName, totalByteArray, 0, False)
######################################################################

######################################################################
# Main Application
######################################################################
print("Starting main application")

# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()

# Start of main program - Set up the MQTT client connection
client = mqtt.Client(serverUrl, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
deathByteArray = bytearray(deathPayload.SerializeToString())
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(serverUrl, 1883, 60)

# Short delay to allow connect callback to occur
time.sleep(.1)
client.loop()

# Init

string_metric = ''
last_button_state = False
current_state = {
    'input/string_metric': {'value': 'HELLO DEVICE', 'datatype': MetricDataType.String},
    'input/boolean_metric':  {'value': True, 'datatype': MetricDataType.Boolean},
    }
old_state = copy.deepcopy(current_state)

# Publish the birth certificates
publishBirth()

loop_count = 0
while True:
    

    #
    if loop_count % 13 == 0: 
        string_metric = 'hello device' if string_metric == 'HELLO DEVICE' else 'HELLO DEVICE'
    
    new_button_state = gpio.input(BUTTON_PIN)
    if last_button_state != new_button_state:
        print(new_button_state)
    last_button_state = new_button_state
    
    current_state['input/string_metric']['value'] = string_metric
    current_state['input/boolean_metric']['value'] = bool(new_button_state)
    # print('current state', current_state)
    # print('old state', old_state)
    # addMetric(payload, "input/Device string_metric", None, MetricDataType.String, string_metric)
    # addMetric(payload, "input/boolean metric", None, MetricDataType.Boolean, True)
    # addMetric(payload, "output/Device Metric2", None, MetricDataType.Int16, 16)
    # addMetric(payload, "output/Device Metric3", None, MetricDataType.Boolean, True)

    # Add some random data to the inputs
    # addMetric(payload, None, None, MetricDataType.String, ''.join(random.choice(string.ascii_lowercase) for i in range(12)))

    # Note this data we're setting to STALE via the propertyset as an example
    # metric = addMetric(payload, None, None, MetricDataType.Boolean, random.choice([True, False]))
    # metric.properties.keys.extend(["Quality"])
    # propertyValue = metric.properties.values.add()
    # propertyValue.type = ParameterDataType.Int32
    # propertyValue.int_value = 500
    print(f'Change occured: {current_state != old_state}')
    if current_state != old_state:
        payload = sparkplug.getDdataPayload()
        for topic, value_dict in current_state.items():
            
            if old_state[topic] == value_dict:
                continue
            print('Change')
            print(topic, value_dict)
            datatype = value_dict['datatype']
            value = value_dict['value']
            addMetric(payload, topic, None, datatype, value)
        # Publish a message data
        byteArray = bytearray(payload.SerializeToString())
        client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)
        print(f'Published payload')

        old_state = copy.deepcopy(current_state)
    
    loop_count += 1
    client.loop()
    time.sleep(.1)
######################################################################
