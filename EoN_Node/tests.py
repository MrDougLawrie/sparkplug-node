import os

import unittest
import sparkplug_b as sparkplug
from sparkplug_client import *

class TestState(unittest.TestCase):

    def setUp(self):
        birth_certificate = BirthCertificate(node_metrics={
            "temperature": sparkplug.MetricDataType.Float,
            "humidity": sparkplug.MetricDataType.Float,
            "is_active": sparkplug.MetricDataType.Boolean
        }, devices={
            "device1": {
                "voltage": sparkplug.MetricDataType.Float,
                "current": sparkplug.MetricDataType.Float
            },
            "device2": {
                "status": sparkplug.MetricDataType.String
            }
        })
        self.state = State(birth_certificate)

    def test_initial_node_metrics(self):
        expected = {
            "temperature": 0.0,
            "humidity": 0.0,
            "is_active": False
        }
        self.assertEqual(self.state.node_metrics, expected)

    def test_initial_devices(self):
        expected = {
            "device1": {
                "voltage": 0.0,
                "current": 0.0
            },
            "device2": {
                "status": ""
            }
        }
        self.assertEqual(self.state.devices, expected)
        

    def test_set_node_metric(self):
        self.state.set_node_metric("temperature", 25.5)
        self.assertEqual(self.state.node_metrics["temperature"], 25.5)
        self.state.set_node_metric("is_active", True)
        self.assertEqual(self.state.node_metrics["is_active"], True)

        with self.assertRaises(ValueError):
            self.state.set_node_metric("unknown_metric", 10)

    def test_set_device_metric(self):
        self.state.set_device_metric("device1", "voltage", 12.3)
        self.assertEqual(self.state.devices["device1"]["voltage"], 12.3)
        self.state.set_device_metric("device2", "status", "OK")
        self.assertEqual(self.state.devices["device2"]["status"], "OK")

        with self.assertRaises(ValueError):
            self.state.set_device_metric("unknown_device", "voltage", 10)

        with self.assertRaises(ValueError):
            self.state.set_device_metric("device1", "unknown_metric", 10)

    def test_get_changes(self):
        self.state.set_node_metric("temperature", 25.5)
        self.state.set_node_metric("is_active", True)
        self.state.set_device_metric("device1", "voltage", 12.3)
        self.state.set_device_metric("device2", "status", "OK")

        node_changes, device_changes = self.state.get_changes()

        expected_node_changes = {
            "temperature": 25.5,
            "is_active": True
        }
        self.assertEqual(node_changes, expected_node_changes)

        expected_device_changes = {
            "device1": {
                "voltage": 12.3
            },
            "device2": {
                "status": "OK"
            }
        }
        self.assertEqual(device_changes, expected_device_changes)

class TestBirthCertificate(unittest.TestCase):

    def test_init(self):
        node_metrics = {
            "solar_power": sparkplug.MetricDataType.Float,
            "load_power": sparkplug.MetricDataType.Float,
            "inverter_status": sparkplug.MetricDataType.Boolean,
            "inverter_fault_code": sparkplug.MetricDataType.UInt16,
            "ip_address": sparkplug.MetricDataType.String,
            "uptime": sparkplug.MetricDataType.UInt64
        }
        devices = {
            "solar_panel": {
                "voltage": sparkplug.MetricDataType.Float,
                "current": sparkplug.MetricDataType.Float
            },
            "battery": {
                "voltage": sparkplug.MetricDataType.Float,
                "current": sparkplug.MetricDataType.Float,
                "temperature": sparkplug.MetricDataType.Float,
                "state_of_charge": sparkplug.MetricDataType.UInt16
            }
        }
        birth_certificate = BirthCertificate(node_metrics, devices)
        self.assertEqual(birth_certificate.node_metrics, node_metrics)
        self.assertEqual(birth_certificate.devices, devices)

    def test_from_file(self):
        here = os.path.dirname(os.path.abspath(__file__))
        birth_certificate = BirthCertificate.from_file(os.path.join(here,'birth_certificate.json'))
        node_metrics = {
            "solar_power": sparkplug.MetricDataType.Float,
            "load_power": sparkplug.MetricDataType.Float,
            "inverter_status": sparkplug.MetricDataType.Boolean,
            "inverter_fault_code": sparkplug.MetricDataType.UInt16,
            "ip_address": sparkplug.MetricDataType.String,
            "uptime": sparkplug.MetricDataType.UInt64
        }
        devices = {
            "solar_panel": {
                "voltage": sparkplug.MetricDataType.Float,
                "current": sparkplug.MetricDataType.Float
            },
            "battery": {
                "voltage": sparkplug.MetricDataType.Float,
                "current": sparkplug.MetricDataType.Float,
                "temperature": sparkplug.MetricDataType.Float,
                "state_of_charge": sparkplug.MetricDataType.UInt16
            }
        }
        self.assertEqual(birth_certificate.node_metrics, node_metrics)
        self.assertEqual(birth_certificate.devices, devices)

    def test_get_metric_datatype(self):
        self.assertEqual(BirthCertificate.get_metric_datatype("bool"), sparkplug.MetricDataType.Boolean)
        self.assertEqual(BirthCertificate.get_metric_datatype("string"), sparkplug.MetricDataType.String)
        self.assertEqual(BirthCertificate.get_metric_datatype("float"), sparkplug.MetricDataType.Float)
        self.assertEqual(BirthCertificate.get_metric_datatype("double"), sparkplug.MetricDataType.Double)
        self.assertEqual(BirthCertificate.get_metric_datatype("int8"), sparkplug.MetricDataType.Int8)
        self.assertEqual(BirthCertificate.get_metric_datatype("uint8"), sparkplug.MetricDataType.UInt8)
        self.assertEqual(BirthCertificate.get_metric_datatype("int16"), sparkplug.MetricDataType.Int16)
        self.assertEqual(BirthCertificate.get_metric_datatype("uint16"), sparkplug.MetricDataType.UInt16)
        self.assertEqual(BirthCertificate.get_metric_datatype("int32"), sparkplug.MetricDataType.Int32)
        self.assertEqual(BirthCertificate.get_metric_datatype("uint32"), sparkplug.MetricDataType.UInt32)
        self.assertEqual(BirthCertificate.get_metric_datatype("int64"), sparkplug.MetricDataType.Int64)
        self.assertEqual(BirthCertificate.get_metric_datatype("uint64"), sparkplug.MetricDataType.UInt64)
        with self.assertRaises(ValueError):
            BirthCertificate.get_metric_datatype("invalid")


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBirthCertificate))
    suite.addTest(unittest.makeSuite(TestState))
    unittest.TextTestRunner().run(suite)