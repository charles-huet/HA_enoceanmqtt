import os
import tempfile
import unittest

from test_bridge_state import LoopOnceEnocean, install_fake_dependencies

install_fake_dependencies()

# pylint: disable=wrong-import-position
from enoceanmqtt.communicator import Communicator
from enoceanmqtt.overlays.homeassistant.ha_communicator import HACommunicator


class OrderedEnocean(LoopOnceEnocean):
    '''Records when the base ID is read'''
    def __init__(self, events):
        super().__init__()
        self._events = events
        self._base_id = self.base_id

    @property
    def base_id(self):
        self._events.append(('base_id',))
        return self._base_id

    @base_id.setter
    def base_id(self, value):
        self._base_id = value


class SlowBaseIdEnocean(LoopOnceEnocean):
    '''Answers the base ID request after a few attempts'''
    def __init__(self, events, attempts):
        self._events = events
        self._attempts = attempts
        super().__init__()

    @property
    def base_id(self):
        self._events.append(('base_id',))
        self._attempts -= 1
        if self._attempts > 0:
            return None
        self._events.append(('base_id_answered',))
        return self._base_id

    @base_id.setter
    def base_id(self, value):
        self._base_id = value


class StartupTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.config = {
            'mqtt_host': 'localhost',
            'enocean_port': '/dev/ttyUSB0',
            'mqtt_prefix': 'enoceanmqtt/',
            'mqtt_discovery_prefix': 'homeassistant/',
            'db_file': os.path.join(self.tmpdir.name, 'db.json'),
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_mqtt_is_not_connected_during_initialization(self):
        for cls in (Communicator, HACommunicator):
            comm = cls(self.config, [])
            self.assertNotIn('loop_start', [event[0] for event in comm.mqtt.events])
            self.assertIsNotNone(comm.enocean)

    def test_base_id_is_read_before_connecting_to_mqtt(self):
        comm = Communicator(self.config, [])
        comm.enocean = OrderedEnocean(comm.mqtt.events)
        comm.run()

        events = [event[0] for event in comm.mqtt.events]
        self.assertLess(events.index('base_id'), events.index('connect_async'))
        self.assertLess(events.index('connect_async'), events.index('loop_start'))
        self.assertEqual(comm.enocean_sender, [0x11, 0x22, 0x33, 0x44])

    def test_base_id_is_requested_again_before_connecting_to_mqtt(self):
        comm = Communicator(self.config, [])
        comm.enocean = SlowBaseIdEnocean(comm.mqtt.events, 3)
        comm.run()

        events = [event[0] for event in comm.mqtt.events]
        self.assertLess(events.index('base_id_answered'), events.index('connect_async'))
        self.assertEqual(comm.enocean_sender, [0x11, 0x22, 0x33, 0x44])

    def test_mqtt_is_connected_without_base_id_after_some_attempts(self):
        comm = Communicator(self.config, [])
        comm.enocean = SlowBaseIdEnocean(comm.mqtt.events, 100)
        with self.assertLogs(level='ERROR') as logs:
            comm.run()

        self.assertIn('Base ID of the transceiver not received', logs.output[0])
        events = [event[0] for event in comm.mqtt.events]
        self.assertEqual(events[:events.index('connect_async')].count('base_id'), comm.BASE_ID_ATTEMPTS)

    def test_ha_connection_handler_runs_right_after_initialization(self):
        comm = HACommunicator(self.config, [])
        # run() reads the base ID before connecting
        comm.enocean_sender = [0x11, 0x22, 0x33, 0x44]
        comm._on_connect(comm.mqtt, None, None, 0)
        learn = [item for item in comm.mqtt.published if item['topic'] == 'enoceanmqtt/__system/learn']
        self.assertEqual(learn[-1]['payload'], 'OFF')

    def test_nothing_is_sent_without_sender(self):
        sensor = {'name': 'enoceanmqtt/device', 'address': 0x058EDA56, 'rorg': 0xD2, 'func': 0x01, 'type': 0x01}
        comm = Communicator(self.config, [sensor])
        comm.enocean.send = self.fail
        with self.assertLogs(level='ERROR'):
            comm._send_packet(sensor, [0x05, 0x8E, 0xDA, 0x56])


if __name__ == '__main__':
    unittest.main()
