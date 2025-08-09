###########EXTERNAL IMPORTS############

import pytest

#######################################

#############LOCAL IMPORTS#############

from controller.registry import ProtocolRegistry, _modbus_rtu_node_factory, _opcua_node_factory
from controller.types import Protocol

#######################################


@pytest.fixture(autouse=True)
def reset_registry():
    original = ProtocolRegistry._registry.copy()
    yield
    ProtocolRegistry._registry = original


def test_get_protocol_plugin_returns_registered_classes():
    modbus_plugin = ProtocolRegistry.get_protocol_plugin(Protocol.MODBUS_RTU)
    assert modbus_plugin is not None
    assert modbus_plugin.meter_class.__name__ == "ModbusRTUEnergyMeter"
    assert modbus_plugin.options_class.__name__ == "ModbusRTUOptions"
    assert modbus_plugin.node_factory is _modbus_rtu_node_factory

    opcua_plugin = ProtocolRegistry.get_protocol_plugin(Protocol.OPC_UA)
    assert opcua_plugin is not None
    assert opcua_plugin.meter_class.__name__ == "OPCUAEnergyMeter"
    assert opcua_plugin.options_class.__name__ == "OPCUAOptions"
    assert opcua_plugin.node_factory is _opcua_node_factory


def test_register_new_protocol_and_retrieve():
    class DummyOptions:
        pass

    class DummyMeter:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            pass

        def stop(self):
            pass

    def dummy_node_factory(record):
        return None

    assert ProtocolRegistry.get_protocol_plugin(Protocol.MQTT) is None

    ProtocolRegistry.register_protocol(Protocol.MQTT, DummyMeter, DummyOptions, dummy_node_factory)
    plugin = ProtocolRegistry.get_protocol_plugin(Protocol.MQTT)
    assert plugin is not None
    assert plugin.meter_class is DummyMeter
    assert plugin.options_class is DummyOptions
    assert plugin.node_factory is dummy_node_factory


def test_get_protocol_plugin_for_unregistered_protocol_returns_none():
    assert ProtocolRegistry.get_protocol_plugin(Protocol.MQTT) is None
