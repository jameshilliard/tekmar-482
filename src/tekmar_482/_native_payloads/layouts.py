"""Byte positions, widths and selectors of the 483's addressed tN4 blocks."""

from enum import IntEnum

SETPOINT_FIELDS = ("wake", "unoccupied", "occupied", "sleep", "away")
MULTI_WEEK = range(0x38, 0x3c)
H_UNAVAILABLE = (0x4000, 0x8000, 0xc000, 0xffff)


class SnowmeltBlock(IntEnum):
    """Addressed native blocks, not zone-status keys or tRPC methods."""

    MELT_SETPOINT = 0x71
    IDLE_SETPOINT = 0x72
    WATER_SENSITIVITY = 0x73
    MELT_RUN_TIME = 0x74
    ADDITIONAL_MELT_TIME = 0x75
    COLD_WEATHER_CUTOFF = 0x76
    WARM_WEATHER_SHUTDOWN = 0x77
    STORM_SETPOINT = 0x78
    STORM_TIME = 0x79
    SNOW_ICE_SENSOR_TYPE = 0x7a
    SLAB_SENSOR_SELECTION = 0x7b
    SLAB_PROTECTION = 0x7c
    MAXIMUM_MELT_TIME = 0x7d
    ZONE_ID = 0xc0
    SENSOR_ID = 0xc1
    MELT_GROUP = 0xc2
    STORM_GROUP = 0xc3
    TRACKED_ZONE = 0xc4
    MELT_START_TYPE = 0xc5
    SLAB_HIGH = 0xc6
    SLAB_LOW = 0xc7
    SUPPLY_HIGH = 0xc8
    SUPPLY_LOW = 0xc9
    RETURN_HIGH = 0xca
    RETURN_LOW = 0xcb
    HEAT_RELAY_HOURS = 0xcc
    HEAT_RELAY_CYCLES = 0xcd
    WET_LEVEL = 0xce
    DRY_LEVEL = 0xcf
    APPLICATION_MODE = 0xd0
    BOILER_MINIMUM = 0xd1
    BOILER_MAXIMUM = 0xd2

    @property
    def selector_field(self) -> str | None:
        selector = BLOCK_SELECTORS.get(self)
        return selector[0] if selector else None


SNOW_SLAB_TARGETS = (SnowmeltBlock.MELT_SETPOINT, SnowmeltBlock.IDLE_SETPOINT, SnowmeltBlock.STORM_SETPOINT)

# (name, width) in logical byte order. Words are little-endian, including
# those starting at an odd offset after a sensor, relay or control ID.
FixedLayout = tuple[tuple[str, int], ...]


def _octets(*names: str) -> FixedLayout:
    return tuple((name, 1) for name in names)


def _setbacks(quantity: str) -> FixedLayout:
    return _octets(*(f"{state}_{quantity}_raw" for state in SETPOINT_FIELDS))


def _loops(buses: range) -> FixedLayout:
    return tuple(field for n in buses for field in _octets(f"bus{n}_subnet", f"bus{n}_heat_source_id"))


_SENSOR = (("sensor_configuration", 1), ("current_value_raw", 2),
           ("minimum_value_raw", 2), ("maximum_value_raw", 2))
_PID = (("proportional_coefficient", 2), ("integral_coefficient", 2),
        ("derivative_coefficient", 2), ("inhibit_time", 1))
_EXCHANGE = _octets("mode") + _setbacks("exchange_target")

FIXED_BLOCKS: dict[int, FixedLayout] = {
    0x01: _octets("stage1_terminal_raw", "stage1_pump_flags", "stage2_terminal_raw",
                  "stage2_pump_flags", "stage2_heat_source_raw"),
    0x02: _octets("pump_flags", "design_outdoor_temp_raw", "design_indoor_temp_raw",
                  "design_supply_temp_raw", "minimum_supply_temp_raw", "maximum_supply_temp_raw"),
    0x04: _octets("supported_cooling_stages", "minimum_cooling_run_time", "minimum_cooling_off_time",
                  "cooling_cph", "heat_cool_interlock", "heat_cool_priority"),
    0x08: _octets("fan_stage_flags", "fan_ventilation_flags", "minimum_heating_coil_temp_raw",
                  "maximum_cooling_coil_temp_raw", "fan_delay", "fan_purge", "heating_purge", "cooling_purge"),
    0x0a: (("filter_replacement_interval", 2), ("time_since_filter_replacement", 2)),
    0x0b: _octets("shutdown_source") + _setbacks("warm_weather_shutdown"),
    0x0c: _setbacks("cold_weather_shutdown"),
    0x0e: _octets("away_period", "hold_period", "permanent_scene_1", "permanent_scene_4",
                  "permanent_scene_5", "temporary_scene_7", "temporary_scene_8", "scenes_enabled"),
    0x14: _octets("access_level", "units", "backlight", "time_format", "dst_mode", "exercise_enable"),
    0x15: (("average_room_temp", 2), ("minimum_room_temp", 2), ("maximum_room_temp", 2)),
    **dict.fromkeys(range(0x16, 0x1b), _SENSOR),
    0x1b: _octets("heat_stage1_output", "heat_stage2_output", "cool_stage1_output",
                  "cool_stage2_output", "fan1_output", "fan2_output"),
    **{i: ((f"{relay}_accumulated_time", 2),) for i, relay in enumerate(
        ("heat_stage1", "heat_stage2", "cool_stage1", "cool_stage2", "fan1", "fan2"), 0x1c)},
    0x22: _octets("mix_output_mode", "mix_actuation_rate"),
    0x23: _octets("boiler_cycle_length", "boiler_differential_raw", "fire_delay", "modulation_speed",
                  "minimum_modulation_percent", "maximum_modulation_percent", "stage_delay", "turndown_ratio"),
    0x24: _EXCHANGE + _octets("dhw_sensor_usage", "dhw_boiler_count"),
    0x25: _EXCHANGE,
    0x27: _octets("exchange_supply_temp_raw", "maximum_supply_temp_raw", "cycle_time", "differential_raw",
                  "minimum_relay_on_time", "minimum_relay_off_time", "heat_source_type", "priority"),
    0x32: (("manual_override_time", 2),),
    0x33: _loops(range(1, 5)),
    0x34: _loops(range(5, 8)),
    0x35: _octets("available_boilers", "enabled_boilers"),
    0x36: _octets("occupied_tank_target_raw", "unoccupied_tank_target_raw", "away_tank_target_raw",
                  "tank_differential_raw"),
    0x37: _octets("override_state"),
    0x3c: _octets("boiler_number", "operation_enable", "btc_type", "boiler_relay_type",
                  "pump_post_purge_time", "pump_pre_purge_time"),
    0x3d: _octets("boiler_number", "thermal_mass", "actuator_motor_speed", "fire_delay",
                  "starting_modulation", "delay", "minimum_modulation", "maximum_modulation"),
    0x3e: _octets("external_signal_operation", "ems_input_signal", "target_offset_raw"),
    0x3f: _octets("boost_time", "staging_mode", "setpoint_priority", "zone_load_shedding",
                  "relay_demand_type", "cold_alarm_delay", "alarm_relay_delay"),
    0x40: _octets("boiler_rotation", "btc1_configuration", "fixed_last_boiler",
                  "fixed_lead_boiler", "boiler_sequence"),
    0x41: _octets("available_primary_pumps", "enabled_primary_pumps", "available_dhw_pumps",
                  "enabled_dhw_pumps", "primary_pump_rotation_time", "primary_pump_purge_time"),
    0x42: _octets("proof_configuration", "pump_proof_delay", "combustion_proof_delay", "damper_stroke_delay"),
    0x43: (("boiler_number", 1), ("boiler_hours", 2)),
    0x44: (("pump_number", 1), ("pump_hours", 2)),
    0x45: (("boiler_number", 1), ("minimum_output", 2), ("maximum_output", 2)),
    0x46: _octets("heating_source_id") + _PID,
    0x47: _octets("cooling_source_id") + _PID,
    0x48: _octets("port_id", "stuffing_errors", "crc_failures", "missing_crc_delimiter", "missing_ack",
                  "missing_ack_delimiter", "missing_eof", "dominant_bit_errors"),
    0x49: _octets("port_id", "local_port_busy", "remote_device_busy", "arbitration_losses", "error_event_count"),
    0x4a: _octets("plant_mode_setting", "stage3_fire_delay", "staging_selection"),
    0x4b: (("energy_usage_raw", 2),),
    0x4c: (("energy_savings_time", 2),),
    0x4d: (("mix_pump_id", 1), ("mix_pump_hours", 2)),
    0x4e: (("outdoor_low", 2),),
    0x4f: (("outdoor_high", 2),),
    0x50: _octets("available_mix_pumps", "enabled_mix_pumps"),
    0x53: _setbacks("maximum_room_heat"),
    0x54: _setbacks("minimum_room_cool"),
    0x55: _setbacks("maximum_room_cool"),
    0x58: _octets("floor_cooling_enable"),
    0x5a: (("tank_id", 1), ("tank_pump_hours", 2)),
    0x5b: _octets("available_tank_pumps", "enabled_tank_pumps"),
    0x62: (("received_message_count", 4),),
    SnowmeltBlock.MELT_SETPOINT: (("melt_setpoint", 2),),
    SnowmeltBlock.IDLE_SETPOINT: (("idle_setpoint", 2),),
    SnowmeltBlock.WATER_SENSITIVITY: (("water_sensitivity", 1),),
    SnowmeltBlock.MELT_RUN_TIME: (("melt_run_time_minutes", 2),),
    SnowmeltBlock.ADDITIONAL_MELT_TIME: (("additional_melt_time_minutes", 2),),
    SnowmeltBlock.COLD_WEATHER_CUTOFF: (("cold_weather_cutoff_raw", 2),),
    SnowmeltBlock.WARM_WEATHER_SHUTDOWN: (("warm_weather_shutdown_raw", 2),),
    SnowmeltBlock.STORM_SETPOINT: (("storm_setpoint", 2),),
    SnowmeltBlock.STORM_TIME: (("storm_time_minutes", 2),),
    SnowmeltBlock.SNOW_ICE_SENSOR_TYPE: (("sensor_id", 1), ("snow_ice_sensor_configuration", 1)),
    SnowmeltBlock.SLAB_SENSOR_SELECTION: (("sensor_id", 1), ("slab_sensor_configuration", 1)),
    SnowmeltBlock.SLAB_PROTECTION: (("slab_protection", 1),),
    SnowmeltBlock.MAXIMUM_MELT_TIME: (("maximum_melt_time_hours", 2),),
    0x83: _octets("water_temperature_flags"),
    0x84: _octets("tn2_bus_link"),
    0x85: _octets("backup_heat_source_type"),
    0x86: _octets("heat_pump_loop_operation"),
    0x87: (("cooling_tank_target_raw", 2),),
    0x88: _octets("floor_cooling_dewpoint_mode_raw"),
    0x89: (("minimum_cooling_tank_target_raw", 2),),
    0x8a: _octets("heat_pump2_configuration"),
    0x8b: _octets("heat_pump_source_id", "heat_pump_source_type"),
    0x8c: _octets("heat_pump_source_id", "heat_pump_switchover_mode"),
    **{i: (("heat_pump_source_id", 1), (field, 2)) for i, field in enumerate((
        "return_heat_limit_raw", "return_cool_limit_raw", "outdoor_heat_limit_raw",
        "outdoor_cool_limit_raw", "minimum_compressor_run_time", "minimum_compressor_off_time",
    ), 0x8d)},
    0x93: _octets("heat_pump_interstage_delay"),
    0x94: (("heat_pump_differential_raw", 2),),
    0x95: _octets("backup_heat_delay"),
    0x96: _octets("plant_heat_cool_mode"),
    0x97: _octets("available_heat_pumps", "enabled_heat_pumps"),
    0x98: (("heat_pump_number", 1), ("heat_pump_hours", 2)),
    **{i: ((f"{output}_hours", 2),) for i, output in enumerate((
        "w1_heat", "w1_cool", "w2_heat", "w2_cool", "y1_heat", "y1_cool",
        "y2_heat", "y2_cool", "humidification", "dehumidification",
    ), 0x99)},
    0xa6: _octets("heat_pump_cycles"),
    0xa7: _octets("cool_purge_minutes"),
    0xa8: _octets("onboard_humidity_sensor_enabled"),
    0xa9: _setbacks("balance_point"),
    0xaa: _octets("balance_point_schedule_id"),
    0xab: _octets("w1_warm_weather_shutdown_raw"),
    0xac: _octets("w2_lockout_target_raw"),
    0xad: _octets("y_during_w2_mode"),
    0xae: _octets("baseload_mode"),
    0xaf: _octets("unoccupied_humidity_enabled"),
    0xb0: _octets("humidity_differential_raw"),
    SnowmeltBlock.ZONE_ID: (("zone_id", 1),),
    SnowmeltBlock.SENSOR_ID: (("sensor_id", 1),),
    SnowmeltBlock.MELT_GROUP: (("melt_group_id", 1),),
    SnowmeltBlock.STORM_GROUP: (("storm_group_id", 1),),
    SnowmeltBlock.TRACKED_ZONE: (("tracked_zone_id", 1),),
    SnowmeltBlock.MELT_START_TYPE: (("sensor_id", 1), ("melt_start_type", 1)),
    SnowmeltBlock.SLAB_HIGH: (("sensor_id", 1), ("slab_high_raw", 2)),
    SnowmeltBlock.SLAB_LOW: (("sensor_id", 1), ("slab_low_raw", 2)),
    SnowmeltBlock.SUPPLY_HIGH: (("sensor_id", 1), ("supply_high_raw", 2)),
    SnowmeltBlock.SUPPLY_LOW: (("sensor_id", 1), ("supply_low_raw", 2)),
    SnowmeltBlock.RETURN_HIGH: (("sensor_id", 1), ("return_high_raw", 2)),
    SnowmeltBlock.RETURN_LOW: (("sensor_id", 1), ("return_low_raw", 2)),
    SnowmeltBlock.HEAT_RELAY_HOURS: (("relay_id", 1), ("heat_relay_hours", 2)),
    SnowmeltBlock.HEAT_RELAY_CYCLES: (("relay_id", 1), ("heat_relay_cycles", 2)),
    SnowmeltBlock.WET_LEVEL: (("sensor_id", 1), ("wet_level_raw", 2)),
    SnowmeltBlock.DRY_LEVEL: (("sensor_id", 1), ("dry_level_raw", 2)),
    SnowmeltBlock.APPLICATION_MODE: (("application_mode", 1), ("application_type", 1)),
    SnowmeltBlock.BOILER_MINIMUM: (("control_id", 1), ("boiler_minimum_raw", 2)),
    SnowmeltBlock.BOILER_MAXIMUM: (("control_id", 1), ("boiler_maximum_raw", 2)),
}


# All secondary IDs in the 483 registry occupy logical report byte 1.
# Schedule headers share it with other flags; other selectors use all bits.
BLOCK_SELECTORS: dict[int, tuple[str, int]] = {
    **dict.fromkeys(MULTI_WEEK, ("schedule_id", 7)),
    **{b: (FIXED_BLOCKS[b][0][0], 255)
       for b in (0x3c, 0x3d, *range(0x43, 0x4a), 0x4d, 0x5a, 0x7a, 0x7b,
                 *range(0x8b, 0x93), 0x98, *range(0xc5, 0xd0), 0xd1, 0xd2)},
}
