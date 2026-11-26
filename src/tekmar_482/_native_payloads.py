"""Native tN4 payloads, independent of the 482's host-side tRPC methods."""

from collections.abc import Callable
from enum import IntEnum

Fields = dict[str, str | int | float]
SETBACK = ("wake", "unoccupied_4", "occupied_4", "sleep", "occupied_2", "unoccupied_2", "away", "unknown")
H_UNAVAILABLE = (0x4000, 0x8000, 0xc000, 0xffff)


class ShortPayload(ValueError):
    pass


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise ShortPayload(f"need_{end}_bytes_have_{len(self.data)}")
        field = self.data[self.pos:end]
        self.pos = end
        return field

    def byte(self) -> int:
        return self.take(1)[0]

    def word(self) -> int:
        return int.from_bytes(self.take(2), "little")

    @property
    def remaining(self) -> bytes:
        return self.data[self.pos:]


def temp(name: str, raw: int, *, deg_e: bool = False) -> Fields:
    fields: Fields = {name + "_raw": raw}
    # Preserve the special value rather than turning a missing sensor into a reading.
    if raw in ((0xff,) if deg_e else H_UNAVAILABLE):
        fields[name] = "unavailable"
    elif raw != 0:
        fields[name + "_f"] = round(raw * 0.9 + 32 if deg_e else (raw - 850) / 10, 1)
    return fields


def subnet(value: int) -> Fields:
    return {"water_subnet": value, "water_bus": value & 7, "water_control": value >> 3}


SNOW_WORDS = {
    0x19: "water_detect_state", 0x1a: "heat_sequence_state",
    0x1c: "warm_weather_shutdown_state", 0x1d: "cold_weather_cutoff_state",
    0x1e: "zone_type", 0x1f: "electric_off_time_15s_ticks",
    0x20: "zone_id", 0x21: "sensor_flags", 0x25: "melt_group_id", 0x26: "storm_group_id",
    0x27: "boiler_output_raw", 0x28: "mix_output_raw", 0x29: "pwm_output_raw",
    0x2a: "system_pump_contacts", 0x2b: "alert_relay_contacts",
    0x2f: "keep_alive", 0x30: "melting_time_minutes",
}
SNOW_TEMPERATURES = {
    0x22: "slab_temperature", 0x23: "slab_setpoint", 0x24: "edge_temperature",
    0x2c: "return_temperature", 0x2d: "supply_temperature", 0x2e: "supply_target",
}
SNOW_OPERATING_STATES = {1: "off", 2: "idle", 3: "storm", 4: "melting"}
SNOW_APPLICATION_MODES = ("pwm", "boiler", "mixing", "electric", "sensor_only")


def snowmelt(r: Reader) -> Fields:
    address, count = r.take(2)
    fields: Fields = {"source_type": address & 7, "source_id": address >> 3, "pairs": count}
    for index in range(count):
        key, value = r.byte(), r.word()
        prefix = f"pair{index}_"
        fields[prefix + "key"] = f"0x{key:02x}"
        if key in SNOW_TEMPERATURES:
            fields.update(temp(prefix + SNOW_TEMPERATURES[key], value))
        elif key == 0x1b:
            fields[prefix + "operating_state"] = value >> 8
            fields[prefix + "operating_substate"] = value & 0xff
            if (state := SNOW_OPERATING_STATES.get(value >> 8)) is not None:
                fields[prefix + "operating_state_name"] = state
        elif 1 <= key <= 24:
            fields[prefix + "command_status"] = value
        else:
            fields[prefix + SNOW_WORDS.get(key, "value_raw")] = value
    return fields


def io_change(r: Reader) -> Fields:
    sub, node, channel, kind = r.take(4)
    return {"subnet_id": sub, "node_id": node, "channel_id": channel,
            "type_id": kind, "io_value": r.word()}


def status_flags(value: int) -> Fields:
    setback = (value >> 1) & 7
    return {"enabled": value & 1, "setback_state": setback, "setback_name": SETBACK[setback],
            "optimum_start": (value >> 4) & 3, "priority": value >> 6}


def setpoint(r: Reader) -> Fields:
    flags = status_flags(r.byte())
    b = r.byte()
    fields = flags | {"actuator_status": b & 3, "demand_status": (b >> 2) & 1,
                      "heat_cool_mode": (b >> 3) & 1, "operating_mode": (b >> 4) & 3,
                      "stat_priority": b >> 6}
    for name in ("target_temp", "temp1", "temp2"):
        fields.update(temp(name, r.word()))
    aux, excuse1, excuse2 = r.take(3)
    return fields | {"aux_relay_status": aux & 3, "requested_enable_status": (aux >> 2) & 3,
                     "actual_enable_status": (aux >> 4) & 3, "reserved": aux >> 6,
                     "status_excuse1": excuse1, "status_excuse2": excuse2}


def dhw(r: Reader) -> Fields:
    fields = status_flags(r.byte())
    b, target, tank = r.take(3)
    return (fields | {"actuator_status": b & 3, "demand_status": (b >> 2) & 1,
                      "heat_cool_mode": (b >> 3) & 3, "operating_mode": b >> 5}
            | temp("target_temp", target, deg_e=True) | temp("tank_temp", tank, deg_e=True))


PLANT_TYPES = (
    "heat_enable", "cool_enable", "modulating_heat", "modulating_cool",
    "staged_boiler", "staged_chiller", "modulating_boiler", "modulating_chiller",
    "air_to_air_heat_pump", "water_to_air_heat_pump", "water_to_water_heat_pump",
    "air_to_water_heat_pump", "modulating_air_to_air_heat_pump",
    "modulating_water_to_air_heat_pump", "modulating_water_to_water_heat_pump",
    "modulating_air_to_water_heat_pump", "staged_electric", "on_off_injection_pump",
    "variable_speed_pump",
)


def modulation(r: Reader) -> Fields:
    return {"current_modulation_level": r.byte(), "minimum_modulation_level": r.byte(),
            "maximum_modulation_level": r.byte()}


def heat_pump(r: Reader, kind: int) -> Fields:
    state = r.byte()
    fields: Fields = {"state": state & 15, "ob_config": state >> 4 & 1, "cooling": state >> 5 & 1}
    if kind < 12:
        fields.update(number_stages_on=r.byte(), number_stages_available=r.byte())
    else:
        fields.update(modulation(r))
    if kind in (10, 11, 14, 15):
        fields["pump_status"] = r.byte() & 3
    if kind in (9, 10, 11, 14, 15):
        if kind in (14, 15) and len(r.remaining) > 4:
            # The declarative layout packs two words; the indexed reader skips
            # an extra byte. An extension is indistinguishable from that byte.
            fields["temperature_tail_hex"] = r.take(len(r.remaining)).hex()
        else:
            fields.update(input_temp_raw=r.word(), outlet_temp_raw=r.word())
    return fields


def plant(r: Reader) -> Fields:
    device, kind = r.take(2)
    fields: Fields = {"device_total": device & 15, "device_id": device >> 4, "device_type": kind}
    if kind >= len(PLANT_TYPES):
        return fields
    fields["plant"] = PLANT_TYPES[kind]
    if kind in (0, 1):
        fields["heat_enabled" if kind == 0 else "cool_enabled"] = r.byte()
    elif kind in (2, 3):
        fields.update(signal_type=r.byte(), output_level=r.word())
    elif kind in (4, 5, 6):
        b = r.byte()
        fields.update(state=b & 15, reserved=b >> 4 & 3, pump_status=b >> 6)
        if kind == 6:
            fields.update(modulation(r))
        else:
            stages = r.byte()
            fields.update(stages_available_mask=stages & 15, stages_operating_mask=stages >> 4)
        fields.update(input_temp_raw=r.byte(), outlet_temp_raw=r.byte())
        if kind != 5:
            output = r.byte()
            fields.update(output=output & 31, return_protection=output >> 5)
    elif 8 <= kind <= 15:
        fields.update(heat_pump(r, kind))
    elif kind >= 16:
        fields["state"] = r.byte()
        if kind == 16:
            fields.update(number_stages_on=r.byte(), number_stages_available=r.byte())
        fields["sensor_temp_raw"] = r.word()
    # No fields are defined for subtype 7. decode_payload retains its tail.
    return fields


def outdoor(r: Reader) -> Fields:
    current = r.word()
    fields = temp("outdoor_temp", current)
    if r.remaining:
        # J1196J 0x3dd8 emits the word twice; 0x3e38 reads the first word only.
        second = r.word()
        fields.update(second_word_raw=second, matches_first=int(second == current))
    return fields


def clock_report(r: Reader) -> Fields:
    minute, time, date, day, year = r.take(5)
    month = date & 15
    fields: Fields = {"minute": minute, "weekday": time & 7, "hour": time >> 3,
                      "date_supported": int(date != 0xff), "month_raw": month,
                      "dst": (date >> 4) & 1, "reserved": date >> 5,
                      "day_raw": day, "year_from_2000": year}
    # J1196J 0xac90 transmits zero-based month/day. These are the remote's
    # fields, not a timezone-qualified timestamp or a claim its clock is set.
    if date != 0xff and month < 12 and day < 31:
        fields.update(year=2000 + year, month=month + 1, day=day + 1)
    if r.remaining:
        fields["dst_mode"] = r.byte()
    return fields


def block_request(r: Reader) -> Fields:
    block, destination = r.word(), r.byte()
    mask = int.from_bytes(r.take(4), "little")
    fields: Fields = {"block_id": block, "target_subnet": destination,
                      "target_nodes_mask": f"0x{mask:08x}"}
    if len(r.remaining) >= 4:  # Optional arguments in the 483 layout, absent in the 482 request.
        argument = int.from_bytes(r.take(4), "little")
        fields["arguments"] = argument
        if (selector := BLOCK_SELECTORS.get(block)) is not None:
            fields[selector[0]] = argument
    return fields


SETPOINT_FIELDS = ("wake", "unoccupied", "occupied", "sleep", "away")
DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
EVENTS = ("wake", "unoccupied", "occupied", "sleep")
SCHEDULE_SETUP = (
    "heating_schedule_id", "cooling_schedule_id", "schedule_cycling_rate",
    "num_daily_events", "optimum_start_operation", "rtc_setback_enable",
)
SINGLE_WEEK = range(0x10, 0x14)
MULTI_WEEK = range(0x38, 0x3c)
CONTROL_APPS = ("setpoint", "hot_tub", "dhw", "floor", "pool")
CONTROL_SETUP = ("application_mode", "mode_configuration", "run_mode_configuration",
                 "relay_two_sampling", "relay_two_start_time", "relay_two_duration",
                 "setpoint_enable_address", "outdoor_temp_enable")

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
FIXED_BLOCKS: dict[int, FixedLayout] = {
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
    **dict.fromkeys((0x3c, 0x3d, 0x43, 0x45), ("boiler_number", 255)),
    0x44: ("pump_number", 255), 0x46: ("heating_source_id", 255),
    0x47: ("cooling_source_id", 255), 0x48: ("port_id", 255), 0x49: ("port_id", 255),
    0x4d: ("mix_pump_id", 255), 0x5a: ("tank_id", 255),
    **dict.fromkeys(range(0x8b, 0x93), ("heat_pump_source_id", 255)),
    0x98: ("heat_pump_number", 255),
    **{b: (FIXED_BLOCKS[b][0][0], 255)
       for b in (0x7a, 0x7b, *range(0xc5, 0xd0), 0xd1, 0xd2)},
}


def fixed_fields(block: int, layout: FixedLayout, values: dict[int, int]) -> Fields:
    remaining = dict(values)
    fields: Fields = {}
    start = 0
    for name, width in layout:
        slots = range(start, start + width)
        start += width
        if not all(i in values for i in slots):
            continue
        value = int.from_bytes(bytes(remaining.pop(i) for i in slots), "little")
        if block in SNOW_SLAB_TARGETS:
            fields.update(temp(name, value))
            if value == 0 and block != SnowmeltBlock.MELT_SETPOINT:
                fields[name] = "off"
        else:
            fields[name] = value
            if (block == SnowmeltBlock.APPLICATION_MODE and name == "application_mode"
                    and value < len(SNOW_APPLICATION_MODES)):
                fields["application_mode_name"] = SNOW_APPLICATION_MODES[value]
    # Preserve partial words and future supported slots without assigning units.
    fields.update({f"byte{i + 1}_raw": value for i, value in remaining.items()})
    return fields


def control_words(block: int) -> tuple[str, ...]:
    if 0x28 <= block <= 0x31:
        return ("wake", "sleep") if block & 1 else ("wake_occupied", "unoccupied_sleep", "away", "maximum")
    return ()


def event_time(day: str, value: int) -> Fields:
    fields: Fields = {day + "_raw": value}
    if value < 144:
        fields[day] = f"{value // 6:02d}:{value % 6 * 10:02d}"
    elif value == 144:
        fields[day] = "disabled"
    return fields


def block_fields(block: int, index: int, value: int) -> Fields:
    if block == 0x26:
        return {CONTROL_SETUP[index]: value}
    if block == 13 and index < 5:
        return {("onboard_sensor", "sensor1", "sensor2", "sensor3", "sensor4")[index]: value}
    if block == 0xa3 and index < 2:
        return {("humidity_mode_setup", "dehumidity_mode_setup")[index]: value}
    if block in (0xa4, 0xa5) and index < len(SETPOINT_FIELDS):
        return {SETPOINT_FIELDS[index] + "_target_rh_percent": value}
    if block in (5, 6, 7) and index < len(SETPOINT_FIELDS):
        return temp(SETPOINT_FIELDS[index], value, deg_e=True)
    if block == 7 and index == 5:
        return temp("maximum", value, deg_e=True)
    if block == 9 and index < len(SETPOINT_FIELDS):
        return {SETPOINT_FIELDS[index] + "_fan_percent": value}
    if block == 3 and index < 3:
        return {("mode_setting", "heating_cph", "air_group_id")[index]: value}
    if block == 15 and index < len(SCHEDULE_SETUP):
        return {SCHEDULE_SETUP[index]: value}
    if block in SINGLE_WEEK and index < len(DAYS):
        return event_time(DAYS[index], value)
    if block in MULTI_WEEK:
        if index == 0:
            return {"schedule_id": value & 7, "master_flag": value >> 3 & 1,
                    "schedule_cycling_rate": value >> 4 & 3, "num_daily_events": value >> 6}
        return event_time(DAYS[index - 1], value)
    return {f"byte{index + 1}_raw": value}


def block_metadata(block: int) -> Fields:
    if 0x28 <= block <= 0x31:
        return {"control_application": CONTROL_APPS[(block - 0x28) // 2]}
    if block in SINGLE_WEEK or block in MULTI_WEEK:
        return {"event": EVENTS[block & 3]}
    return {}


def block_report(r: Reader) -> Fields:
    block, support = r.word(), r.byte()
    fields: Fields = {"block_id": block, "support_mask": f"0x{support:02x}"}
    count, span = support.bit_count(), support.bit_length()
    length = len(r.remaining)
    if count < length < span:
        # Neither an exact packed layout nor a complete positional layout.
        # The remaining bytes are retained by decode_payload.
        return fields | {"block_layout": "ambiguous"}
    positional = count < span <= length
    if positional:
        fields["block_layout"] = "positional"
    layout = FIXED_BLOCKS.get(block)
    values: dict[int, int] = {}
    for index in range(span):
        if support & (1 << index):
            values[index] = value = r.byte()
            if layout is None:
                fields.update(block_fields(block, index, value))
        elif positional:
            fields[f"byte{index + 1}_unsupported_raw"] = r.byte()
    if layout is not None:
        return fields | fixed_fields(block, layout, values)
    if block == 0:
        for name, start, size in (("product_number", 0, 3), ("j_number", 3, 2),
                                  ("z_number", 5, 1), ("max_tn4_version", 6, 1)):
            slots = range(start, start + size)
            if all(i in values for i in slots):
                fields[name] = int.from_bytes(bytes(values[i] for i in slots), "little")
    for i, name in enumerate(control_words(block)):
        lo, hi = 2 * i, 2 * i + 1
        if lo in values and hi in values:
            fields.update(temp(name, values[lo] | values[hi] << 8))
    return fields | block_metadata(block)


def block_update(r: Reader) -> Fields:
    dest, node = r.take(2)
    block = r.word()
    fields: Fields = {"target_subnet": dest, "target_node": node & 31,
                      "target_node_flags": node >> 5, "block_id": block}
    # Updates have no support mask. Decode only layouts with known ordering.
    if (layout := FIXED_BLOCKS.get(block)) is not None:
        data = r.take(sum(width for _, width in layout))
        return fields | fixed_fields(block, layout, dict(enumerate(data)))
    words = control_words(block)
    if words:
        if block == 0x2e and len(r.remaining) == 6:
            words = words[:3]  # Some floor controls omit the maximum.
        for name in words:
            fields.update(temp(name, r.word()))
        return fields | block_metadata(block)
    sizes = {3: 3, 5: 5, 6: 5, 7: 6, 9: 5, 13: 5, 15: 6, 0x26: 8, 0xa3: 2, 0xa4: 5, 0xa5: 5}
    size = 7 if block in SINGLE_WEEK else 8 if block in MULTI_WEEK else sizes.get(block)
    if size is not None:
        for index in range(size):
            fields.update(block_fields(block, index, r.byte()))
    return fields | block_metadata(block)


def stage(r: Reader) -> Fields:
    fields = subnet(r.byte())
    b, p = r.take(2)
    fields.update(stage=b & 1, override_status=(b >> 1) & 3, first_report=(b >> 3) & 1,
                  setpoint_control=(b >> 4) & 1, dhw_control=(b >> 5) & 1,
                  priority_level=b >> 6, pump_delay=p & 63, pump_not_required=(p >> 6) & 1,
                  output_on=p >> 7)
    if p & 0x80:
        fields.update(temp("target_temp", r.word()))
    return fields


def air_member(r: Reader) -> Fields:
    a, b, target = r.take(3)
    actual, outputs, remote = r.word(), r.byte(), r.word()
    return ({"air_group_id": a & 31, "mode": "cool" if a & 32 else "heat",
             "request_for_master_report": (a >> 6) & 1, "reserved0": a >> 7,
             "heat_cool_enabled": b & 1, "heat_cool_demand": (b >> 1) & 1,
             "schedule_status": (b >> 2) & 7, "schedule_name": SETBACK[(b >> 2) & 7],
             "optimum_start": (b >> 5) & 3,
             "weather_shutdown": b >> 7, "output_1_status": outputs & 3,
             "output_2_status": (outputs >> 2) & 3, "reserved5": outputs >> 4}
            | temp("target", target, deg_e=True) | temp("temperature", actual)
            | temp("remote_temperature", remote))


def master(r: Reader, *, dhw_supply: bool = True) -> Fields:
    supply, ret, mode_target = r.word(), r.word(), r.word()
    loop, pump, b = r.take(3)
    fields = (temp("supply_temp", supply) | temp("return_temp", ret)
              | temp("target_temp", mode_target & 0x1fff))
    mode = mode_target >> 13
    modes = ("neither", "heating", "heating_cooling_disallowed", "cooling", "cooling_heating_disallowed")
    fields.update(heat_cool_mode=mode, mode_name=modes[mode] if mode < len(modes) else "unknown",
                  loop_output_pct=loop, pump_output_pct=pump,
                  setback_status=b & 7, weather_shutdown=(b >> 3) & 1,
                  operating_on_max=(b >> 4) & 1, operating_on_min=(b >> 5) & 1,
                  output_limited=(b >> 6) & 1, reserved=b >> 7)
    if dhw_supply and r.remaining:
        fields.update(temp("dhw_supply_temp", r.word()))
    return fields


def network_error(r: Reader) -> Fields:
    code, value = r.take(2)
    return {"code": code, "instance": value & 63, "priority": value >> 6}


def octets(*names: str) -> Callable[[Reader], Fields]:
    def read(r: Reader) -> Fields:
        return dict(zip(names, r.take(len(names)), strict=True))
    return read


def target_selection(r: Reader) -> Fields:
    dest, flags = r.take(2)
    return {"target_subnet": dest, "output_level": flags & 127, "stage": flags >> 7,
            "target_nodes_mask": f"0x{int.from_bytes(r.take(3), 'little'):06x}"}


def snow_request(r: Reader) -> Fields:
    address, count = r.take(2)
    return {"destination_type": address & 7, "destination_id": address >> 3,
            "count": count, "keys_hex": r.take(count).hex()}


def io_discovery(r: Reader) -> Fields:
    fields = octets("subnet_id", "node_id", "channel_id", "channel_count")(r)
    # Preserve embedded NULs and non-ASCII labels exactly.
    return fields | {"label_hex": r.take(6).hex(), "type_id": r.byte(), "io_value": r.word()}


def schedule(r: Reader) -> Fields:
    a, b, edge = r.take(3)
    return {"schedule_id": a & 3, "master": a >> 2 & 1, "previous_schedule_id": a >> 3 & 3,
            "previous_master": a >> 5 & 1, "events": a >> 6, "current_state": b & 3,
            "next_state": b >> 2 & 3, "duplicate_response": b >> 4 & 1, "time_to_next_edge": edge}


def mix_report(r: Reader) -> Fields:
    mode, pump, actuator, high, target, supply, ret = r.take(7)
    return {"output_mode": mode & 3, "cooling_logic": mode >> 2 & 1, "external_demand": mode >> 4 & 1,
            "adjustment_latched": mode >> 5 & 1, "output_limited": mode >> 6 & 1, "fault": mode >> 7,
            "pump_output": pump, "actuator_percent": actuator & 63, "decreasing": actuator >> 6 & 1,
            "increasing": actuator >> 7, "target_raw": (high & 3) << 8 | target,
            "supply_raw": (high >> 2 & 3) << 8 | supply, "return_raw": (high >> 4 & 3) << 8 | ret,
            "return_sensor_fault": high >> 6 & 1, "supply_sensor_fault": high >> 7}


def mix_update(r: Reader) -> Fields:
    b, target, pump = r.take(3)
    return {"output_mode": b & 3, "cooling_logic": b >> 2 & 1, "clear_error": b >> 3 & 1,
            "target_raw": (b >> 6) << 8 | target, "target_pump_output": pump}


def air_master(r: Reader) -> Fields:
    group, cycle = r.take(2)
    fields: Fields = {"air_group_id": group & 31, "inhibit_heat": group >> 5 & 1,
                      "inhibit_cool": group >> 6 & 1, "cycle_length": cycle & 127, "new_cycle": cycle >> 7}
    for mode in ("heat", "cool"):
        flags, target = r.take(2)
        fields.update({mode + "_enabled": flags & 1, mode + "_demand": flags >> 1 & 1,
                       mode + "_schedule": flags >> 2 & 7, mode + "_optimum_start": flags >> 5 & 3,
                       mode + "_weather_shutdown": flags >> 7})
        fields.update(temp(mode + "_target", target, deg_e=True))
        fields.update(temp(mode + "_temperature", r.word()))
    outputs = r.byte()
    for index, name in enumerate(("heat1", "heat2", "cool1", "cool2")):
        fields[name + "_status"] = outputs >> (2 * index) & 3
    return fields | temp("remote_temperature", r.word())


def rh_report(r: Reader) -> Fields:
    count, flags, control = r.take(3)
    fields: Fields = {"count": count, "flags": flags, "control": control, "dewpoint_raw": r.word(),
                      "internal_sensor_raw": r.byte()}
    # Both layouts place the optional targets at 6+count and 7+count.
    # Only the declarative one consistently consumes the intervening bytes.
    fields["sensor_bytes_hex"] = r.take(count).hex()
    for name in ("humidity_current_target", "dehumidity_current_target"):
        if r.remaining:
            fields[name] = r.byte()
    return fields


def bits(value: int, *fields: tuple[str, int, int]) -> Fields:
    return {name: (value >> shift) & mask for name, shift, mask in fields}


def master_present(r: Reader) -> Fields:
    fields: Fields = {"boiler_subnet": r.byte()}
    for n in range(1, min(7, len(r.remaining)) + 1):
        fields[f"mix{n}_subnet"] = r.byte()
    return fields


DECODERS: dict[int, tuple[str, Callable[[Reader], Fields]]] = {
    0x010: ("ZoneSynchAdjustment", octets("synch_cycle", "purging", "target_mode")),
    0x011: ("ZoneSynchQuery", octets()),
    0x173: ("TankStatus", octets("tank_id", "pump_output_pct", "tank_temp_raw", "target_temp_raw")),
    0x17f: ("SnowmeltZoneStatusRequest", snow_request),
    0x1f3: ("IORequestResponse", io_change),
    0x1f4: ("IORequest", octets("subnet_id", "node_id", "channel_id")),
    0x1f5: ("IODiscoverReport", io_discovery),
    0x1f6: ("IODiscoverRequest", octets("subnet_id", "node_id")),
    0x200: ("SecureGatewayAccess", lambda r: bits(r.byte(), ("request", 0, 1), ("access_open", 1, 1))),
    0x20f: ("SetpointEnableReport", lambda r: {"group_id": r.byte()} | bits(
        r.byte(), ("requested_enable", 0, 1), ("actual_enable", 4, 1))),
    0x21f: ("SetpointRemoteEnable", octets("group_id", "enable")),
    0x25f: ("FanDuctStatus", lambda r: bits(r.byte(), ("fan1", 0, 3), ("fan2", 2, 3),
        ("optimum_start", 4, 3), ("freeze_protect", 6, 1)) |
        {"duct_temp_raw": r.word(), "coil_temp_raw": r.word()}),
    0x26f: ("SlabStatus", lambda r: {"slab_status": r.byte() & 7, "target_raw": r.byte(), "current_raw": r.word()}),
    0x287: ("CalibrationMode", octets("mode")),
    0x2ef: ("NetworkLock", lambda r: bits(r.byte(), ("access_lock", 0, 1), ("address_lock", 1, 1))),
    0x2f7: ("ScheduleStatus", schedule),
    0x2ff: ("SceneReport", lambda r: {"scene": r.byte(), "duration": r.word()}),
    0x307: ("WWSDRequest", octets()),
    0x30f: ("WWSDStatus", lambda r: bits(r.byte(), *((name, i, 1) for i, name in enumerate(SETPOINT_FIELDS)))),
    0x317: ("RealTimeClockRequest", octets()),
    0x322: ("RHQuery", lambda r: {"target_subnet": r.byte(), "node_selector": int.from_bytes(r.take(4), "little")}),
    0x323: ("RHReport", rh_report),
    0x327: ("OutdoorTemperatureRequest", octets()),
    0x347: ("MixingModulePresent", lambda r: bits(r.byte(), ("actuator_type", 0, 15),
        ("adjustable_speed", 6, 1), ("system_pump_controllable", 7, 1)) | {"motor_speed": r.byte()}),
    0x34f: ("MixingModuleReport", mix_report),
    0x357: ("MixingModuleUpdate", mix_update),
    0x35f: ("StageHeatDemandRequest", target_selection),
    0x363: ("GeneralDemand", lambda r: subnet(r.byte()) | bits(r.byte(), ("cooling", 6, 1), ("demand", 7, 1))),
    0x36a: ("AirGroupMasterConfig", lambda r: bits(r.byte(), ("group", 0, 31), ("no_response_required", 6, 1),
        ("master", 7, 1)) | bits(r.byte(), ("previous_group", 0, 31), ("previous_master", 7, 1))),
    0x377: ("AirGroupMasterStatus", air_master),
    0x37f: ("WaterCycleStart", lambda r: bits(r.byte(), ("cycle_length", 0, 127), ("hold_override", 7, 1))),
    0x387: ("NetworkTestTime", lambda r: {"additional_time": r.word()}),
    0x38f: ("NetworkTestMode", lambda r: {"test_mode": r.byte(), "expected_duration": r.word()}),
    0x397: ("EndPurge", target_selection),
    0x39f: ("StartPurge", target_selection),
    0x3a7: ("StartOverrideOn", target_selection),
    0x3af: ("EndOverrideOff", target_selection),
    0x3b7: ("EndOverrideOn", target_selection),
    0x3bf: ("StartOverrideOff", target_selection),
    0x3ce: ("PlantLoopStatus", lambda r: master(r, dhw_supply=False)),
    0x3e7: ("TakingAddress", lambda r: {"previous_subnet": r.byte()} | bits(
        r.byte(), ("previous_node", 0, 31), ("locked", 5, 1))),
    0x3ef: ("AddressTaken", octets()),
    0x3ff: ("MasterPresent", master_present),
    0x17e: ("SnowmeltZoneStatus", snowmelt),
    0x1f2: ("IOChangeReport", io_change),
    0x22f: ("SetpointStatus", setpoint),
    0x23f: ("DHWStatus", dhw),
    0x24f: ("PlantOutputStatus", plant),
    0x2d7: ("BlockRequest", block_request),
    0x2df: ("BlockUpdate", block_update),
    0x2e7: ("BlockReport", block_report),
    0x31f: ("RealTimeClockReport", clock_report),
    0x32f: ("OutdoorTemperatureReport", outdoor),
    0x337: ("MaximumBoilerPercent", lambda r: {"max_value": r.byte()}),
    0x33f: ("CycleLengthRequest", lambda r: {"air_group_id": r.byte(), "requested_length": r.byte()}),
    0x367: ("StageHeatDemand", stage),
    0x36f: ("AirGroupMemberStatus", air_member),
    0x3c7: ("RelayStatusHeartbeat", lambda r: subnet(r.byte())),
    0x3cf: ("MasterStatus", master),
    0x3d7: ("NetworkError", network_error),
    0x3df: ("NetworkInitRequest", lambda _: {}),
    0x3f7: ("MasterQuery", lambda _: {}),
}


def decode_payload(message: int, data: bytes) -> Fields:
    if message not in DECODERS:
        return {}
    reader = Reader(data)
    try:
        fields = DECODERS[message][1](reader)
    except ShortPayload as exc:
        return {"truncated": str(exc)}
    if reader.remaining:
        fields["extension_hex"] = reader.remaining.hex()
    return fields


def details(message: int, payload: bytes) -> str:
    if message not in DECODERS:
        return ""
    fields = decode_payload(message, payload)
    return " ".join([DECODERS[message][0], *(f"{key}={v}" for key, v in fields.items())])
