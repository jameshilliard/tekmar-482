"""Native tN4 payloads, independent of the 482's host-side tRPC methods."""

from collections.abc import Callable

from .climate import AIR_MASTER, AIR_MEMBER, AirStatus, ThermostatMode
from .codec import Fields, Reader, SETBACK, ShortPayload, status_flags, temp
from .layouts import (
    BLOCK_SELECTORS as BLOCK_SELECTORS,
    FIXED_BLOCKS,
    H_UNAVAILABLE as H_UNAVAILABLE,
    MULTI_WEEK,
    SETPOINT_FIELDS,
    SNOW_SLAB_TARGETS as SNOW_SLAB_TARGETS,
    FixedLayout,
    SnowmeltBlock as SnowmeltBlock,
)
from .line import ACCESS_STATUS, CLOCK_REPORT, GROUP_REPORT, GROUP_UPDATE, ZONE_ADJUSTMENT, ClockReport, GroupReport
from .products import identify_product
from .scenes import SCENE_REPORT, SCENE_RESPONSE_FIELDS, SceneReport, SceneResponse
from .schedules import DAY_NAMES, SINGLE_WEEK, DailyEvents, ScheduleBinding, ScheduleCycle, ScheduleEvent
from .snowmelt import SNOW_REPORT, SNOW_REQUEST, SnowmeltApplication, SnowmeltReport
from .water import DHW_STATUS, MASTER_PRESENT, MASTER_STATUS, PLANT_LOOP_STATUS, TANK_STATUS, DhwStatus, HeatSource, LoopStatus, TankStatus


def subnet(value: int) -> Fields:
    return {"water_subnet": value, "water_bus": value & 7, "water_control": value >> 3}


def io_change(r: Reader) -> Fields:
    sub, node, channel, kind = r.take(4)
    return {"subnet_id": sub, "node_id": node, "channel_id": channel,
            "type_id": kind, "io_value": r.word()}


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
    head = r.take(5)
    report = ClockReport(head)
    minute, time, date, day, year = head
    month = date & 15
    fields: Fields = {"minute": minute, "weekday": time & 7, "hour": time >> 3,
                      "date_supported": int(date != 0xff), "month_raw": month,
                      "dst_bit_raw": (date >> 4) & 1, "reserved": date >> 5,
                      "day_raw": day, "year_from_2000": year}
    if report.daylight is not None:
        fields["dst"] = int(report.daylight)
    if (calendar := report.standard_date) is not None:
        fields.update(year=calendar.year, month=calendar.month, day=calendar.day,
                      weekday_matches=int(calendar.isoweekday() == report.weekday))
    if (wall := report.wall_datetime) is not None:
        fields["local_datetime"] = wall.isoformat(timespec="minutes")
    if r.remaining:
        fields["dst_mode"] = r.byte()
    return fields


def group_report(r: Reader) -> Fields:
    group = GroupReport(*r.take(2))
    return {"group_id": group.group, "flags_raw": group.flags, "requested_enable": int(group.requested),
            "actual_enable": int(group.actual)}


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


SCHEDULE_SETUP = (
    "heating_schedule_id", "cooling_schedule_id", "schedule_cycling_rate",
    "num_daily_events", "optimum_start_operation", "rtc_setback_enable",
)
CONTROL_APPS = ("setpoint", "hot_tub", "dhw", "floor", "pool")
CONTROL_SETUP = ("application_mode", "mode_configuration", "run_mode_configuration",
                 "relay_two_sampling", "relay_two_start_time", "relay_two_duration",
                 "setpoint_enable_address", "outdoor_temp_enable")

_PACKED_OCTETS = {
    "fan_stage_flags": (("fan_operation_type", 0, 3), ("fan_heat_stage1", 2, 3),
                        ("fan_heat_stage2", 4, 3), ("fan_cool_stage1", 6, 3)),
    "fan_ventilation_flags": (("fan_cool_stage2", 0, 3), ("ventilation_mode", 2, 3)),
    "water_temperature_flags": (("boiler_flag", 0, 1), ("mix_flag", 1, 1), ("tank_flag", 2, 1)),
}


def fixed_value(block: int, name: str, value: int) -> Fields:
    if block in SNOW_SLAB_TARGETS or block in (0x15, 0x4e, 0x4f):
        fields = temp(name, value)
        if value == 0 and block in (SnowmeltBlock.IDLE_SETPOINT, SnowmeltBlock.STORM_SETPOINT):
            fields[name] = "off"
        return fields
    fields = {name: value}
    if name in _PACKED_OCTETS:
        fields.update(bits(value, *_PACKED_OCTETS[name]))
    elif name in ("pump_flags", "stage1_pump_flags", "stage2_pump_flags"):
        pump = name.removesuffix("_flags")
        fields.update(bits(value, (pump + "_delay", 0, 127), (pump + "_required", 7, 1)))
    elif block == 0x0e and name in SCENE_RESPONSE_FIELDS and value in range(len(SceneResponse)):
        fields[name + "_name"] = SceneResponse(value).name.lower()
    elif block in (0x33, 0x34) and name.endswith("_subnet"):
        prefix = name.removesuffix("_subnet")
        fields.update(bits(value, (prefix + "_water_bus", 0, 7), (prefix + "_water_control", 3, 31)))
    elif block in (0x33, 0x34) and name.endswith("_heat_source_id"):
        prefix = name.removesuffix("_id")
        source = HeatSource(value)
        return {**fields, prefix + "_kind": source.kind.value, prefix + "_loop": source.loop,
                prefix + "_mode": source.mode}
    elif block == 0x37 and value < 9:
        fields["override_state_name"] = (*SETBACK[:7], "auto_4", "auto_2")[value]
    elif (block == SnowmeltBlock.APPLICATION_MODE and name == "application_mode"
          and value < len(SnowmeltApplication)):
        fields["application_mode_name"] = SnowmeltApplication(value).name.lower()
    return fields


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
        fields.update(fixed_value(block, name, value))
    # Preserve partial words and future supported slots without assigning units.
    fields.update({f"byte{i + 1}_raw": value for i, value in remaining.items()})
    return fields


def scene_report(r: Reader) -> Fields:
    report = SceneReport.from_payload(r.take(3))
    fields: Fields = {"scene": report.wire_id, "duration": report.duration_minutes,
                      "duration_minutes": report.duration_minutes}
    if report.scene is not None:
        fields.update(scene_number=report.scene.value, scene_name=report.scene.name.lower())
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
        if index == 0 and (mode := ThermostatMode.decode(value)) is not None:
            return {"mode_setting": value, "mode_setting_name": mode.name.lower()}
        return {("mode_setting", "heating_cph", "air_group_id")[index]: value}
    if block == 15 and index < len(SCHEDULE_SETUP):
        name = SCHEDULE_SETUP[index]
        fields: Fields = {name: value}
        if index in (0, 1) and value in range(len(ScheduleBinding)):
            fields[name + "_name"] = ScheduleBinding(value).name.lower()
        if index == 2 and value in range(len(ScheduleCycle)):
            fields[name + "_name"] = ScheduleCycle(value).name.lower()
        if index == 3 and value in range(len(DailyEvents)):
            fields["daily_event_count"] = len(DailyEvents(value).events)
        return fields
    if block in SINGLE_WEEK and index < len(DAY_NAMES):
        return event_time(DAY_NAMES[index], value)
    if block in MULTI_WEEK:
        if index == 0:
            return {"schedule_id": value & 7, "master_flag": value >> 3 & 1,
                    "schedule_cycling_rate": value >> 4 & 3, "num_daily_events": value >> 6}
        return event_time(DAY_NAMES[index - 1], value)
    return {f"byte{index + 1}_raw": value}


def block_metadata(block: int) -> Fields:
    if 0x28 <= block <= 0x31:
        return {"control_application": CONTROL_APPS[(block - 0x28) // 2]}
    if block in SINGLE_WEEK or block in MULTI_WEEK:
        return {"event": ScheduleEvent(block & 3).name.lower()}
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
        number, j_number = fields.get("product_number"), fields.get("j_number")
        if isinstance(number, int):
            product = identify_product(number, j_number if isinstance(j_number, int) else None)
            if product is not None:
                fields.update(model=product.model, family=product.family)
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
    # The stock 482's sensor-setup emitter copies five cached bytes but sets
    # native payload length to eight: four destination/block and four values.
    sizes = {3: 3, 5: 5, 6: 5, 7: 6, 9: 5, 13: 4 if len(r.remaining) == 4 else 5,
             15: 6, 0x26: 8, 0xa3: 2, 0xa4: 5, 0xa5: 5}
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
    ZONE_ADJUSTMENT: ("ZoneSynchAdjustment", octets("synch_cycle", "purging", "target_mode")),
    0x011: ("ZoneSynchQuery", octets()),
    TANK_STATUS: ("TankStatus", lambda r: TankStatus.read(r).fields()),
    SNOW_REQUEST: ("SnowmeltZoneStatusRequest", snow_request),
    0x1f3: ("IORequestResponse", io_change),
    0x1f4: ("IORequest", octets("subnet_id", "node_id", "channel_id")),
    0x1f5: ("IODiscoverReport", io_discovery),
    0x1f6: ("IODiscoverRequest", octets("subnet_id", "node_id")),
    ACCESS_STATUS: ("SecureGatewayAccess", lambda r: bits(r.byte(), ("request", 0, 1), ("access_open", 1, 1))),
    GROUP_REPORT: ("SetpointEnableReport", group_report),
    GROUP_UPDATE: ("SetpointRemoteEnable", octets("group_id", "enable")),
    0x25f: ("FanDuctStatus", lambda r: bits(r.byte(), ("fan1", 0, 3), ("fan2", 2, 3),
        ("optimum_start", 4, 3), ("freeze_protect", 6, 1)) |
        {"duct_temp_raw": r.word(), "coil_temp_raw": r.word()}),
    0x26f: ("SlabStatus", lambda r: {"slab_status": r.byte() & 7, "target_raw": r.byte(), "current_raw": r.word()}),
    0x287: ("CalibrationMode", octets("mode")),
    0x2ef: ("NetworkLock", lambda r: bits(r.byte(), ("access_lock", 0, 1), ("address_lock", 1, 1))),
    0x2f7: ("ScheduleStatus", schedule),
    SCENE_REPORT: ("SceneReport", scene_report),
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
    AIR_MASTER: ("AirGroupMasterStatus", lambda r: AirStatus.read(r, master=True).fields()),
    0x37f: ("WaterCycleStart", lambda r: bits(r.byte(), ("cycle_length", 0, 127), ("hold_override", 7, 1))),
    0x387: ("NetworkTestTime", lambda r: {"additional_time": r.word()}),
    0x38f: ("NetworkTestMode", lambda r: {"test_mode": r.byte(), "expected_duration": r.word()}),
    0x397: ("EndPurge", target_selection),
    0x39f: ("StartPurge", target_selection),
    0x3a7: ("StartOverrideOn", target_selection),
    0x3af: ("EndOverrideOff", target_selection),
    0x3b7: ("EndOverrideOn", target_selection),
    0x3bf: ("StartOverrideOff", target_selection),
    PLANT_LOOP_STATUS: ("PlantLoopStatus", lambda r: LoopStatus.read(r, master=False).fields()),
    0x3e7: ("TakingAddress", lambda r: {"previous_subnet": r.byte()} | bits(
        r.byte(), ("previous_node", 0, 31), ("locked", 5, 1))),
    0x3ef: ("AddressTaken", octets()),
    MASTER_PRESENT: ("MasterPresent", master_present),
    SNOW_REPORT: ("SnowmeltZoneStatus", lambda r: SnowmeltReport.read(r).fields()),
    0x1f2: ("IOChangeReport", io_change),
    0x22f: ("SetpointStatus", setpoint),
    DHW_STATUS: ("DHWStatus", lambda r: DhwStatus.read(r).fields()),
    0x24f: ("PlantOutputStatus", plant),
    0x2d7: ("BlockRequest", block_request),
    0x2df: ("BlockUpdate", block_update),
    0x2e7: ("BlockReport", block_report),
    CLOCK_REPORT: ("RealTimeClockReport", clock_report),
    0x32f: ("OutdoorTemperatureReport", outdoor),
    0x337: ("MaximumBoilerPercent", lambda r: {"max_value": r.byte()}),
    0x33f: ("CycleLengthRequest", lambda r: {"air_group_id": r.byte(), "requested_length": r.byte()}),
    0x367: ("StageHeatDemand", stage),
    AIR_MEMBER: ("AirGroupMemberStatus", lambda r: AirStatus.read(r, master=False).fields()),
    0x3c7: ("RelayStatusHeartbeat", lambda r: subnet(r.byte())),
    MASTER_STATUS: ("MasterStatus", lambda r: LoopStatus.read(r, master=True).fields()),
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
