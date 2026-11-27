"""Native tN4 payloads, independent of the 482's host-side tRPC methods."""

from collections.abc import Callable

from .climate import AIR_GROUP_CONFIG, AIR_MASTER, AIR_MEMBER, AirGroupConfiguration, AirStatus, ThermostatMode
from .codec import Fields, Reader, SETBACK, ShortPayload, temp
from .faults import NETWORK_ERROR, NetworkFault
from .io import IO_CHANGE, IO_DISCOVER, IO_DISCOVERY, IO_REPLY, IO_REQUEST, IoReport
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
from .line import GROUP_UPDATE, LINE_MESSAGES, NETWORK_INIT_REQUEST, LineValue
from .plant import MIXING_PRESENT, MIXING_REPORT, MIXING_UPDATE, PLANT_OUTPUT, MixingPresent, MixingReport, MixingUpdate, PlantOutput
from .products import corrected_identification, identify_product
from .scenes import SCENE_RESPONSE_FIELDS, SceneResponse
from .schedules import DAY_NAMES, SCHEDULE_STATUS, SINGLE_WEEK, DailyEvents, ScheduleBinding, ScheduleCycle, ScheduleEvent, ScheduleStatus
from .service import CALIBRATION_MODE, NETWORK_TEST_MODE, NETWORK_TEST_TIME
from .setpoint import SETPOINT_STATUS, SetpointStatus
from .snowmelt import SNOW_REPORT, SNOW_REQUEST, SnowmeltApplication, SnowmeltReport
from .thermostat import FAN_STATUS, HUMIDITY_STATUS, SLAB_STATUS, FanDuctStatus, HumidityStatus, SlabStatus
from .water import (
    BOILER_MAXIMUM, DHW_STATUS, GENERAL_DEMAND, MASTER_PRESENT, MASTER_STATUS, PLANT_LOOP_STATUS,
    RELAY_HEARTBEAT, STAGE_DEMAND, TANK_STATUS, WATER_CYCLE, BoilerMaximum, DhwStatus, GeneralDemand,
    HeatSource, LoopStatus, RelayHeartbeat, StageCommand, StageDemand, StageSelection, TankStatus, WaterCycle,
)


def _line_fields(read: Callable[[Reader], LineValue]) -> Callable[[Reader], Fields]:
    return lambda r: read(r).fields()


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
    "fan_ventilation_flags": (("fan_cool_stage2", 0, 3), ("ventilation_mode", 2, 3),
                              ("fan_ventilation_reserved", 4, 15)),
    "water_temperature_flags": (("boiler_flag", 0, 1), ("mix_flag", 1, 1), ("tank_flag", 2, 1),
                                ("water_temperature_reserved", 3, 31)),
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
        number, raw_j = fields.get("product_number"), fields.get("j_number")
        if isinstance(number, int):
            j = raw_j if isinstance(raw_j, int) else None
            effective_number, effective_j = corrected_identification(number, j)
            if effective_number != number:
                fields["corrected_product_number"] = effective_number
            if effective_j is not None and effective_j != j:
                fields["corrected_j_number"] = effective_j
            product = identify_product(effective_number, effective_j)
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


def octets(*names: str) -> Callable[[Reader], Fields]:
    def read(r: Reader) -> Fields:
        return dict(zip(names, r.take(len(names)), strict=True))
    return read


def snow_request(r: Reader) -> Fields:
    address, count = r.take(2)
    return {"destination_type": address & 7, "destination_id": address >> 3,
            "count": count, "keys_hex": r.take(count).hex()}


def bits(value: int, *fields: tuple[str, int, int]) -> Fields:
    return {name: (value >> shift) & mask for name, shift, mask in fields}


def master_present(r: Reader) -> Fields:
    if not r.remaining:
        return {}  # The 482 also accepts the header alone as proof of presence.
    fields: Fields = {"boiler_subnet": r.byte()}
    for n in range(1, min(7, len(r.remaining)) + 1):
        fields[f"mix{n}_subnet"] = r.byte()
    return fields


DECODERS: dict[int, tuple[str, Callable[[Reader], Fields]]] = {
    **{mid: (name, _line_fields(read)) for mid, (name, read) in LINE_MESSAGES.items()},
    0x011: ("ZoneSynchQuery", octets()),
    TANK_STATUS: ("TankStatus", lambda r: TankStatus.read(r).fields()),
    SNOW_REQUEST: ("SnowmeltZoneStatusRequest", snow_request),
    IO_REPLY: ("IORequestResponse", lambda r: IoReport.read_value(r).fields()),
    IO_REQUEST: ("IORequest", octets("subnet_id", "node_id", "channel_id")),
    IO_DISCOVERY: ("IODiscoverReport", lambda r: IoReport.read_discovery(r).fields()),
    IO_DISCOVER: ("IODiscoverRequest", octets("subnet_id", "node_id")),
    GROUP_UPDATE: ("SetpointRemoteEnable", octets("group_id", "enable")),
    FAN_STATUS: ("FanDuctStatus", lambda r: FanDuctStatus.read(r).fields()),
    SLAB_STATUS: ("SlabStatus", lambda r: SlabStatus.read(r).fields()),
    CALIBRATION_MODE: ("CalibrationMode", octets("mode")),
    SCHEDULE_STATUS: ("ScheduleStatus", lambda r: ScheduleStatus.read(r).fields()),
    0x307: ("WWSDRequest", octets()),
    0x317: ("RealTimeClockRequest", octets()),
    0x322: ("RHQuery", lambda r: {"target_subnet": r.byte(), "node_selector": int.from_bytes(r.take(4), "little")}),
    HUMIDITY_STATUS: ("RHReport", lambda r: HumidityStatus.read(r).fields()),
    0x327: ("OutdoorTemperatureRequest", octets()),
    MIXING_PRESENT: ("MixingModulePresent", lambda r: MixingPresent.read(r).fields()),
    MIXING_REPORT: ("MixingModuleReport", lambda r: MixingReport.read(r).fields()),
    MIXING_UPDATE: ("MixingModuleUpdate", lambda r: MixingUpdate.read(r).fields()),
    **{kind: (kind.name.title().replace("_", ""), lambda r: StageSelection.read(r).fields())
       for kind in StageCommand},
    GENERAL_DEMAND: ("GeneralDemand", lambda r: GeneralDemand.read(r).fields()),
    AIR_GROUP_CONFIG: ("AirGroupMasterConfig", lambda r: AirGroupConfiguration.read(r).fields()),
    AIR_MASTER: ("AirGroupMasterStatus", lambda r: AirStatus.read(r, master=True).fields()),
    WATER_CYCLE: ("WaterCycleStart", lambda r: WaterCycle(r.byte()).fields()),
    NETWORK_TEST_TIME: ("NetworkTestTime", lambda r: {"additional_time": r.word()}),
    NETWORK_TEST_MODE: ("NetworkTestMode", lambda r: {"test_mode": r.byte(), "expected_duration": r.word()}),
    PLANT_LOOP_STATUS: ("PlantLoopStatus", lambda r: LoopStatus.read(r, master=False).fields()),
    0x3e7: ("TakingAddress", lambda r: {"previous_subnet": r.byte()} | bits(
        r.byte(), ("previous_node", 0, 31), ("locked", 5, 1))),
    0x3ef: ("AddressTaken", octets()),
    MASTER_PRESENT: ("MasterPresent", master_present),
    SNOW_REPORT: ("SnowmeltZoneStatus", lambda r: SnowmeltReport.read(r).fields()),
    IO_CHANGE: ("IOChangeReport", lambda r: IoReport.read_value(r).fields()),
    SETPOINT_STATUS: ("SetpointStatus", lambda r: SetpointStatus.read(r).fields()),
    DHW_STATUS: ("DHWStatus", lambda r: DhwStatus.read(r).fields()),
    PLANT_OUTPUT: ("PlantOutputStatus", lambda r: PlantOutput.read(r).fields()),
    0x2d7: ("BlockRequest", block_request),
    0x2df: ("BlockUpdate", block_update),
    0x2e7: ("BlockReport", block_report),
    BOILER_MAXIMUM: ("MaximumBoilerPercent", lambda r: BoilerMaximum(r.byte()).fields()),
    0x33f: ("CycleLengthRequest", lambda r: {"air_group_id": r.byte(), "requested_length": r.byte()}),
    STAGE_DEMAND: ("StageHeatDemand", lambda r: StageDemand.read(r).fields()),
    AIR_MEMBER: ("AirGroupMemberStatus", lambda r: AirStatus.read(r, master=False).fields()),
    RELAY_HEARTBEAT: ("RelayStatusHeartbeat", lambda r: RelayHeartbeat(r.byte()).fields()),
    MASTER_STATUS: ("MasterStatus", lambda r: LoopStatus.read(r, master=True).fields()),
    NETWORK_ERROR: ("NetworkError", lambda r: NetworkFault.read(r).fields()),
    NETWORK_INIT_REQUEST: ("NetworkInitRequest", lambda _: {}),
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
