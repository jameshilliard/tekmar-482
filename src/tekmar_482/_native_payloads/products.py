"""Native product identities and addressed-read hints, not live capabilities."""

from collections.abc import Mapping
from dataclasses import dataclass

from .layouts import BLOCK_SELECTORS, MULTI_WEEK, LinkCounterBlock


@dataclass(frozen=True, slots=True)
class NativeProduct:
    model: str
    family: str

    @property
    def candidate_blocks(self) -> tuple[int, ...]:
        """Candidates for reads, not a whitelist or evidence that a field exists."""
        return _BLOCKS.get(self.model, (0,))


# Read these blocks first when present in a device's candidate set.
AVAILABILITY_FIELDS = {
    0x35: "available_boilers", 0x50: "available_mix_pumps",
    0x5b: "available_tank_pumps", 0x97: "available_heat_pumps",
}
_BOILER_INSTANCES = (0x3c, 0x3d, 0x43, 0x45)
INSTANCE_AVAILABILITY = {**dict.fromkeys(_BOILER_INSTANCES, 0x35),
                         0x4d: 0x50, 0x5a: 0x5b, 0x98: 0x97}


def block_selectors(block: int, product: NativeProduct | None, subnet: int,
                    availability: Mapping[int, int]) -> tuple[int | None, ...]:
    """Candidate wire arguments for one device. A live zero mask is not absent."""
    if block not in BLOCK_SELECTORS:
        return (None,)
    if block in (LinkCounterBlock.BUS_ERRORS, LinkCounterBlock.BUS_EVENTS):
        bus = subnet & 7
        # SW 11's base Device lists no ports; only ResetControl supplies an index.
        return (bus - 1,) if bus and product and product.family == "reset_control" else ()
    if product is not None:
        if product.model == "033" and block in MULTI_WEEK:
            return (0, 1, 2, 3)
        if product.model in ("400", "401", "402"):
            if block in _BOILER_INSTANCES:
                return (0,)
            if block == 0x44:
                return (0, 129)
        if product.family == "reset_control":
            if block == 0x44:
                return (0,) if product.model == "406" else (0, 1)
    if block in INSTANCE_AVAILABILITY and (mask := availability.get(INSTANCE_AVAILABILITY[block])) is not None:
        return tuple(i for i in range(8) if mask & (1 << i))
    return (0,)  # Reference probe, not evidence there are no other instances.


# Model, family, Tektra numbers, J numbers. Some early revisions share
# another model's Tektra number. A J number alone is not an identity.
_MODELS: tuple[tuple[str, str, tuple[int, ...], tuple[int, ...]], ...] = (
    ("033", "timer", (101103,), (1173,)),
    ("161", "setpoint_control", (101101, 3006346), (1141,)),
    ("162", "setpoint_control", (101102, 3071882), (1142,)),
    ("274", "reset_control", (102002,), (1188,)),
    ("275", "reset_control", (102001,), (1183,)),
    ("284", "reset_control", (105601,), (1220,)),
    ("325", "zone_expansion_module", (99801,), ()),
    ("326", "zone_expansion_module", (99901,), ()),
    ("335", "zone_manager", (99601,), ()),
    ("336", "zone_manager", (99701,), ()),
    ("337", "zone_manager", (99602,), ()),
    ("345", "power_manager", (99603,), ()),
    ("346", "power_manager", (101901,), (1159,)),
    ("400", "reset_control", (103302, 104101), (1191, 1204)),
    ("401", "reset_control", (103501, 104102), (1191, 1205)),
    ("402", "reset_control", (103301, 104103), (1191, 1206)),
    ("403", "reset_control", (104104,), (1207,)),
    ("406", "reset_control", (104106,), (1209,)),
    ("420", "reset_control", (99101,), (1122,)),
    ("421", "reset_control", (99102,), (1123,)),
    ("422", "reset_control", (99103,), (1124,)),
    ("423", "reset_control", (100601,), (1147,)),
    ("440", "mixing_module", (100001, 100002), (1125,)),
    ("441", "mixing_module", (101001,), (1143,)),
    ("444", "mixing_module", (101801,), (1164, 1125)),
    ("479", "user_switch", (100901,), (1144,)),
    ("481", "user_switch", (101401, 101901), (1145,)),
    ("527", "thermostat", (102301,), (1180,)),
    ("528", "thermostat", (102302,), (1181,)),
    ("529", "thermostat", (102303,), (1208,)),
    ("530", "thermostat", (102304,), (1182,)),
    ("532", "thermostat", (107201,), (1234,)),
    ("537", "thermostat", (100102,), (1161,)),
    ("538", "thermostat", (100103,), (1162,)),
    ("540", "thermostat", (100101,), (1163,)),
    ("541", "thermostat", (99301, 15041281, 10599181), (1118,)),
    ("542", "thermostat", (99302, 10599181, 15106817), (1116,)),
    ("543", "thermostat", (99401,), (1126,)),
    ("544", "thermostat", (99203,), (1127,)),
    ("545", "thermostat", (99202,), (1128,)),
    ("546", "thermostat", (99201,), (1129,)),
    ("552", "thermostat", (105103,), (1213,)),
    ("553", "thermostat", (105102,), (1222,)),
    ("554", "thermostat", (105101,), ()),
    ("557", "thermostat", (104401,), (1215,)),
    ("654", "snowmelt_control", (105801,), (1226,)),
    ("670", "snowmelt_control", (108401,), ()),
    ("671", "snowmelt_control", (108402,), ()),
    ("1049", "reset_control", (104901,), (1202,)),
)

_INDEX: dict[int, list[tuple[NativeProduct, tuple[int, ...]]]] = {}
for _model, _family, _numbers, _versions in _MODELS:
    _product = NativeProduct(_model, _family)
    for _number in _numbers:
        _INDEX.setdefault(_number, []).append((_product, _versions))


_SWAPPED_J = {24068: 1118, 23556: 1116, 29956: 1141, 30212: 1142}


def corrected_identification(product_number: int, j_number: int | None) -> tuple[int, int | None]:
    """SW 11 Block.fixup_device_identification, without guessing from a J number alone."""
    if product_number == 0 and j_number == 1161:
        return 100102, j_number
    if j_number is not None and (corrected := _SWAPPED_J.get(j_number)) is not None:
        if any(corrected in versions for _, versions in _INDEX.get(product_number, ())):
            return product_number, corrected
    return product_number, j_number


def identify_product(product_number: int, j_number: int | None = None) -> NativeProduct | None:
    """Resolve one catalog match; keep missing or contradictory identities unknown."""
    if j_number == 0:  # Explicit zero is a corrupt report; an unsupported field is different.
        return None
    product_number, j_number = corrected_identification(product_number, j_number)
    candidates = [p for p, versions in _INDEX.get(product_number, ())
                  if j_number is None or not versions or j_number in versions]
    return candidates[0] if len(candidates) == 1 else None


def product_revision(fields: Mapping[str, object]) -> tuple[NativeProduct | None, int | None]:
    """Identify a device from supported identification fields; Z is zero-based A..Z."""
    number, part, z = (fields.get(k) for k in ("product_number", "j_number", "z_number"))
    product = identify_product(number, part if isinstance(part, int) else None) if isinstance(number, int) else None
    return product, z if isinstance(z, int) and z in range(26) else None


# SW 11 per-model read tables, including zero-mask rows. Always allow the
# identity probe. 654 block CD is also known from an addressed readback.
_BLOCKS: dict[str, tuple[int, ...]] = {
    "033": (0x00, 0x14, 0x1b, *range(0x38, 0x3c)),
    "1049": (0x00, 0x02, 0x0b, 0x14, 0x23, 0x24, 0x36, 0x41, 0x43, 0x44),
    "161": (0x00, 0x01, 0x0e, 0x0f, 0x14, 0x17, 0x1b, *range(0x26, 0x29), 0x2a, 0x2c, 0x2e, 0x32),
    "162": (0x00, 0x01, 0x0b, 0x0c, *range(0x0e, 0x15), 0x17, 0x1b, *range(0x26, 0x29), 0x2a, 0x2c, 0x2e, 0x30, 0x32),
    "274": (0x00, 0x02, 0x05, 0x0b, *range(0x0f, 0x15), *range(0x23, 0x26), 0x35, 0x36, 0x3c, *range(0x3e, 0x47), *range(0x48, 0x4b)),
    "275": (0x00, 0x02, 0x05, 0x0b, *range(0x0f, 0x15), *range(0x23, 0x26), 0x35, 0x36, *range(0x3c, 0x47), 0x48, 0x49),
    "284": (0x00, 0x02, 0x0b, *range(0x0f, 0x15), *range(0x23, 0x26), 0x35, 0x36, *range(0x3e, 0x47), 0x4e, 0x4f),
    **dict.fromkeys(("346", "440", "441", "444", "479", "481"), (0x00,)),
    **dict.fromkeys(("400", "401"), (0x00, 0x02, 0x0b, 0x14, *range(0x23, 0x26), 0x3c, 0x41, 0x43, 0x44, 0x4b, 0x4c, 0x4e)),
    **dict.fromkeys(("402", "403"), (0x00, 0x02, 0x0b, 0x14, *range(0x22, 0x26), 0x3c, 0x41, 0x43, 0x44, *range(0x4b, 0x4f), 0x50)),
    "406": (0x00, 0x02, 0x04, 0x0b, 0x14, *range(0x22, 0x26), 0x33, 0x36, 0x3d, 0x41, 0x43, 0x44, 0x48, 0x49, 0x4d, 0x50, 0x5a, 0x5b, *range(0x83, 0x8b), *range(0x93, 0x98)),
    "420": (0x00, 0x02, 0x0b, 0x0f, 0x14, *range(0x23, 0x26)),
    "421": (0x00, 0x02, 0x0b, 0x0f, 0x14, 0x22, 0x23),
    "422": (0x00, 0x02, 0x0b, 0x0f, 0x14, *range(0x22, 0x26)),
    "423": (0x00, 0x02, 0x0b, 0x0f, 0x14, *range(0x22, 0x26), 0x33, 0x35, 0x36),
    **dict.fromkeys(("527", "537"), (0x00, 0x03, 0x05, 0x0e, 0x0f, 0x14, 0x15, 0x1b)),
    **dict.fromkeys(("528", "538"), (0x00, 0x03, 0x05, 0x07, 0x0e, 0x0f, 0x14, 0x15, 0x17, 0x1b, 0x37)),
    "529": (0x00, 0x03, 0x05, 0x07, 0x0e, 0x0f, 0x14, 0x15, 0x17, 0x1b),
    "530": (0x00, 0x01, *range(0x03, 0x07), 0x08, 0x09, 0x0e, 0x0f, 0x14, 0x15, 0x1b, 0x37),
    "532": (0x00, 0x03, 0x05, 0x07, *range(0x0d, 0x10), 0x14, 0x15, 0x17, 0x18, 0x1b),
    "540": (0x00, 0x01, *range(0x03, 0x07), 0x08, 0x09, 0x0f, 0x14, 0x15, 0x1b, 0x37),
    "541": (0x00, 0x01, 0x03, 0x05, 0x07, *range(0x0d, 0x10), *range(0x14, 0x18), 0x1b),
    "542": (0x00, 0x01, 0x03, 0x05, 0x07, 0x0b, *range(0x0d, 0x19), 0x1b),
    "543": (0x00, 0x01, 0x03, 0x05, 0x07, 0x0b, *range(0x0d, 0x15)),
    "544": (0x00, 0x01, *range(0x03, 0x0a), 0x0b, *range(0x0d, 0x15), *range(0x16, 0x19), 0x1c, 0x1e, 0x20),
    "545": (0x00, 0x01, *range(0x03, 0x19), *range(0x1c, 0x1f), 0x20),
    "546": (0x00, 0x01, *range(0x03, 0x1a), *range(0x1c, 0x22)),
    "552": (0x00, 0x01, 0x03, 0x05, 0x07, 0x0b, *range(0x0d, 0x19), 0x1b, 0x53, 0x58),
    "553": (0x00, 0x01, *range(0x03, 0x0c), *range(0x0d, 0x19), *range(0x53, 0x56), 0x97, *range(0xa3, 0xa6), 0xab, 0xac),
    "557": (0x00, 0x01, *range(0x03, 0x0c), *range(0x0d, 0x1a), 0x1b, *range(0x53, 0x56), 0x97, *range(0xa3, 0xa6), 0xab, 0xac),
    "654": (0x00, *range(0x0e, 0x15), 0x22, 0x23, 0x3d, 0x43, 0x44, 0x48, 0x49, *range(0x4d, 0x50), *range(0x71, 0x7e), *range(0xc0, 0xd3)),
}
