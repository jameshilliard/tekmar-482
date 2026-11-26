import asyncio
import json
import unittest
from datetime import UTC, datetime, timedelta, tzinfo
from io import StringIO

from tekmar_482 import (
    BroadcastIssue,
    DeviceGroupBinding,
    FailureReason,
    LineBroadcast,
    NativeAddress,
    NativeBlockQuery,
    NativeFrame,
    NativeMessage,
    Packet,
    SetpointApplication,
    TxStatus,
    ZoneSyncAdjustment,
    ZoneTargetMode,
)
from tekmar_482.cli import CliError, build_parser, run
from tekmar_482.raw import RawDispatcher
from tekmar_482.tpck import serialize

from .test_bridge import listen, management

_HOST = NativeAddress(0, 0)
_PEER = NativeAddress(0x23, 6)


class _Daylight(tzinfo):
    def __init__(self, minutes: int) -> None:
        self.adjustment = timedelta(minutes=minutes)

    def dst(self, _dt: datetime | None, /) -> timedelta:
        return self.adjustment

    def utcoffset(self, _dt: datetime | None, /) -> timedelta:
        return self.adjustment + timedelta(hours=2)

    def tzname(self, _dt: datetime | None, /) -> str:
        return "Synthetic daylight zone"


class AnnouncementTests(unittest.TestCase):
    def test_civil_time_roundtrip_and_invalid_calendar(self) -> None:
        local = datetime(2007, 1, 8, 0, 30, tzinfo=_Daylight(60))
        send = LineBroadcast.clock((3, 1), local)
        assert send.message.to_bytes() == bytes.fromhex("31f00005 1e bf 10 06 07")
        frame = NativeFrame(3, 100, send.message.to_bytes())
        report = frame.clock
        assert report and report.wall_datetime == local.replace(tzinfo=None)
        assert report.daylight and report.weekday == 7
        assert frame.group_report is None
        for payload in ("0001 ff ffff", "0010 01 1e18", "3c00 00 0018"):
            body = NativeMessage(0x31F, _PEER, bytes.fromhex(payload)).to_bytes()
            invalid = NativeFrame(1, 0, body).clock
            assert invalid and invalid.wall_datetime is None
        assert NativeFrame(1, 0, bytes.fromhex("31f00002 0102")).clock is None
        with self.assertRaisesRegex(ValueError, "one-hour"):
            LineBroadcast.clock((1,), local.replace(tzinfo=_Daylight(30)))
        winter = LineBroadcast.clock((1,), local.replace(tzinfo=UTC), dst_mode=0x85)
        assert winter.message.payload == bytes.fromhex("1e 01 00 07 07 85")

    def test_group_echo_is_not_a_status_and_members_can_disagree(self) -> None:
        update = LineBroadcast.group((0, 2), 12, enabled=True)
        assert update.message.to_bytes() == bytes.fromhex("21f00002 0c01")
        assert NativeFrame(0, 0, update.message.to_bytes()).group_report is None
        for flags, requested, actual in ((0xE1, True, False), (0x1E, False, True)):
            frame = NativeFrame(
                2,
                0,
                NativeMessage(0x20F, _PEER, bytes([12, flags, 9])).to_bytes(),
            )
            assert frame.group_report is not None and frame.group_report.flags == flags
            assert (
                frame.group_report.group,
                frame.group_report.requested,
                frame.group_report.actual,
            ) == (12, requested, actual)
            assert frame.decoded["extension_hex"] == "09"
        assert NativeFrame(1, 0, bytes.fromhex("20f00001 0c")).group_report is None
        cmd = LineBroadcast.zone_sync(
            (0,),
            ZoneSyncAdjustment(1, purging=False, target=ZoneTargetMode.RESET_ONLY),
            source=_PEER,
        )
        assert cmd.message.to_bytes() == bytes.fromhex("01023183 010001")


class GroupReadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.messages: list[NativeMessage] = []
        self.blocks = {
            0: b"\x1f" + (101102).to_bytes(3, "little") + (1142).to_bytes(2, "little"),
            0x26: bytes.fromhex("41 04 05"),
        }
        self.stream = RawDispatcher(self.send, 32)
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        await self.enterAsyncContext(self.stream.receiving(idle.get))

    async def send(self, data: bytes) -> None:
        assert data[:2] == bytes([1, 3])
        cmd = NativeMessage.from_bytes(data[2:])
        self.messages.append(cmd)
        self.stream.feed(Packet(1, bytes([2, 3, 0])))
        if cmd.message_id == 0x2D7:
            assert (
                cmd.payload[2] == _PEER.subnet
                and int.from_bytes(cmd.payload[3:7], "little") == _PEER.request_mask()
            )
            block = int.from_bytes(cmd.payload[:2], "little")
            if (body := self.blocks.get(block)) is not None:
                reply = NativeMessage(0x2E7, _PEER, cmd.payload[:2] + body)
                self.stream.feed(
                    Packet(1, bytes([0, 3, 0, 0, 0, 0]) + reply.to_bytes()),
                )

    async def test_device_group_reads_fresh_assignment_then_sends_to_group_only(
        self,
    ) -> None:
        bind = await DeviceGroupBinding.read(self.stream, 3, _PEER)
        assert bind.assignment.application is SetpointApplication.FLOOR
        assert (
            bind.assignment.group_raw == 5 and bind.port == 3 and bind.address == _PEER
        )
        with self.assertRaisesRegex(ValueError, "expected remote-enable group"):
            bind.announcement(1, enabled=True)
        send = bind.announcement(5, enabled=True)
        assert send.ports == (3,) and send.message.source == _HOST
        assert [m.message_id for m in self.messages] == [0x2D7, 0x2D7]
        (result,) = await send.send(self.stream)
        assert result.link_acknowledged and result.port == 3
        assert self.messages[-1].to_bytes() == bytes.fromhex("21f00002 0501")
        # The group changed: confirmation against a fresh binding must now fail.
        self.blocks[0x26] = bytes.fromhex("41 04 06")
        new = await DeviceGroupBinding.read(self.stream, 3, _PEER)
        with self.assertRaisesRegex(ValueError, "expected remote-enable group"):
            new.announcement(5, enabled=False)
        assert len([m for m in self.messages if m.message_id == 0x21F]) == 1

    async def test_no_group_guesses_or_fallbacks(self) -> None:
        for setup in ("41 02 05", "41 05 05", "41 00 00", "41 fe 06", "01 00", "40 0c"):
            self.blocks[0x26] = bytes.fromhex(setup)
            bind = await DeviceGroupBinding.read(self.stream, 3, _PEER)
            assert bind.assignment.remote_group is None
            with self.assertRaises(ValueError):
                bind.announcement(5, enabled=True)
        self.blocks[0] = b"\x07" + (105801).to_bytes(3, "little")
        self.messages.clear()
        with self.assertRaisesRegex(ValueError, "setpoint control"):
            await DeviceGroupBinding.read(self.stream, 3, _PEER)
        assert len(self.messages) == 1

    async def test_different_ports_can_have_distinct_terminal_results(self) -> None:
        tx: asyncio.Queue[bytes] = asyncio.Queue()
        idle: asyncio.Queue[Packet] = asyncio.Queue()
        stream = RawDispatcher(tx.put, 16)
        async with stream.receiving(idle.get):
            cmd = LineBroadcast.group((0, 2, 3), 4, enabled=False)
            task = asyncio.create_task(cmd.send(stream, timeout=0.1))
            for _ in cmd.ports:
                sent = await tx.get()
                assert sent[2:] == bytes.fromhex("21f00002 0400")
                if sent[1] != 3:
                    outcome = (
                        bytes([2, FailureReason.NO_ACK]) if sent[1] == 2 else bytes([0])
                    )
                    stream.feed(Packet(1, bytes([2, sent[1]]) + outcome))
            ack, failed, timeout = await task
            assert ack.link_acknowledged and not failed.link_acknowledged
            assert failed.port == 2 and timeout.outcome is BroadcastIssue.DEADLINE
            (busy,) = await LineBroadcast.group((3,), 4, enabled=True).send(stream)
            assert busy.outcome is BroadcastIssue.PORT_BUSY and tx.empty()
            stream.feed(Packet(1, bytes([2, 3, TxStatus.ACK])))

    async def test_cli_previews_are_offline_and_device_confirmation_needs_group(
        self,
    ) -> None:
        parser = build_parser()
        for args, wire in (
            (["set-clock", "2007-01-08T00:30", "--dst"], "31f000051ebf100607"),
            (["set-group", "4", "off"], "21f000020400"),
            (
                [
                    "set-zone-sync",
                    "--cycle",
                    "0",
                    "--purge",
                    "on",
                    "--target",
                    "setpoint",
                ],
                "01000003000102",
            ),
        ):
            out = StringIO()
            assert (
                await run(
                    parser.parse_args([*args, "--ports", "2", "0", "--json"]),
                    stdout=out,
                )
                == 0
            )
            preview = json.loads(out.getvalue())["preview"]
            assert preview["body"] == wire and preview["ports"] == [2, 0]
            with self.assertRaisesRegex(CliError, "specify --serial or --tcp"):
                await run(parser.parse_args([*args, "--ports", "0", "--confirm"]))
        with self.assertRaisesRegex(CliError, "expect-group"):
            await run(
                parser.parse_args(
                    [
                        "set-device-group",
                        "1",
                        "0x23",
                        "6",
                        "on",
                        "--confirm",
                        "--tcp",
                        "localhost:1",
                    ],
                ),
            )

    async def test_tcp_cli_previews_refuses_reassignment_and_transmits_exactly_once(
        self,
    ) -> None:
        async def exercise(expected: int, *, confirm: bool) -> None:
            async def serve(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
            ) -> None:
                if (first := await reader.readexactly(1)) == b"?":
                    assert await reader.readexactly(5) == b"\r\nJ\r\n"
                    assert not await reader.read()
                    return
                caps = management(1, 1)
                assert first + await reader.readexactly(len(caps) - 1) == caps
                writer.write(management(1, 4, bytes([1, 0, 5, 3])))
                for method, value in ((2, 1), (9, 0)):
                    cmd = management(method, 0, bytes([value]))
                    assert await reader.readexactly(len(cmd)) == cmd
                    writer.write(management(method, 3, bytes([value])))

                ack = serialize(Packet(1, bytes([2, 3, 0])))
                for block, sample in self.blocks.items():
                    native = NativeBlockQuery(_PEER, block).request(_HOST)
                    request = serialize(Packet(1, bytes([1, 3]) + native.to_bytes()))
                    assert await reader.readexactly(len(request)) == request
                    report = NativeMessage(
                        0x2E7,
                        _PEER,
                        block.to_bytes(2, "little") + sample,
                    )
                    writer.write(
                        ack
                        + serialize(
                            Packet(1, bytes([0, 3, 0, 0, 0, 0]) + report.to_bytes()),
                        ),
                    )
                if confirm and expected == 5:
                    cmd = serialize(Packet(1, bytes.fromhex("0103 21f00002 0501")))
                    assert await reader.readexactly(len(cmd)) == cmd
                    writer.write(ack)
                leave = management(2, 0, b"\x00")
                assert await reader.readexactly(len(leave)) == leave
                writer.write(management(2, 3, b"\x00"))

            async with listen(serve) as endpoint, asyncio.timeout(5):
                args = build_parser().parse_args(
                    [
                        "set-device-group",
                        "3",
                        "0x23",
                        "6",
                        "on",
                        "--expect-group",
                        str(expected),
                        "--tcp",
                        f"{endpoint.address}:{endpoint.port}",
                        "--json",
                        *(["--confirm"] if confirm else []),
                    ],
                )
                out = StringIO()
                if expected != 5:
                    with self.assertRaisesRegex(
                        ValueError,
                        "expected remote-enable group",
                    ):
                        await run(args, stdout=out)
                else:
                    assert await run(args, stdout=out) == 0
                    rows = [json.loads(v) for v in out.getvalue().splitlines()]
                    if confirm:
                        assert rows[0]["broadcast"]["remote_group"] == 5
                        assert rows[1] == {
                            "port": 3,
                            "tx_status": "ACK",
                            "tx_reason": None,
                        }
                    else:
                        assert rows[0]["preview"]["body"] == "21f000020501"

        await exercise(5, confirm=False)
        await exercise(6, confirm=True)
        await exercise(5, confirm=True)
