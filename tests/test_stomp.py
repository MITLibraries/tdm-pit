from asyncio import coroutine
from unittest.mock import Mock

import pytest
from stompest.protocol import StompFrame

from pit.stomp import AsyncTransport, Protocol, BrokenSocketError


@pytest.fixture
def transport(event_loop):
    t = AsyncTransport('localhost', 61613, event_loop)
    t._reader = Mock()
    t._writer = Mock()
    t._writer.drain.side_effect = coroutine(Mock())
    t.connect = coroutine(Mock())
    return t


class TestAsyncTransport:
    @pytest.mark.asyncio
    async def test_receive_returns_first_frame(self, transport):
        transport._frames.append(StompFrame('MESSAGE', body=b'FOOBAR'))
        transport._frames.append(StompFrame('MESSAGE', body=b'FOOBAZ'))
        frame = await transport.receive()
        assert frame.body == b'FOOBAR'

    @pytest.mark.asyncio
    async def test_receive_reads_frames_if_none(self, transport):
        transport._reader.read.side_effect = \
            coroutine(Mock(return_value=b'MESSAGE\n\nFOOBAR\x00'))
        frame = await transport.receive()
        assert frame.body == b'FOOBAR'

    @pytest.mark.asyncio
    async def test_read_frames_adds_multiple_frames_to_buffer(self, transport):
        transport._reader.read.side_effect = \
            coroutine(Mock(return_value=b'MESSAGE\n\nFOOBAR\x00'
                                        b'MESSAGE\n\nFOOBAZ\x00'))
        await transport._read_frames()
        assert transport._frames.popleft().body == b'FOOBAR'
        assert transport._frames.popleft().body == b'FOOBAZ'

    @pytest.mark.asyncio
    async def test_read_frames_raises_error_on_empty_socket(self, transport):
        transport._reader.read.side_effect = coroutine(Mock(return_value=b''))
        with pytest.raises(BrokenSocketError):
            await transport._read_frames()

    def test_disconnect_closes_writer(self, transport):
        transport.disconnect()
        assert transport._writer.close.called


class TestProtocol:
    @pytest.mark.asyncio
    async def test_connect_sends_connect_frame(self, transport):
        p = Protocol('localhost', 61613, None)
        transport._reader.read.side_effect = \
            coroutine(Mock(return_value=b'CONNECTED\nversion:1.2\n\n\x00'))
        p._transport = transport
        await p.connect()
        frame = transport._writer.write.call_args[0][0]
        assert frame.startswith(b'CONNECT\n')
        assert p.session.state == p.session.CONNECTED

    @pytest.mark.asyncio
    async def test_disconnect_sends_disconnect_frame(self, transport):
        p = Protocol('localhost', 61613, None)
        p._transport = transport
        p.session._state = p.session.CONNECTED
        await p.disconnect()
        frame = transport._writer.write.call_args[0][0]
        assert frame.startswith(b'DISCONNECT')
        assert transport._writer.close.called

    @pytest.mark.asyncio
    async def test_send_sends_send_frame(self, transport):
        p = Protocol('localhost', 61613, None)
        p._transport = transport
        p.session._state = p.session.CONNECTED
        await p.send('/queue/foo', b'FOOBAR')
        frame = transport._writer.write.call_args[0][0]
        assert frame == b'SEND\ndestination:/queue/foo\n\nFOOBAR\x00'

    @pytest.mark.asyncio
    async def test_subscribe_sends_subscribe_frame(self, transport):
        p = Protocol('localhost', 61613, None)
        p._transport = transport
        p.session._state = p.session.CONNECTED
        await p.subscribe('/queue/foo')
        frame = transport._writer.write.call_args[0][0]
        assert frame.startswith(b'SUBSCRIBE\n')
        assert b'destination:/queue/foo' in frame

    def test_subscription_returns_sub_context(self, transport):
        p = Protocol('localhost', 61613, None)
        p.session._state = p.session.CONNECTED
        _, tkn = p.session.subscribe('/queue/foo', headers={'id': '1'},
                                     context='FOOBAR')
        assert p.subscription(tkn) == 'FOOBAR'

    def test_message_returns_token(self, transport):
        p = Protocol('localhost', 61613, None)
        p.session._state = p.session.CONNECTED
        _, tkn = p.session.subscribe('/queue/foo', headers={'id': '1'},
                                     context='FOOBAR')
        assert p.message(StompFrame('MESSAGE', body=b'FOOBAZ',
                         headers={'destination': '/queue/foo',
                                  'subscription': '1',
                                  'message-id': '1'})) == \
            ('id', '1')
