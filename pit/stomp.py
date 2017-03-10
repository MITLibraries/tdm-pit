import asyncio
from collections import deque

from stompest.protocol import StompParser, StompSession, StompSpec
from stompest.protocol.frame import StompHeartBeat


class Protocol:
    def __init__(self, host, port, loop):
        self._transport = AsyncTransport(host, port, loop)
        self.session = StompSession(version=StompSpec.VERSION_1_2)

    async def connect(self):
        await self._transport.connect()
        frame = self.session.connect(heartBeats=(0, 60000))
        await self.send_frame(frame)
        while True:
            # The heartbeat can sometimes come before the CONNECTED frame
            frame = await self.receive_frame()
            if frame != StompHeartBeat():
                break
        self.session.connected(frame)

    async def disconnect(self):
        await self.send_frame(self.session.disconnect())
        self._transport.disconnect()

    async def send(self, destination, body=b'', headers=None):
        await self.send_frame(self.session.send(destination, body, headers))

    async def subscribe(self, destination, context=None):
        frame, token = self.session.subscribe(destination, headers={'id': '1'},
                                              context=context)
        await self.send_frame(frame)
        return token

    def message(self, frame):
        return self.session.message(frame)

    def subscription(self, token):
        return self.session.subscription(token)

    async def send_frame(self, frame):
        await self._transport.send(frame)

    async def receive_frame(self):
        frame = await self._transport.receive()
        self.session.received()
        return frame

    @property
    def lastReceived(self):
        return self.session.lastReceived


class AsyncTransport:
    def __init__(self, host, port, loop):
        self.host, self.port, self.loop = host, port, loop
        self._parser = StompParser(StompSpec.VERSION_1_2)
        self._frames = deque()

    async def connect(self):
        self._reader, self._writer = \
            await asyncio.open_connection(self.host, self.port, loop=self.loop)
        self._parser.reset()

    def disconnect(self):
        self._writer.close()

    async def receive(self):
        if not self._frames:
            await self._read_frames()
        return self._frames.popleft()

    async def send(self, frame):
        self._writer.write(bytes(frame))
        await self._writer.drain()

    async def _read_frames(self):
        while not self._parser.canRead():
            data = await self._reader.read(1024)
            if not data:
                raise BrokenSocketError()
            self._parser.add(data)
        while self._parser.canRead():
            frame = self._parser.get()
            self._frames.append(frame)


class BrokenSocketError(Exception):
    pass
