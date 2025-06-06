import uuid
from uuid import UUID as std_UUID
from gel.protocol.protocol import UUID as c_UUID

import time


TEST_UUID = uuid.uuid4()
TEST_CUUID = c_UUID(TEST_UUID.bytes)

ubytes = uuid.uuid4().bytes
ustr = str(std_UUID(bytes=ubytes))

print(str(std_UUID(bytes=ubytes)))
print(str(c_UUID(ubytes)))

N = 1_000_000


assert issubclass(c_UUID, std_UUID)
assert isinstance(c_UUID(ubytes), std_UUID)
assert c_UUID(ubytes).bytes == std_UUID(bytes=ubytes).bytes, \
    f'{ubytes}: {c_UUID(ubytes).bytes}'
assert c_UUID(ubytes).hex == std_UUID(bytes=ubytes).hex
assert c_UUID(ubytes).int == std_UUID(bytes=ubytes).int
assert c_UUID(str(std_UUID(bytes=ubytes))).int == std_UUID(bytes=ubytes).int
assert str(c_UUID(ubytes)) == str(std_UUID(bytes=ubytes))
assert hash(c_UUID(ubytes)) == hash(std_UUID(bytes=ubytes))


print()

print(repr(std_UUID(bytes=ubytes)))
print(repr(c_UUID(ubytes)))


print()

st = time.monotonic()
for _ in range(N):
    std_UUID(bytes=ubytes)
std_total = time.monotonic() - st
print(f'std_UUID(bytes):\t  {std_total:.4f}')

st = time.monotonic()
for _ in range(N):
    c_UUID(ubytes)
c_total = time.monotonic() - st
print(f'c_UUID(bytes):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

st = time.monotonic()
for _ in range(N):
    object()
print(f'object():\t\t  {time.monotonic() - st:.4f}')


print()

st = time.monotonic()
for _ in range(N):
    std_UUID(ustr)
std_total = time.monotonic() - st
print(f'std_UUID(str):\t\t  {std_total:.4f}')

st = time.monotonic()
for _ in range(N):
    c_UUID(ustr)
c_total = time.monotonic() - st
print(f'c_UUID(str):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')


print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    str(u)
std_total = time.monotonic() - st
print(f'str(std_UUID()):\t  {std_total:.4f}')

u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    str(u)
c_total = time.monotonic() - st
print(f'str(c_UUID()):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

u = object()
st = time.monotonic()
for _ in range(N):
    str(u)
print(f'str(object()):\t\t  {time.monotonic() - st:.4f}')


print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    u.bytes
std_total = time.monotonic() - st
print(f'std_UUID().bytes:\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    u.bytes
c_total = time.monotonic() - st
print(f'c_UUID().bytes:\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    u.int
std_total = time.monotonic() - st
print(f'std_UUID().int:\t\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    u.int
c_total = time.monotonic() - st
print(f'c_UUID().int:\t\t* {c_total:.4f}')

print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    u.hex
std_total = time.monotonic() - st
print(f'std_UUID().hex:\t\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    u.hex
c_total = time.monotonic() - st
print(f'c_UUID().hex:\t\t* {c_total:.4f}')

print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    hash(u)
std_total = time.monotonic() - st
print(f'hash(std_UUID()):\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    hash(u)
c_total = time.monotonic() - st
print(f'hash(c_UUID()):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

print()

dct = {_: _ for _ in range(1000)}

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    dct.get(u)
std_total = time.monotonic() - st
print(f'dct[std_UUID()]:\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    dct.get(u)
c_total = time.monotonic() - st
print(f'dct[c_UUID()]:\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    _ = u == TEST_UUID
std_total = time.monotonic() - st
print(f'std_UUID() ==:\t\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    _ = u == TEST_CUUID
c_total = time.monotonic() - st
print(f'c_UUID() ==:\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')
