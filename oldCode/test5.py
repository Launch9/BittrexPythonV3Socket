import gzip, zlib
f = gzip.open('foo.gz', 'wb')
f.write(b"hello world")
f.close()

c = open('foo.gz', 'rb').read()

print(c)