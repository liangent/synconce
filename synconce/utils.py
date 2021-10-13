import hashlib


def append_transfer(srcf, destf):
    transferred = 0
    while len(data := srcf.read(32768)) > 0:
        destf.write(data)
        transferred += len(data)
    return transferred


def head_sha1(fileobj, head_size):
    sha1sum = hashlib.sha1()
    file_to_read = head_size

    while file_to_read > 0:
        data = fileobj.read(min(32768, file_to_read))
        sha1sum.update(data)
        file_to_read -= len(data)
        if len(data) == 0:
            break

    if file_to_read > 0:
        # file reading ends early. broken file?
        return None

    return sha1sum.hexdigest()
