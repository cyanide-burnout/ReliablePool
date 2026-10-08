#!/usr/bin/env python3
# Compare block dumps of Tests/Replication (-o FILE) taken on several nodes
#
# Usage: Compare.py NAME1 DUMP1 NAME2 DUMP2 [NAME3 DUMP3 ...]
#
# NAME is the node name passed with -n, DUMP is the file written by that node.
# For every author the blocks it still holds must be present on each peer with the same
# identifier, length, CRC32C and sequence, otherwise they are reported as missing or mismatch.
# Blocks of an author that exist on a peer but not on the author are zombies (removals lost
# while the nodes were disconnected), they are reported but do not fail the comparison.
#
# Exit status: 0 = converged, 1 = missing or mismatched blocks, 2 = usage error

import sys
import uuid

# Node identifier is uuid_generate_sha1() over the node name in the OID namespace, the same as uuid5
NAMESPACE = uuid.UUID('6ba7b812-9dad-11d1-80b4-00c04fd430c8')


def GetIdentifier(name):
    try:
        return str(uuid.UUID(name))
    except ValueError:
        return str(uuid.uuid5(NAMESPACE, name))


def LoadDump(path):
    blocks = {}
    with open(path) as file:
        for line in file:
            # identifier length crc32c author sequence # number type count
            fields = line.split('#')[0].split()
            if len(fields) >= 5:
                blocks[fields[0]] = tuple(fields[1:5])
    return blocks


def main(arguments):
    if (len(arguments) < 4) or (len(arguments) % 2 != 0):
        print('Usage: Compare.py NAME1 DUMP1 NAME2 DUMP2 [NAME3 DUMP3 ...]', file=sys.stderr)
        return 2

    names = arguments[0::2]
    dumps = {name: LoadDump(path) for name, path in zip(names, arguments[1::2])}
    status = 0

    for name in names:
        author = GetIdentifier(name)
        own = {key: value for key, value in dumps[name].items() if value[2] == author}

        for peer in names:
            if peer == name:
                continue

            copy = {key: value for key, value in dumps[peer].items() if value[2] == author}
            missing = [key for key in own if key not in copy]
            mismatch = [key for key in own if (key in copy) and (copy[key] != own[key])]
            zombies = [key for key in copy if key not in own]
            foreign = sum(1 for value in dumps[peer].values() if value[2] == '-')

            print(f'{name} -> {peer}: live={len(own)} missing={len(missing)} mismatch={len(mismatch)} zombies={len(zombies)} foreign={foreign}')

            for key in (missing + mismatch)[:5]:
                print(f'   {key} author={own[key]} peer={copy.get(key)}')

            status |= bool(missing or mismatch)

    return status


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
