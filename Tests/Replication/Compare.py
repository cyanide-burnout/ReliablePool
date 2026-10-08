#!/usr/bin/env python3
# Compare block dumps of Tests/Replication (-o FILE) taken on several nodes
#
# Usage: Compare.py NAME1 DUMP1 NAME2 DUMP2 [NAME3 DUMP3 ...]
#
# NAME is the node name passed with -n, DUMP is the file written by that node.
# For every author the blocks it still holds must be present on each peer with the same
# identifier, length, CRC32C and sequence, otherwise they are reported as missing or mismatch.
# Own blocks are the ones the author allocated itself (RELIABLE_TYPE_RECOVERABLE in Tests/Replication),
# a block carrying the author's payload but held as a received copy on the author is resurrected:
# the author freed it and then received it back from a peer.
# Blocks of an author that exist on a peer but not on the author are zombies (removals lost
# while the nodes were disconnected). Resurrected blocks and zombies are reported but do not fail
# the comparison.
#
# Exit status: 0 = converged, 1 = missing or mismatched blocks, 2 = usage error

import sys
import uuid

# Node identifier is uuid_generate_sha1() over the node name in the OID namespace, the same as uuid5
NAMESPACE = uuid.UUID('6ba7b812-9dad-11d1-80b4-00c04fd430c8')

# Type of the blocks allocated by Tests/Replication itself
RECOVERABLE = '1'


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
            fields = line.split('#')
            values = fields[0].split()
            extra  = fields[1].split() if len(fields) > 1 else []
            if len(values) >= 5:
                blocks[values[0]] = (tuple(values[1:5]), extra[1] if len(extra) > 1 else None)
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
        own = {key: value[0] for key, value in dumps[name].items() if (value[0][2] == author) and (value[1] in (None, RECOVERABLE))}
        resurrected = sum(1 for value in dumps[name].values() if (value[0][2] == author) and (value[1] not in (None, RECOVERABLE)))

        for peer in names:
            if peer == name:
                continue

            copy = {key: value[0] for key, value in dumps[peer].items() if value[0][2] == author}
            missing = [key for key in own if key not in copy]
            mismatch = [key for key in own if (key in copy) and (copy[key] != own[key])]
            zombies = [key for key in copy if key not in own]
            foreign = sum(1 for value in dumps[peer].values() if value[0][2] == '-')

            print(f'{name} -> {peer}: live={len(own)} missing={len(missing)} mismatch={len(mismatch)} zombies={len(zombies)} resurrected={resurrected} foreign={foreign}')

            for key in (missing + mismatch)[:5]:
                print(f'   {key} author={own[key]} peer={copy.get(key)}')

            status |= bool(missing or mismatch)

    return status


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
