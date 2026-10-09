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
# A damaged block (a copy overwritten by a transfer and not repaired, INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE)
# is judged by its identifier, its payload cannot be trusted:
# - a copy of an object that its author still holds, or a damaged own block, fails the comparison;
# - a copy of an object that its author listed as released is a damaged zombie, reported only;
# - otherwise its fate is unknown (e.g. the author restarted and lost its list), the result is unconfirmed.
#
# User messages (Tests/Replication -u): every node lists its incarnation and the count of messages it sent
# (sent INCARNATION COUNT) and the last message received from every author (received AUTHOR INCARNATION SEQUENCE).
# The peers must have received the last message of the incarnation that wrote the dump, otherwise the tail
# of the stream was lost; gaps inside the stream are checked by the test itself.
#
# Exit status: 0 = converged, 1 = missing, mismatched or damaged live blocks or a lost tail of user messages,
# 2 = usage error, 3 = damaged blocks of unknown objects

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
    damaged = set()
    released = set()
    messages = {'sent': None, 'received': {}}
    with open(path) as file:
        for line in file:
            # identifier length crc32c author sequence # number type count damaged
            # released identifier
            fields = line.split('#')
            values = fields[0].split()
            extra  = fields[1].split() if len(fields) > 1 else []
            if (len(values) == 2) and (values[0] == 'released'):
                released.add(values[1])
                continue
            if (len(values) == 3) and (values[0] == 'sent'):
                messages['sent'] = (values[1], int(values[2]))
                continue
            if (len(values) == 4) and (values[0] == 'received'):
                messages['received'][values[1]] = (values[2], int(values[3]))
                continue
            if len(values) >= 5:
                if (len(extra) > 3) and (extra[3] == '1'):
                    # A damaged own block (allocated by the test itself) is a live object of this node
                    damaged.add((values[0], extra[1] == RECOVERABLE))
                    continue
                blocks[values[0]] = (tuple(values[1:5]), extra[1] if len(extra) > 1 else None)
    return blocks, damaged, released, messages


def main(arguments):
    if (len(arguments) < 4) or (len(arguments) % 2 != 0):
        print('Usage: Compare.py NAME1 DUMP1 NAME2 DUMP2 [NAME3 DUMP3 ...]', file=sys.stderr)
        return 2

    names = arguments[0::2]
    loads = {name: LoadDump(path) for name, path in zip(names, arguments[1::2])}
    dumps = {name: value[0] for name, value in loads.items()}
    failed = False
    unconfirmed = False

    # Objects still held by their authors and objects their authors released
    live = set()
    released = set()
    for name in names:
        author = GetIdentifier(name)
        live |= {key for key, value in dumps[name].items() if (value[0][2] == author) and (value[1] in (None, RECOVERABLE))}
        released |= loads[name][2]

    for name in names:
        author = GetIdentifier(name)
        own = {key: value[0] for key, value in dumps[name].items() if (value[0][2] == author) and (value[1] in (None, RECOVERABLE))}
        resurrected = sum(1 for value in dumps[name].values() if (value[0][2] == author) and (value[1] not in (None, RECOVERABLE)))

        for peer in names:
            if peer == name:
                continue

            copy = {key: value[0] for key, value in dumps[peer].items() if value[0][2] == author}
            missing = [key for key in own if (key not in copy) and (key not in {key for key, _ in loads[peer][1]})]
            mismatch = [key for key in own if (key in copy) and (copy[key] != own[key])]
            zombies = [key for key in copy if key not in own]
            foreign = sum(1 for value in dumps[peer].values() if value[0][2] == '-')

            print(f'{name} -> {peer}: live={len(own)} missing={len(missing)} mismatch={len(mismatch)} zombies={len(zombies)} resurrected={resurrected} foreign={foreign}')

            for key in (missing + mismatch)[:5]:
                print(f'   {key} author={own[key]} peer={copy.get(key)}')

            failed |= bool(missing or mismatch)

    for name in names:
        damaged = loads[name][1]
        alive = [key for key, own in damaged if own or (key in live)]
        deleted = [key for key, own in damaged if (not own) and (key not in live) and (key in released)]
        unknown = [key for key, own in damaged if (not own) and (key not in live) and (key not in released)]

        if damaged:
            print(f'{name}: damaged live={len(alive)} deleted={len(deleted)} unknown={len(unknown)}')

        for key in (alive + unknown)[:5]:
            print(f'   {key} damaged {"live" if key in live else "unknown"}')

        failed |= bool(alive)
        unconfirmed |= bool(unknown)

    for name in names:
        sent = loads[name][3]['sent']
        if (sent is None) or (sent[1] == 0):
            continue

        for peer in names:
            if peer == name:
                continue

            received = loads[peer][3]['received'].get(GetIdentifier(name))
            last = received[1] if (received is not None) and (received[0] == sent[0]) else 0

            print(f'{name} -> {peer}: messages sent={sent[1]} last received={last}')

            failed |= (last != sent[1])

    return 1 if failed else 3 if unconfirmed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
