import gzip
import json
import socket
from datetime import datetime, timezone
from time import time, sleep

RETROARCH_PORT = 55355
POLL_INTERVAL = 1.01
SYNC_INTERVAL = 6
backoff_sync_interval = SYNC_INTERVAL
PAUSE_DELAY_INTERVAL = 0.05
SIMILARITY_THRESHOLD = 0.95
SERIES_NUMBER = int(round(time()))

FIELD_ITEM_ADDRESS = 0x7e1869
BATTLE_ITEM_ADDRESS = 0x7e2686
PLAYED_TIME_ADDRESS = 0x7e021b

retroarch_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
retroarch_socket.settimeout(POLL_INTERVAL / 5.0)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.settimeout(POLL_INTERVAL)

previous_inventory = None
previous_played_time = 0
previous_sync_request = 0
change_queue = []
message_index = 0


def log(msg):
    print(datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S'), msg)


def convert_dict_keys_to_int(mydict):
    if not isinstance(mydict, dict):
        return mydict

    temp = {}
    for key, value in mydict.items():
        try:
            key = int(key)
        except:
            pass
        temp[key] = value

    return temp


def server_send(msg):
    msg = msg.encode()
    temp = b'!' + gzip.compress(msg)
    if len(temp) < len(msg):
        msg = temp
    assert len(msg) < 4096
    server_socket.send(msg)


def server_receive():
    msg = server_socket.recv(4096)
    server_socket.settimeout(POLL_INTERVAL)
    if msg[0] == '!':
        msg = gzip.decompress(msg[1:])
    msg = msg.decode('ascii').strip()
    return msg


def get_retroarch_data(address, num_bytes):
    cmd = 'READ_CORE_RAM {0:0>6x} {1}'.format(address, num_bytes)
    retroarch_socket.sendto(cmd.encode(), ('localhost', RETROARCH_PORT))
    expected_length = 21 + (3 * num_bytes)
    data = retroarch_socket.recv(expected_length).decode('ascii').strip()
    data = [int(d, 0x10) for d in data.split(' ')[2:]]
    assert len(data) == num_bytes
    return data


def items_to_dict(items):
    order, inventory = [], {}

    for i in range(0x100):
        inventory[i] = 0

    for i, a in items:
        if i in order:
            order.append(0xff)
        else:
            order.append(i)

        if i == 0xff:
            inventory[i] = 0
        else:
            inventory[i] = max(inventory[i], a)

    return order, inventory


def get_field_items_raw():
    data = get_retroarch_data(FIELD_ITEM_ADDRESS, 512)
    return data


def get_field_items(data):
    items, amounts = data[:256], data[256:]
    assert len(items) == len(amounts) == 256
    return list(zip(items, amounts))


def get_battle_items_raw():
    data = get_retroarch_data(BATTLE_ITEM_ADDRESS, 1280)
    return data


def get_battle_items(data):
    items, amounts = data[::5], data[3::5]
    assert len(items) == len(amounts) == 256
    return list(zip(items, amounts))


def calculate_similarity(aa, bb):
    # assumption: no duplicates in either list, except 0xFF
    numer, denom = 0, 0
    for ((a_item, a_amount), (b_item, b_amount)) in zip(aa, bb):
        if a_item == b_item:
            numer += 1
            if a_amount == b_amount:
                numer += 1
        denom += 2
    assert denom == 512
    return numer / float(denom)


def sync_field_battle(battle_order, battle_inventory):
    values = list(battle_order)
    for v in list(values):
        if v == 0xff:
            values.append(0)
            continue
        values.append(battle_inventory[v])

    cmd = 'WRITE_CORE_RAM {0:0>6x}'.format(FIELD_ITEM_ADDRESS)
    for v in values:
        cmd = ' '.join([cmd, '{0:0>2X}'.format(v)])

    retroarch_socket.sendto(cmd.encode(), ('localhost', RETROARCH_PORT))


def write_inventory(order, to_inventory, raw_data, in_battle):
    inventory = dict(to_inventory)
    for item in range(0x100):
        if item not in inventory:
            inventory[item] = 0
            continue
        inventory[item] = min(max(inventory[item], 0), 99)
    inventory[0xFF] = 0

    for item in sorted(inventory):
        if item < 0xFF:
            if item in order and (
                    item not in inventory or inventory[item] == 0):
                assert order.count(item) == 1
                index = order.index(item)

    for item in sorted(inventory):
        if item < 0xFF:
            if inventory[item] > 0 and item not in order:
                index = order.index(0xFF)
                order[index] = item

    assert len(order) == 256
    unique = [item for item in order if item != 0xFF]
    assert len(unique) == len(set(unique))

    if in_battle:
        data = get_retroarch_data(BATTLE_ITEM_ADDRESS, 1280)
        data[::5] = order
        amounts = []
        for item in order:
            amounts.append(inventory[item] if item < 0xFF else 0)
        data[3::5] = amounts
        battle_cmd = ' '.join(['{0:0>2X}'.format(d) for d in data])
        battle_cmd = 'WRITE_CORE_RAM {0:0>6x} {1}'.format(
            BATTLE_ITEM_ADDRESS, battle_cmd)
        battle_cmd = battle_cmd.encode()

    field_cmd = ' '.join(['{0:0>2X}'.format(item) for item in order])
    for item in order:
        if item == 0xFF:
            field_cmd += ' 00'
        else:
            field_cmd += ' {0:0>2X}'.format(inventory[item])

    field_cmd = 'WRITE_CORE_RAM {0:0>6x} {1}'.format(
        FIELD_ITEM_ADDRESS, field_cmd)
    field_cmd = field_cmd.encode()

    # Here we perform multiple hacky checks to guarantee that memory has not
    # changed before we write to it, without interrupting the player
    # experience *too* much.
    if in_battle:
        new_raw = get_battle_items_raw()
    else:
        new_raw = get_field_items_raw()

    if new_raw != raw_data:
        return False

    pause_retroarch()

    try:
        sleep(PAUSE_DELAY_INTERVAL)
        if in_battle:
            new_raw = get_battle_items_raw()
        else:
            new_raw = get_field_items_raw()

        assert new_raw == raw_data

        if in_battle:
            retroarch_socket.sendto(battle_cmd, ('localhost', RETROARCH_PORT))
        retroarch_socket.sendto(field_cmd, ('localhost', RETROARCH_PORT))

        toggle_pause_retroarch()
        return True
    except:
        toggle_pause_retroarch()
        return False


def get_played_time():
    data = get_retroarch_data(PLAYED_TIME_ADDRESS, 4)
    hours, minutes, seconds, frames = data
    frames -= 1
    assert 0 <= frames <= 59
    frames = (frames + (seconds * 60) + (minutes * 60 * 60)
              + (hours * 60 * 60 * 60))

    return frames


def get_server_directive():
    response = server_receive()
    directive, parameters = response.split(' ', 1)
    parameters = json.loads(parameters)
    parameters = convert_dict_keys_to_int(parameters)
    #log('Received {0} from server.'.format(response))
    log('Received {0} from server.'.format(directive))
    return directive, parameters


def pause_retroarch():
    cmd = b'FRAMEADVANCE'
    retroarch_socket.sendto(cmd, ('localhost', RETROARCH_PORT))


def toggle_pause_retroarch():
    cmd = b'PAUSE_TOGGLE'
    retroarch_socket.sendto(cmd, ('localhost', RETROARCH_PORT))


def send_change_queue():
    temp = list(change_queue)
    while True:
        payload = json.dumps(temp)
        msg = 'LOG {0} {1}'.format(SERIES_NUMBER, payload)
        if len(msg) > 4095:
            temp = temp[:len(temp)/2]
        else:
            server_send(msg)
            break


def main_loop():
    global message_index, change_queue
    global previous_inventory, previous_played_time
    global backoff_sync_interval

    directive, directive_parameters = None, None
    try:
        directive, directive_parameters = get_server_directive()
    except ConnectionError:
        log('Unable to connect to server.')
    except socket.timeout:
        pass

    try:
        # read RAM data from retroarch
        played_time = get_played_time()
        field_raw = get_field_items_raw()
        battle_raw = get_battle_items_raw()
        field_items = get_field_items(field_raw)
        battle_items = get_battle_items(battle_raw)
    except socket.timeout:
        raise Exception('RetroArch not responding.')

    # determine whether the game is currently in combat
    similarity = calculate_similarity(field_items, battle_items)
    if similarity > SIMILARITY_THRESHOLD:
        in_battle = True
        current_order, current_inventory = items_to_dict(battle_items)
        raw_data = battle_raw
    else:
        in_battle = False
        current_order, current_inventory = items_to_dict(field_items)
        raw_data = field_raw

    # sync field to battle inventory to always stay above threshold
    if in_battle and similarity < 1.0:
        sync_field_battle(current_order, current_inventory)

    # update change queue
    if (previous_inventory is not None
            and played_time > previous_played_time
            and current_inventory != previous_inventory):
        for item in sorted(set(previous_inventory.keys())
                           | set(current_inventory.keys())):
            if previous_inventory[item] != current_inventory[item]:
                message_index += 1
                change_queue.append((
                    message_index, item,
                    current_inventory[item]-previous_inventory[item]))

    previous_inventory = current_inventory

    # ignore all inventory changes after game load until sync with server
    if previous_played_time <= played_time:
        previous_played_time = played_time
    else:
        previous_played_time = 999999999

    synced_inventory = None
    if directive is not None:
        backoff_sync_interval = SYNC_INTERVAL
        if directive == 'SYNC':
            synced_inventory = directive_parameters
            if previous_played_time > played_time:
                previous_played_time = played_time
            for item in range(0x100):
                if item not in synced_inventory:
                    synced_inventory[item] = 0
            for (index, item, change) in change_queue:
                synced_inventory[item] += change
        if directive == 'REPORT':
            temp_inventory = {}
            for item, amount in current_inventory.items():
                if amount >= 1:
                    temp_inventory[item] = amount
            payload = json.dumps(temp_inventory)
            msg = 'REPORT {0} {1}'.format(SERIES_NUMBER, payload)
            server_send(msg)
        if directive == 'LOG':
            indexes = directive_parameters
            change_queue = [(index, item, change)
                            for (index, item, change) in change_queue
                            if index not in indexes]

    if change_queue:
        try:
            send_change_queue()
        except ConnectionError:
            log('Unable to connect to server.')

    if synced_inventory is not None:
        try:
            if write_inventory(current_order, synced_inventory,
                               raw_data, in_battle=in_battle):
                previous_inventory = synced_inventory
        except socket.timeout:
            pass


def create_new_session(name):
    server_send('NEW {0} {1}'.format(name, SERIES_NUMBER))
    server_socket.settimeout(30)
    msg = server_receive()
    if msg.startswith('ERROR'):
        raise Exception(msg)


def join_session(name):
    server_send('JOIN {0} {1}'.format(name, SERIES_NUMBER))
    server_socket.settimeout(30)
    msg = server_receive()
    if msg.startswith('ERROR'):
        raise Exception(msg)


def send_sync_request():
    global backoff_sync_interval
    backoff_sync_interval *= 1.5
    backoff_sync_interval = min(backoff_sync_interval, SYNC_INTERVAL * 10)
    server_send('SYNC {0}'.format(SERIES_NUMBER))


if __name__ == '__main__':
    try:
        host = socket.gethostbyname(input('Host address? '))

        port = input('Port? ')
        port = int(port)

        server_socket.connect((host, port))

        OPTION_JOIN_SESSION, OPTION_NEW_SESSION = 1, 2
        option = input('\nChoose one: \n'
                       '{0}. Join an existing session.\n'
                       '{1}. Start a new session.\n\n'
                       '? '.format(OPTION_JOIN_SESSION, OPTION_NEW_SESSION))
        option = int(option)
        assert option in [OPTION_JOIN_SESSION, OPTION_NEW_SESSION]

        session_name = input('Session name? ').strip()

        if option == OPTION_JOIN_SESSION:
            join_session(session_name)
        elif option == OPTION_NEW_SESSION:
            create_new_session(session_name)

        previous_network_time = 0
        while True:
            now = time()
            diff = now - previous_network_time
            if diff < POLL_INTERVAL:
                sleep(POLL_INTERVAL - diff)
                now = time()
            previous_network_time = now

            if now - previous_sync_request > backoff_sync_interval:
                send_sync_request()
                previous_sync_request = now

            main_loop()

    except Exception:
        from sys import exc_info
        print('Error:', exc_info()[0], exc_info()[1])
        input('')
