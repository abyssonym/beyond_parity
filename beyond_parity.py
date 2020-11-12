import gzip
import json
import random
import socket
import traceback
from configparser import ConfigParser
from datetime import datetime, timezone
from sys import argv, exc_info
from time import time, sleep

try:
    config = ConfigParser()
    if len(argv) > 1:
        config.read(argv[1])
    else:
        config.read('beyond_parity.cfg')

    if config.has_option('Settings', 'DEBUG'):
        DEBUG = config.get('Settings', 'DEBUG').lower() == 'yes'
    else:
        DEBUG = False

    SYNC_INVENTORY = config.get('Settings', 'SYNC_INVENTORY').lower() != 'no'
    SYNC_CHESTS = config.get('Settings', 'SYNC_CHESTS').lower() != 'no'
    SYNC_STATUS = config.get('Settings', 'SYNC_STATUS').lower() != 'no'
    SYNC_GP = config.get('Settings', 'SYNC_GP').lower() != 'no'

    RETROARCH_PORT = int(config.get('Settings', 'RETROARCH_PORT'))
    POLL_INTERVAL = float(config.get('Settings', 'POLL_INTERVAL'))
    SYNC_INTERVAL = float(config.get('Settings', 'SYNC_INTERVAL'))
    backoff_sync_interval = SYNC_INTERVAL
    PAUSE_DELAY_INTERVAL = float(
        config.get('Settings', 'PAUSE_DELAY_INTERVAL'))
    SIMILARITY_THRESHOLD = float(
        config.get('Settings', 'SIMILARITY_THRESHOLD'))
    SERIES_NUMBER = int(round(time()))
    MINIMUM_PLAYED_TIME = int(config.get('Settings', 'MINIMUM_PLAYED_TIME'))
    MIN_SANE_INVENTORY = int(config.get('Settings', 'MIN_SANE_INVENTORY'))

    FIELD_ITEM_ADDRESS = int(
        config.get('Settings', 'FIELD_ITEM_ADDRESS'), 0x10)
    BATTLE_ITEM_ADDRESS = int(
        config.get('Settings', 'BATTLE_ITEM_ADDRESS'), 0x10)
    PLAYED_TIME_ADDRESS = int(
        config.get('Settings', 'PLAYED_TIME_ADDRESS'), 0x10)
    BATTLE_CHAR_ADDRESS = int(
        config.get('Settings', 'BATTLE_CHAR_ADDRESS'), 0x10)
    STATUS_1_ADDRESS = int(config.get('Settings', 'STATUS_1_ADDRESS'), 0x10)
    STATUS_2_ADDRESS = int(config.get('Settings', 'STATUS_2_ADDRESS'), 0x10)
    CHEST_ADDRESS = int(config.get('Settings', 'CHEST_ADDRESS'), 0x10)
    GP_ADDRESS = int(config.get('Settings', 'GP_ADDRESS'), 0x10)
    BUTTON_MAP_ADDRESS = int(
        config.get('Settings', 'BUTTON_MAP_ADDRESS'), 0x10)

    if config.has_option('Settings', 'TEST_LATENCY'):
        TEST_LATENCY = config.get('Settings', 'TEST_LATENCY').lower() == 'yes'
    else:
        TEST_LATENCY = False
except:
    input("Configuration file error. ")
    exit(0)

retroarch_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
retroarch_socket.connect(('localhost', RETROARCH_PORT))
retroarch_socket.settimeout(POLL_INTERVAL / 5.0)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.settimeout(POLL_INTERVAL)

previous_inventory = None
previous_played_time = 999999999
previous_status = None
previous_chests = None
previous_gp = None

previous_sync_request = 0
change_queue = []
message_index = 0
previous_log = None
previous_log_time = 0
previous_log_count = 0


def log(msg, is_debug=False):
    global previous_log, previous_log_count, previous_log_time
    if is_debug and not DEBUG:
        return

    now = time()
    time_diff = now - previous_log_time
    if msg == previous_log:
        previous_log_count += 1
        if previous_log_count >= 3 and time_diff < 60:
            return
    else:
        previous_log = msg
        previous_log_count = 0

    if is_debug:
        msg = 'DEBUG {0}'.format(msg)

    try:
        print(datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S'), msg)
    except ValueError:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), msg)

    previous_log_time = now


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
    if TEST_LATENCY:
        sleep(random.random() * 6)
    server_socket.send(msg)


def server_receive():
    msg = server_socket.recv(4096)
    server_socket.settimeout(POLL_INTERVAL)
    if msg[0] == ord('!'):
        msg = gzip.decompress(msg[1:])
    msg = msg.decode('ascii').strip()
    if TEST_LATENCY:
        sleep(random.random() * 6)
    return msg


def write_retroarch_data(address, data):
    MAX_WRITE_LENGTH = 4
    while data:
        s = ' '.join(['{0:0>2X}'.format(d) for d in data[:MAX_WRITE_LENGTH]])
        cmd = 'WRITE_CORE_RAM {0:0>6x} {1}'.format(address, s)
        retroarch_socket.send(cmd.encode())
        data = data[MAX_WRITE_LENGTH:]
        address += MAX_WRITE_LENGTH


def get_retroarch_data(address, num_bytes):
    cmd = 'READ_CORE_RAM {0:0>6x} {1}'.format(address, num_bytes)
    retroarch_socket.send(cmd.encode())
    expected_length = 21 + (3 * num_bytes)
    try:
        data = retroarch_socket.recv(expected_length).decode('ascii').strip()
    except socket.timeout:
        raise IOError('RetroArch not responding.')
    data = [int(d, 0x10) for d in data.split(' ')[2:]]
    if len(data) != num_bytes:
        raise IOError('RetroArch RAM data read error.')
    return data


def fix_button_mapping():
    DEFAULT_BUTTON_MAP = [0x12, 0x34, 0x56, 0x06]
    write_retroarch_data(BUTTON_MAP_ADDRESS, DEFAULT_BUTTON_MAP)


def test_write_retroarch():
    DEFAULT_BUTTON_MAP = [0x12, 0x34, 0x56, 0x06]
    REVISED_BUTTON_MAP = [0x12, 0x34, 0x56, 0xF6]
    data = get_retroarch_data(BUTTON_MAP_ADDRESS, 4)
    if data == DEFAULT_BUTTON_MAP and data != REVISED_BUTTON_MAP:
        log('RetroArch read SUCCESS')
        pause_retroarch()
        write_retroarch_data(BUTTON_MAP_ADDRESS, REVISED_BUTTON_MAP)
        sleep(0.05)
        toggle_pause_retroarch()
        data = get_retroarch_data(BUTTON_MAP_ADDRESS, 4)
        if data == REVISED_BUTTON_MAP and data != DEFAULT_BUTTON_MAP:
            log('RetroArch write SUCCESS')
            fix_button_mapping()
        else:
            log('RetroArch write FAILURE')
    else:
        log('RetroArch read FAILURE')


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

    write_retroarch_data(FIELD_ITEM_ADDRESS, values)


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
                order[index] = 0xFF

    for item in sorted(inventory):
        if item < 0xFF:
            if inventory[item] > 0 and item not in order:
                index = order.index(0xFF)
                order[index] = item

    assert len(order) == 256
    unique = [item for item in order if item != 0xFF]
    assert len(unique) == len(set(unique))

    if in_battle:
        battle_data = get_retroarch_data(BATTLE_ITEM_ADDRESS, 1280)
        battle_data[::5] = order
        amounts = []
        for item in order:
            amounts.append(inventory[item] if item < 0xFF else 0)
        battle_data[3::5] = amounts

    field_data = order + [inventory[item] for item in order]

    # Here we perform multiple hacky checks to guarantee that memory has not
    # changed before we write to it, without interrupting the player
    # experience *too* much.
    if in_battle:
        new_raw = get_battle_items_raw()
    else:
        new_raw = get_field_items_raw()

    if new_raw != raw_data:
        log('Did not write inventory because of race condition (1).',
            is_debug=True)
        return False

    pause_retroarch()

    try:
        sleep(PAUSE_DELAY_INTERVAL)
        if in_battle:
            new_raw = get_battle_items_raw()
        else:
            new_raw = get_field_items_raw()

        assert new_raw == raw_data

        success = False
        if SYNC_INVENTORY:
            if in_battle:
                write_retroarch_data(BATTLE_ITEM_ADDRESS, battle_data)
                log('Wrote battle inventory.', is_debug=True)
            write_retroarch_data(FIELD_ITEM_ADDRESS, field_data)
            log('Wrote field inventory.', is_debug=True)
            success = True
            if DEBUG:
                verify_raw = get_field_items_raw()
                verify_items = get_field_items(verify_raw)
                _, verify_inventory = items_to_dict(verify_items)
                if verify_inventory == inventory:
                    log('The write was successful.', is_debug=True)
                    success = True
                else:
                    log('ALERT: The write has failed!', is_debug=True)
                    error_dict = {
                        k: (inventory[k], verify_inventory[k])
                        for k in range(0x100)
                        if k in inventory and k in verify_inventory
                        and inventory[k] != verify_inventory[k]
                        }
                    log(error_dict)
                    success = False
        else:
            log('Did not write inventory because of configuration.',
                is_debug=True)
            success = False

        toggle_pause_retroarch()
        return success
    except:
        log('Did not write inventory because of race condition (2).',
            is_debug=True)
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


def get_battle_characters():
    data = get_retroarch_data(BATTLE_CHAR_ADDRESS, 8)
    characters = []
    for i in range(4):
        a, b = data[i*2:(i+1)*2]
        if a == b == 0xFF:
            characters.append(False)
        else:
            characters.append(True)
    return characters


def get_status_data():
    status1 = get_retroarch_data(STATUS_1_ADDRESS, 8)
    status2 = get_retroarch_data(STATUS_2_ADDRESS, 8)
    char_statuses = {}
    for i in range(4):
        a = status1[i*2] | status1[(i*2)+1]
        b = status2[i*2] | status2[(i*2)+1]
        char_statuses[i] = a | (b << 16)
    return char_statuses


def write_status(char_statuses):
    status1, status2 = [], []
    for i, char_status in sorted(char_statuses.items()):
        if char_status is None:
            status1 += [0, 0]
            status2 += [0, 0]
            continue
        a = char_status & 0xFFFF
        b = char_status >> 16
        status1 += [a & 0xFF, a >> 8]
        status2 += [b & 0xFF, b >> 8]

    write_retroarch_data(STATUS_1_ADDRESS, status1)
    write_retroarch_data(STATUS_2_ADDRESS, status2)


def get_chest_data():
    data = get_retroarch_data(CHEST_ADDRESS, 0x40)
    return data


def write_chests(old_chests, new_chests):
    to_write = []
    assert len(old_chests) == len(new_chests)
    for (a, b) in zip(old_chests, new_chests):
        assert 0 <= a <= 0xFF
        assert 0 <= b <= 0xFF
        to_write.append(a | b)
    assert len(to_write) == 0x40

    if SYNC_CHESTS:
        write_retroarch_data(CHEST_ADDRESS, to_write)


def get_gp():
    data = get_retroarch_data(GP_ADDRESS, 3)
    return (data[2] << 16) | (data[1] << 8) | data[0]


def get_server_directive():
    response = server_receive()
    try:
        directive, parameters = response.split(' ', 1)
        parameters = json.loads(parameters)
        parameters = convert_dict_keys_to_int(parameters)
    except:
        log('Bad directive: {0}'.format(response))
        raise Exception(response)

    if DEBUG:
        log('Received {0} from server.'.format(response), is_debug=True)
    else:
        log('Received {0} from server.'.format(directive))
    return directive, parameters


def pause_retroarch():
    if PAUSE_DELAY_INTERVAL <= 0:
        return
    cmd = b'FRAMEADVANCE'
    retroarch_socket.send(cmd)


def toggle_pause_retroarch():
    if PAUSE_DELAY_INTERVAL <= 0:
        return
    cmd = b'PAUSE_TOGGLE'
    retroarch_socket.send(cmd)


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


def send_chests(chests):
    msg = 'CHESTS {0} {1}'.format(SERIES_NUMBER, json.dumps(chests))
    server_send(msg)


def check_inventory_size(inventory):
    count = 0
    for (item, amount) in inventory.items():
        if item == 0xFF:
            continue
        if 1 <= amount <= 99:
            count += 1
    return count


def main_loop():
    global message_index, change_queue
    global previous_inventory, previous_played_time
    global previous_status, previous_chests, previous_gp
    global backoff_sync_interval, previous_sync_request, force_sync

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
        if played_time < MINIMUM_PLAYED_TIME:
            previous_played_time = 999999999
        field_raw = get_field_items_raw()
        battle_raw = get_battle_items_raw()

        battle_characters = get_battle_characters()
        current_status = get_status_data()

        current_chests = get_chest_data()
    except (IOError, AssertionError):
        log('{0}: {1}'.format(*exc_info()[:2]))
        force_sync = True
        return

    now = time()
    if now - previous_sync_request > backoff_sync_interval:
        send_sync_request()
        previous_sync_request = now

    chests_opened = False
    if previous_chests is None:
        previous_chests = current_chests
    elif previous_chests != current_chests:
        chests_opened = True

    current_gp = get_gp()
    if previous_gp is None:
        previous_gp = current_gp

    field_items = get_field_items(field_raw)
    battle_items = get_battle_items(battle_raw)

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

    # if in combat, determine changed statuses
    if in_battle:
        status_on, status_off = {}, {}
        for (i, c) in enumerate(battle_characters):
            assert i in current_status
            if not c:
                current_status[i] = None
                continue
            if (previous_status and previous_status[i] is not None
                    and current_status[i] is not None):
                differences = current_status[i] ^ previous_status[i]
                on_flags = current_status[i] & differences
                off_flags = (current_status[i] ^ 0xFFFFFFFF) & differences
                status_on[i] = on_flags
                status_off[i] = off_flags
        previous_status = current_status
    else:
        status_on, status_off = None, None
        battle_characters = None
        current_status = None

    # sanity check to prevent inventory wipe on re-load
    if (previous_inventory is not None and current_inventory is not None
            and check_inventory_size(previous_inventory) >= MIN_SANE_INVENTORY
            and check_inventory_size(current_inventory) <= 0):
        previous_played_time = 999999999

    # sync field to battle inventory to always stay above threshold
    if in_battle and similarity < 1.0:
        sync_field_battle(current_order, current_inventory)

    # update change queue
    if SYNC_INVENTORY:
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

    # update change queue (statuses)
    if status_on is not None and status_off is not None:
        for i in range(4):
            if i in status_on and status_on[i] > 0:
                change_queue.append((
                    'STATUS_ON', i, '{0:X}'.format(status_on[i])))
            if i in status_off and status_off[i] > 0:
                change_queue.append((
                    'STATUS_OFF', i, '{0:X}'.format(status_off[i])))

    previous_inventory = current_inventory

    # ignore all inventory changes after game load until sync with server
    if previous_played_time <= played_time:
        previous_played_time = played_time
    else:
        previous_played_time = 999999999

    synced_inventory = None
    synced_status = {}
    update_status_flag = False
    if directive is not None:
        backoff_sync_interval = SYNC_INTERVAL
        if directive == 'SYNC':
            synced_inventory = directive_parameters
            for item in range(0x100):
                if item not in synced_inventory:
                    synced_inventory[item] = 0
            for (index, item, change) in change_queue:
                if isinstance(index, int):
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
        if directive == 'CHESTS':
            synced_chests = directive_parameters
            write_chests(current_chests, synced_chests)

        if in_battle and directive in ['STATUS_ON', 'STATUS_OFF']:
            character, change = directive_parameters
            change = int(change, 0x10)
            for i in range(4):
                synced_status[i] = current_status[i]
                if current_status[i] is not None:
                    value = current_status[i]
                    if i == character:
                        if directive == 'STATUS_ON':
                            value |= change
                        elif directive == 'STATUS_OFF':
                            value &= (0xFFFFFFFF ^ change)

                    if value != current_status[i]:
                        update_status_flag = True
                        synced_status[i] = value

    if change_queue:
        try:
            send_change_queue()
        except ConnectionError:
            log('Unable to connect to server.')
        change_queue = [(index, item, change)
                        for (index, item, change) in change_queue
                        if isinstance(index, int)]

    if SYNC_CHESTS and chests_opened:
        try:
            send_chests(current_chests)
            previous_chests = current_chests
        except ConnectionError:
            log('Unable to connect to server.')

    if SYNC_INVENTORY and synced_inventory is not None:
        simplified_inventory = {k:v for (k, v) in synced_inventory.items()
                                if v > 0}
        simplified_current = {k:v for (k, v) in current_inventory.items()
                              if v > 0}
        log('Inventory write attempt: {0}'.format(simplified_inventory),
            is_debug=True)
        if simplified_inventory == simplified_current:
            log('The new inventory is THE SAME as the old inventory.',
                is_debug=True)
            previous_inventory = current_inventory
            if previous_played_time > played_time:
                previous_played_time = played_time

        else:
            log('The new inventory is DIFFERENT from the old inventory.',
                is_debug=True)
            try:
                if write_inventory(current_order, synced_inventory,
                                   raw_data, in_battle=in_battle):
                    previous_inventory = synced_inventory
                    if previous_played_time > played_time:
                        previous_played_time = played_time
                else:
                    force_sync = True
            except socket.timeout:
                pass

    if update_status_flag:
        write_status(synced_status)


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
    global backoff_sync_interval, force_sync
    backoff_sync_interval *= 1.5
    backoff_sync_interval = min(backoff_sync_interval, SYNC_INTERVAL * 10)
    if previous_played_time >= 999999999 or force_sync:
        server_send('SYNC {0} !'.format(SERIES_NUMBER))
        force_sync = False
    else:
        server_send('SYNC {0}'.format(SERIES_NUMBER))


if __name__ == '__main__':
    try:
        test_write_retroarch()
        fix_button_mapping()

        for s in ['SYNC_INVENTORY', 'SYNC_CHESTS', 'SYNC_STATUS', 'SYNC_GP']:
            log('{0}: {1}'.format(s, globals()[s]))

        if DEBUG:
            log('Debug mode enabled.', is_debug=True)
        else:
            log('Debug mode not enabled.')

        host, port, session_name = None, None, None
        if config.has_option('Settings', 'SERVER_HOSTNAME'):
            host = config.get('Settings', 'SERVER_HOSTNAME').strip()
        if not host:
            host = input('Host address? ')
        host = socket.gethostbyname(host)

        if config.has_option('Settings', 'SERVER_PORT'):
            port = config.get('Settings', 'SERVER_PORT').strip()
        if not port:
            port = input('Port? ')
        port = int(port)

        server_socket.connect((host, port))

        OPTION_JOIN_SESSION, OPTION_NEW_SESSION = 1, 2
        if config.has_option('Settings', 'JOIN_SESSION_NAME'):
            session_name = config.get('Settings', 'JOIN_SESSION_NAME').strip()
            if session_name:
                option = OPTION_JOIN_SESSION

        while not session_name:
            option = input('\nChoose one: \n'
                           '{0}. Join an existing session.\n'
                           '{1}. Start a new session.\n\n'
                           '? '.format(OPTION_JOIN_SESSION,
                                       OPTION_NEW_SESSION))
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

            main_loop()

    except:
        traceback.print_exc()
        print('Error:', exc_info()[0], exc_info()[1])
        input('')
