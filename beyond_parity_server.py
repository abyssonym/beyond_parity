import gzip
import json
import socket
from sys import exc_info
from time import time, sleep

POLL_INTERVAL = 0.5
SERVER_IP = '10.0.0.111'
SERVER_PORT = 55333
LOG_RETENTION_DURATION = 600
BACKUP_INTERVAL = 900

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.settimeout(POLL_INTERVAL)


members = {}
item_ledger = {}
processed_logs = {}


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


def client_send(msg, client):
    msg = msg.encode()
    temp = b'!' + gzip.compress(msg)
    print(len(msg), len(temp))
    if len(temp) < len(msg):
        msg = temp
    assert len(msg) < 4096
    server_socket.sendto(msg, client)


def client_receive():
    msg, client = server_socket.recvfrom(4096)
    if msg[0] == '!':
        msg = gzip.decompress(msg[1:])
    msg = msg.decode('ascii').strip()
    return msg, client


def main_loop():
    timestamp = int(round(time()))
    msg, sender, sender_address, sender_port = None, None, None, None
    try:
        msg, sender = client_receive()
        sender_address, sender_port = sender
        print(msg, sender)

        if msg.startswith('NEW '):
            _, session_name, series_number = msg.split(' ')
            if session_name in item_ledger:
                reply = 'ERROR: Session "{0}" already exists.'.format(
                    session_name)
                client_send(reply, sender)
            else:
                member_name = '{0}-{1}'.format(sender_address, series_number)
                members[member_name] = session_name
                item_ledger[session_name] = None

                reply = 'Success'.format(
                    session_name)
                client_send(reply, sender)
                client_send('REPORT {}', sender)

        elif msg.startswith('JOIN '):
            _, session_name, series_number = msg.split(' ')
            if session_name not in item_ledger:
                reply = 'ERROR: Session "{0}" does not exist.'.format(
                    session_name)
                client_send(reply, sender)
            else:
                member_name = '{0}-{1}'.format(sender_address, series_number)
                members[member_name] = session_name

                reply = 'Success'.format(
                    session_name)
                client_send(reply, sender)

        elif msg.startswith('REPORT '):
            _, series_number, payload = msg.split(' ', 2)
            member_name = '{0}-{1}'.format(sender_address, series_number)
            session_name = members[member_name]

            if (session_name in item_ledger
                    and item_ledger[session_name] is None):
                item_ledger[session_name] = {}
                current_inventory = convert_dict_keys_to_int(
                    json.loads(payload))
                for i in range(0x100):
                    if i in current_inventory:
                        item_ledger[session_name][i] = current_inventory[i]
                    else:
                        item_ledger[session_name][i] = 0

        elif msg.startswith('LOG '):
            _, series_number, payload = msg.split(' ', 2)
            member_name = '{0}-{1}'.format(sender_address, series_number)
            session_name = members[member_name]

            change_queue = json.loads(payload)
            done_indexes = []
            for (index, item, change) in change_queue:
                done_indexes.append(index)
                log_identifier = '{0}-{1}'.format(member_name, index)
                if log_identifier in processed_logs:
                    continue

                processed_logs[log_identifier] = timestamp
                item_ledger[session_name][item] += change

            reply = 'LOG {0}'.format(json.dumps(done_indexes))
            client_send(reply, sender)

        elif msg.startswith('SYNC '):
            _, series_number = msg.split(' ', 1)
            member_name = '{0}-{1}'.format(sender_address, series_number)
            session_name = members[member_name]
            if item_ledger[session_name] is None:
                reply = 'REPORT {}'
            else:
                session_inventory = dict(item_ledger[session_name])
                for key in list(session_inventory.keys()):
                    if session_inventory[key] <= 0:
                        del(session_inventory[key])
                reply = 'SYNC {0}'.format(json.dumps(session_inventory))
            client_send(reply, sender)

    except socket.timeout:
        for (key, oldtime) in list(processed_logs.items()):
            if timestamp - oldtime > LOG_RETENTION_DURATION:
                del(processed_logs[key])

    except:
        error_msg = 'ERROR: {0} {1}'.format(exc_info()[0], exc_info()[1])
        print(error_msg)
        client_send(error_msg, sender)

if __name__ == '__main__':
    previous_network_time = 0
    while True:
        now = time()
        diff = now - previous_network_time
        if diff < POLL_INTERVAL:
            sleep(POLL_INTERVAL - diff)
            previous_network_time = time()
        else:
            previous_network_time = now

        if not int(round(now)) % BACKUP_INTERVAL:
            # TODO backup data
            pass

        main_loop()
