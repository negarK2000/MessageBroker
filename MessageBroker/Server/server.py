import socket
import threading

HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 1373 # Port to listen on (non-privileged ports are > 1023)
CLIENT_PORT = 1500

MAX_MESSAGE_LENGTH = 64
BACKLOG = 5
ENCODING = 'utf-8'

instruction = ['publish', 'subscribe', 'PING', 'PONG']
published_messages = {} # {topic1 : [message1, ...], ...}
connected_clients = {} # {address: (ConnectedSocket, ClientThread), ...}
# published_messages['foo'] = ['FOO MESSAGE?']
# published_messages['hello'] = ['HELLO WORLD!']

PING = False
client_off = {}


def main():
    address = socket.gethostbyname(socket.gethostname())

    HOST_INFO = (address, PORT)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind(HOST_INFO)

    print("[SERVER STARTS] Server is Starting ...")

    start(s, HOST_INFO)


def start(server, info):
    server.listen(BACKLOG)

    while True:
        connection, address = server.accept()

        if PING:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:

                s.connect((info[0], CLIENT_PORT))
            except ConnectionRefusedError:
                print('[CONNECTION WAS REFUSED] Client is not Listening')
                return

            s.settimeout(10)

            timer = threading.Timer(10, ping, args=[s, address])
            client_off[address] = 0
            timer.start()

        t = threading.Thread(target=client_handler, args=(connection, address))
        t.start()

        connected_clients[address] = connection


# Client Health Check
def ping(s, client_addr):
    timer = threading.Timer(10, ping, args=[s, client_addr])

    if client_addr in connected_clients.keys() and client_off[client_addr] < 3:
        send_message(s, instruction[2])

        timer.start()

        try:
            msg = receive_message(s)

            if instruction[3] not in msg:
                raise socket.timeout

        except socket.timeout:
            try:
                client_off[client_addr] += 1
                print('[PING: CLIENT IS OFFLINE] the {}th time : Client = {}'.format(client_off[client_addr], client_addr))

                if client_off[client_addr] >= 3:
                    print('[PING: CLIENT IS OFFLINE] connection will be disconnected : Client = {}'.format(client_addr))

                    timer.cancel()
                    send_message(s, "Disconnect")
                    s.close()
                    client_off.pop(client_addr)

                    conn = connected_clients.pop(client_addr)
                    send_message(conn, "Disconnect")
                    conn.close()
                    print("[CONNECTION ENDED]")

            except KeyError:
                print('Client closed the Connection')

    else:
        timer.cancel()


def client_handler(connection, address):
    print("[NEW CONNECTION] connected from {}".format(address))

    CONNECTED = True
    try:
        while CONNECTED:

            message = receive_message(connection)

            if message == 'Disconnect':
                CONNECTED = False
                continue

            msg = message.split('$')

            if msg[0] == instruction[0] or msg[0] == instruction[1]:
                connection.send('1'.encode(ENCODING))  # Acknowledgement message

            if msg[0] == instruction[0]:
                publish_msg(msg[1], msg[2].strip())

            elif msg[0] == instruction[1]:
                subscribe_msg(connection, msg[1])

            elif instruction[2] in message:
                print(message)
                send_message(connection, instruction[3])

    except ConnectionResetError:
        pass

    connection.close()
    connected_clients.pop(address)
    print("[CONNECTION ENDED] ended connection {}".format(address))


def publish_msg(topic, msg):
    print('[NEW PUBLISHED MESSAGE] {} : {}'.format(topic, msg))

    if topic in published_messages.keys():
        published_messages[topic].append(msg)

    else:
        published_messages[topic] = []
        published_messages[topic].append(msg)


def subscribe_msg(conn, topic):

    if topic in published_messages.keys():
        requested_msg = published_messages[topic]

        l = len(requested_msg)

        if not send_message(conn, str(l)):
            return

        for i in range(l):
            msg = topic + '$' + requested_msg[i]
            send_message(conn, msg)

    else:
        requested_msg = '< There wasnt any published message for ' + topic + ' topic >'
        if not send_message(conn, '1'):
            return

        msg = topic + '$' + requested_msg
        send_message(conn, msg)


def send_message(conn, msg):
    try:
        message = msg.encode(ENCODING)

        msg_len = str(len(message)).encode(ENCODING)
        msg_len += b' ' * (MAX_MESSAGE_LENGTH - len(msg_len))

        conn.send(msg_len)
        conn.send(message)

    except OSError or ConnectionAbortedError:
        return False

    return True


def receive_message(conn):
    msg = ''
    try:
        msg_len = int(conn.recv(MAX_MESSAGE_LENGTH).decode(ENCODING))

        if msg_len <= MAX_MESSAGE_LENGTH:
            msg = conn.recv(msg_len).decode(ENCODING)
        else:
            while msg_len > 0:
                msg += conn.recv(MAX_MESSAGE_LENGTH).decode(ENCODING)
                msg_len -= MAX_MESSAGE_LENGTH

    except OSError or ConnectionAbortedError:
        return ''

    return msg

if __name__ == '__main__':
    main()
