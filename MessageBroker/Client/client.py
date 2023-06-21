import socket
import argparse
import threading

PORT = 1373 # Port to listen on (non-privileged ports are > 1023)
CLIENT_PORT = 1500

MAX_MESSAGE_LENGTH = 64
ENCODING = 'utf-8'

instruction = ['publish', 'subscribe', 'PING', 'PONG']

server_off = 0
connections = []

PONG = False


def main():
    parser = argparse.ArgumentParser(description='Conversing with Message Broker')

    parser.add_argument('--address', '-a', nargs=2, help='setting host and port address')
    parser.add_argument('--publish', '-p', nargs='+', help = 'publishing a message from a topic')
    parser.add_argument('--subscribe', '-s', nargs='+', help = 'subscribing topics in order to receive related messages')
    parser.add_argument('--ping', '-i', action="store_const", const=True, help = 'sending a ping message to the server and waiting for a pong answer')

    arguments = parser.parse_args()

    address = socket.gethostbyname(socket.gethostname())

    SERVER_INFO = (address, PORT)
    if arguments.address != None:
        SERVER_INFO = (arguments.address[0], int(arguments.address[1]))

    if PONG:
        t = threading.Thread(target=pong, args=[address])
        t.start()

    if arguments.subscribe != None:
        subscribe(SERVER_INFO, arguments.subscribe)

    if arguments.publish != None:
        publish(SERVER_INFO, arguments.publish)

    if arguments.ping:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(SERVER_INFO)
        except ConnectionRefusedError:
            print('[CONNECTION WAS REFUSED] Server is not Listening')
            return

        s.settimeout(10)

        timer = threading.Timer(1, ping, args=[s, SERVER_INFO])
        timer.start()


def pong(address):
    HOST_INFO = (address, CLIENT_PORT)

    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2.bind(HOST_INFO)

    print("[CLIENT STARTS] Client is listening ...")
    s2.listen(1)

    conn, addr = s2.accept()
    conn.settimeout(1)

    while len(connections) != 0:
        try:
            message = receive_message(conn)

            if instruction[2] in message:
                send_message(conn, instruction[3])

        except socket.timeout or ConnectionResetError:
            conn.close()

    s2.close()


# Server Health Check
def ping(s, SERVER_INFO):
    global server_off
    timer = threading.Timer(10, ping, args=[s, SERVER_INFO])

    if len(connections) != 0 and server_off < 3:
        try:
            send_message(s, instruction[2])
            timer.start()

            try:
                msg = receive_message(s)

                if instruction[3] not in msg:
                    raise socket.timeout

            except socket.timeout:
                server_off += 1
                print('[PING: SERVER IS OFFLINE] the {}th time : Server = {}'.format(server_off, SERVER_INFO))

                if server_off >= 3:
                    print('[PING: SERVER IS OFFLINE] connection will be disconnected : Server = {}'.format(SERVER_INFO))

                    timer.cancel()
                    send_message(s, "Disconnect")
                    s.close()

                    for conn in connections:
                        send_message(conn, "Disconnect")
                        conn.close()

                    print("[CONNECTION ENDED]")

        except ConnectionResetError:
            print('< Server is disconnected >')
            timer.cancel()
            print("[CONNECTION ENDED]")

    else:
        timer.cancel()
        send_message(s, "Disconnect")
        s.close()


# different parts of message are separated by '$'
def publish(server, args):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(server)
    except ConnectionRefusedError:
        print('[CONNECTION WAS REFUSED] Server is offline')
        return

    print("[CONNECTED TO SERVER] server: {}".format(server))

    connections.append(s)

    message = instruction[0] + '$' + args[0] + '$'
    l = len(args)

    for i in range(1, l):
        message += args[i] + ' '

    if not send_message(s, message):
        return

    recv_ack_msg(s)

    send_message(s, "Disconnect")
    connections.remove(s)
    s.close()
    print("[CONNECTION ENDED]")


# different parts of message are separated by '$'
def subscribe(server, topics):

    l = len(topics)
    for i in range(l):
        message = instruction[1] + '$' + topics[i]

        t = threading.Thread(target=subscribe_handler, args=(server, topics[i], message))
        t.start()


def subscribe_handler(server, topic, msg):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(server)
    except ConnectionRefusedError:
        print('[CONNECTION WAS REFUSED] Server is offline')
        return

    print("[CONNECTED TO SERVER] server: {}".format(server))
    connections.append(s)

    if not send_message(s, msg):
        return

    if not recv_ack_msg(s):
        print('The request for <{}> topic has failed.'.format(topic))
        return

    num = receive_message(s)
    if num != '':
        num = int(num)

        for i in range(num):
            ans = receive_message(s).split('$')
            print('     <topic> {} : <message> {}'.format(ans[0], ans[1]))

        send_message(s, "Disconnect")
        s.close()
        connections.remove(s)
        print("[CONNECTION ENDED]")


def recv_ack_msg(conn, timer = 10, m = '1'):
    default_timeout = conn.settimeout(timer)
    msg = 0

    try:
        msg = int(conn.recv(len(m)).decode(ENCODING))
        print('[SERVER ACKNOWLEDGEMENT] Your message was successfully received.')

    except socket.timeout:
        print('[SERVER TIMEOUT] Your message failed!')

    conn.settimeout(default_timeout)
    return msg


def send_message(conn, msg):
    try:
        message = msg.encode(ENCODING)

        msg_len = str(len(message)).encode(ENCODING)
        msg_len += b' ' * (MAX_MESSAGE_LENGTH - len(msg_len))

        conn.send(msg_len)
        conn.send(message)

    except ConnectionAbortedError:
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

    except ConnectionAbortedError:
        pass

    return msg


if __name__ == '__main__':
    main()
