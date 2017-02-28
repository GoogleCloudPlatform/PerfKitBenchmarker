def _recv_all(sock, length):
  data = b''
  while len(data) < length:
    packet = sock.recv(length - len(data))
    if len(packet) == 0:
      return None
    data += packet
  return data

class ServiceConnection:
  def __init__(self, sock):
    self.socket = sock

  def send(self, msg):
    msg_len = len(msg).to_bytes(4, byteorder='little')
    self.socket.sendall(msg_len + msg)

  def recv(self):
    msg_len = self.socket.recv(4)
    msg_len = int.from_bytes(msg_len, byteorder='little')
    return _recv_all(self.socket, msg_len)

  def send_str(self, msg):
    self.send(msg.encode('utf-8'))

  def recv_str(self):
    return self.recv().decode('utf-8')
  
  def close(self):
    self.socket.close()
