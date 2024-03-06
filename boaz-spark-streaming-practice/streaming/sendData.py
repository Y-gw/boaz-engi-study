import os
import socket
from faker import Faker
import time

# 소켓 오브젝트 생성
sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
host = "127.0.0.1"     
port = 5555    
# 소켓에 ip주소와 포트 연결
sk.bind((host, port))        

print("Listening on port: %s" % str(port))
# 클라이언트 소켓 연결 대기 
sk.listen(5)    
# 클라이언트 연결 승인
client_socket, addr = sk.accept()       

print("Received request from: " + str(addr))

fake = Faker()

try:
    while True:
        sentence = fake.sentence()
        client_socket.send(("\n\n" + sentence).encode('utf-8'))
        print(sentence.encode('utf-8'))
        time.sleep(2)  # 2초마다 데이터 전송
except KeyboardInterrupt:
    client_socket.close()
    sk.close()