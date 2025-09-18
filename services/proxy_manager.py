from services.utils import config
import requests
import socket
from typing import Callable

class ProxyManager:
    def __init__(self, logger: Callable[..., None]):
        self.logger: Callable[..., None] = logger
 
    def check_ip(self) -> None:
        url = config.IP_CHECK_URL
        proxies = config.PROXIES if config.USE_PROXY else None
        response = requests.get(url, proxies=proxies)
        self.logger(response.text)

    def send_signal_newnym(self, password: str = None) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((config.PROXY_SETTING_IP, config.PROXY_SETTING_PORT))

        def send_cmd(cmd):
            s.send((cmd + '\r\n').encode())
            return s.recv(1024).decode()

        if password:
            resp = send_cmd(f'AUTHENTICATE "{password}"')
        else:
            resp = send_cmd('AUTHENTICATE')

        if '250 OK' not in resp:
            self.logger('Authentication failed:', resp)
            s.close()
            return False

        resp = send_cmd('SIGNAL NEWNYM')
        if '250 OK' in resp:
            self.logger('circuit changed')
            s.close()
            return True
        else:
            self.logger('Failed to change circuit:', resp)
            s.close()
            return False