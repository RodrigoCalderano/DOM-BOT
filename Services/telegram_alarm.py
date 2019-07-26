import requests
from Helper import Constants

API_KEY_TELEGRAM = Constants.API_KEY_TELEGRAM
BASE_URL_TELEGRAM = 'https://api.telegram.org/bot'
SEND_MESSAGE = "/sendMessage?text="
CHAT_ID = '&chat_id='
CHAT_ID_RODRIGO = '698900494'


class Telegram:

    @staticmethod
    def send_message(message):
        url = BASE_URL_TELEGRAM + API_KEY_TELEGRAM + SEND_MESSAGE + message + CHAT_ID + CHAT_ID_RODRIGO
        session = requests.Session()
        response = session.get(url)
        print("Telegram response code: " + str(response.status_code))

