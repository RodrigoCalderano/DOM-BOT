import requests

# TELEGRAM TODO TROCAR O TOKEN!!!!
API_KEY_TELEGRAM = "669087829:AAGKDCOfyOwmAKMLlOvhyK9ziEM_pIJzqDs"
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

