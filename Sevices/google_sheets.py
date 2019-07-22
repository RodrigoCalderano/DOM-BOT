from oauth2client.service_account import ServiceAccountCredentials
import gspread


class GoogleSheets:

    @staticmethod
    def google_sheets_connector():
        """
            Connect to google sheets
            Parameters: null
            Return: sheet
        """
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
        client = gspread.authorize(credentials)
        sheet = client.open('backend').sheet1
        return sheet

    @staticmethod
    def get_orders_from_sheet(sheet):
        """
            Get orders from Google Sheets
            Parameters: sheet
            Return: orders list
            order: (stock name, entry, stop gain, stop partial, stop loss, flag (already bought?))
        """
        my_orders = []
        for row in range(100):
            order = sheet.row_values(row + 2)
            if order[0] != '-':
                my_orders.append(order)
                continue
            break
        return my_orders
