import blpapi
import matplotlib.pyplot as plt
from datetime import datetime

def bloomberg_historical_data(symbol, start_date, end_date):
    # Connect to Bloomberg
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost("localhost")
    sessionOptions.setServerPort(8194)
    session = blpapi.Session(sessionOptions)

    if not session.start():
        print("Failed to start session.")
        return None

    try:
        if not session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return None

        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue(symbol)
        request.getElement("fields").appendValue("PX_LAST")
        request.set("startDate", start_date.strftime("%Y%m%d"))
        request.set("endDate", end_date.strftime("%Y%m%d"))
        request.set("periodicitySelection", "DAILY")

        session.sendRequest(request)

        response = []
        while True:
            event = session.nextEvent()
            for msg in event:
                if msg.hasElement("securityData"):
                    securityData = msg.getElement("securityData")
                    if securityData.hasElement("fieldData"):
                        fieldDataArray = securityData.getElement("fieldData")
                        for i in range(fieldDataArray.numValues()):
                            fieldData = fieldDataArray.getValueAsElement(i)
                            date = fieldData.getElementAsDatetime("date")
                            price = fieldData.getElementAsFloat("PX_LAST")
                            response.append((date, price))

            if event.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        session.stop()

    return response

def plot_stock_prices(data):
    if not data:
        print("No data available to plot.")
        return

    dates = [x[0] for x in data]
    prices = [x[1] for x in data]

    plt.scatter(dates, prices, color='blue', marker='o', label='Close Price')
    plt.xlabel('Date')
    plt.ylabel('Price ($)')
    plt.title('Stock Prices Over Time')
    plt.legend()
    plt.xticks(rotation=45)
    plt.show()

if __name__ == "__main__":
    symbol = 'AAPL US Equity'
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)

    stock_data = bloomberg_historical_data(symbol, start_date, end_date)
    if stock_data:
        plot_stock_prices(stock_data)
