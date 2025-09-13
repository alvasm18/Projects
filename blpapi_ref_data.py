import blpapi
from blpapi import Session, SessionOptions
import pandas as pd


def bloomberg_session():
    options = SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)
    session = Session(options)
    if not session.start():
        raise ConnectionError("Failed to connect to Bloomberg")
    if not session.openService("//blp/refdata"):
        raise RuntimeError("Failed to open //blp/refdata service")
    return session

-
def bloomberg_reference_data(ticker: str, fields: list):
    """
    Fetches descriptive/reference data for a given Bloomberg ticker.

    Parameters:
        ticker (str): Bloomberg ticker (e.g. 'GCZ4 Comdty')

    Returns:
        pd.DataFrame: DataFrame with field-value pairs
    """
    session = bloomberg_session()
    refDataService = session.getService("//blp/refdata")

    # Create request
    request = refDataService.createRequest("ReferenceDataRequest")
    request.append("securities", ticker)
    for field in fields:
        request.append("fields", field)

    # Send request
    session.sendRequest(request)

    # Process response
    data = {}
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement("securityData"):
                secData = msg.getElement("securityData")
                fieldData = secData.getElement(0).getElement("fieldData")
                for field in fields:
                    if fieldData.hasElement(field):
                        data[field] = fieldData.getElementAsString(field)
                    else:
                        data[field] = None

        if event.eventType() == blpapi.Event.RESPONSE:
            break

    session.stop()
    return pd.DataFrame([data], index=[ticker])


if __name__ == "__main__":
    ticker = "GCZ4 Comdty"  # Gold December 2024 future
    fields = [
        "PX_LAST",          # Last price
        "FUT_DLV_DT_LAST",  # Last delivery date
        "FUT_NOTICE_FIRST", # First notice date
        "FUT_CONT_SIZE",    # Contract size
        "SECURITY_DES",     # Security description
    ]

    df = bloomberg_reference_data(ticker, fields)
    print(df)
