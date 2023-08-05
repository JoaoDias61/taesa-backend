import datetime
from ECM import *

def main():
    ecm = ECM()
    data = ecm.request_results(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 1)

    print(data)
    print(type(data))

if __name__ == '__main__':
    main()
