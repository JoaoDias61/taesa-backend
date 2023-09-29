from matplotlib.dates import DateFormatter, HourLocator

from ECM import *
import matplotlib.pyplot as plt
import numpy as np


def main():
    ecm = ECM()
    ecm_ids = [
        15,
        6,
        7,
        8,
        1,
        2,
        3,
        19,
        20,
        21,
        5,
        11,
        12,
        30,
        31,
        32,
        37,
        38,
        39,
        40,
        41,
        42,
        33,
        24,
        29,
        26,
        27,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
        13,
        16,
        17,
        25,
        10,
        23
    ]

    dates = ['2023-08-01T00:00:00', '2023-08-18T00:15:00']
    print(f'Data de inicial: {dates[0]}')
    print(f'Data de final: {dates[1]}')

    res_dt = []
    res_val = []

    for equi in ecm_ids:
        result = ecm.request_most_recent(str('2023-09-29T00:00:00'), equi, 2001)
        print(result)

    for equi in ecm_ids:
        result = ecm.request_time_series(str(dates[0]), str(dates[1]), equi, 2001)
        print(result)
        for item in result:
            res_dt.append(item['data'])
            res_val.append(item['valor'])

        plot_data(res_dt, res_val)

        res_dt = []
        res_val = []
        result = ecm.request_time_series(str(dates[0]), str(dates[1]), equi, 2006)
        print(result)

        for item in result:
            res_dt.append(item['data'])
            res_val.append(item['valor'])

        plot_data(res_dt, res_val)



def plot_data(dates, values):
    datas = [datetime.fromisoformat(data[:-1]) for data in dates]
    temps = [float(temp.replace(',', '.')) for temp in values]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(datas, temps, marker='o', linestyle='-', markersize=4, color='b')
    ax.xaxis.set_major_locator(HourLocator(interval=24))
    ax.xaxis.set_major_formatter(DateFormatter('%d-%m-%Y %H-%M'))
    plt.xticks(rotation=45)
    plt.xlabel('Data e Hora')
    plt.ylabel('Temperatura (°C)')
    plt.title('Dado Variável')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    main()
