from datetime import datetime

import dbf
import pandas as pd

dir_path = "res/testData.CSV"
drop_list = [2, 3, 4, 6, 7, 8, 9, 10, 13, 14, 16, 18,
             19, 21, 24, 30, 37, 40, 41, 42]

neeKeY = ["IBAN_CRED", "OPERDATA", "OPERATION", "KODVA", "OPERSUM", "NAZNACH", "IBAN_DEB", "NAIM_DEB",
          "MFO_DEB", "KOD_DEB", "NOMER"]


def loud_data():
    dirty_data = pd.read_csv(dir_path, sep=';', skiprows=1, encoding='cp1252', header=None)

    dirty_data = dirty_data.dropna(axis=1)

    dirty_data = dirty_data.drop(drop_list, axis=1)

    dirty_data.columns = neeKeY

    new_table = dbf.Table('new_file_name.dbf', 'IBAN_CRED C(31);'
                                               'OPERDATA D;'
                                               'KODVA N(3,0);'
                                               'OPERSUM N(16,2);'
                                               'NAZNACH C(160);'
                                               'IBAN_DEB C(31);'
                                               'NAIM_DEB C(38);'
                                               'MFO_DEB N(6,0);'
                                               'KOD_DEB C(10);'
                                               'NOMER C(10)', codepage='cp1252')

    new_table.open(dbf.READ_WRITE)

    for data in dirty_data.values:
        date = datetime.strptime(str(data[1]), '%Y%m%d')

        new_table.append({'IBAN_CRED': data[0], 'OPERDATA': date,
                          'KODVA': 980,
                          'OPERSUM': int(data[4]) / 100 * -1 if data[2] == 1 else int(data[4]) / 100,
                          'NAZNACH': data[5],
                          'IBAN_DEB': data[6],
                          'NAIM_DEB': data[7],
                          'MFO_DEB': data[8],
                          'KOD_DEB': str(data[9]),
                          'NOMER': data[10]})

        # print(u"{}".format(data).encode('latin1').decode('cp1251'))


if __name__ == '__main__':
    loud_data()
