from pandas import read_csv, option_context, DataFrame, concat
import sys


def draw_progress_bar(percent, bar_len=20):
    sys.stdout.write("\r")
    progress = ""
    for i in range(bar_len):
        if i < int(bar_len * percent):
            progress += "="
        else:
            progress += " "
    sys.stdout.write("[ %s ] %.2f%%" % (progress, percent * 100))
    sys.stdout.flush()


df1 = read_csv('/home/mihaahre/Downloads/Telegram Desktop/alfa/alfa.txt', nrows=0, encoding="utf-16", delimiter=';')
df1 = DataFrame(data=None, columns=df1.columns, index=df1.index)


CHUNKSIZE = 10 ** 7
NROWS = 100000000000
i = 1
for df in read_csv(
    filepath_or_buffer='/home/mihaahre/Downloads/Telegram Desktop/alfa/alfa.txt',
    nrows=NROWS,
    encoding="utf-16",
    delimiter=';',
    chunksize=CHUNKSIZE,
):
    df['FullName'] = df['FullName'].str.lower()
    # print(CHUNKSIZE * i, 'from', NROWS, '=', format(CHUNKSIZE * i / NROWS, '.0%'))

    draw_progress_bar(CHUNKSIZE * i / NROWS, 100)

    i += 1
    df = df[df['FullName'].str.contains('ахр')]
    if not df.empty:
        print(df.to_string())
    df1 = concat([df1, df])
    # df = df.sort_values(by=['FullName', 'BirthDate'])

df1 = df1.sort_values(by=['FullName', 'BirthDate'])
print(df1.to_string())

# print(df.columns)
#df = df.filter(like='ахременко', axis=0)
