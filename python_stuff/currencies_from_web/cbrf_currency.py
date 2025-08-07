from requests import get

rates = {}
response = get('http://www.cbr.ru/scripts/XML_daily.asp')
# response.encoding = 'cp1251'
#
# text = response.text.encode('utf-8').replace('windows-1251', 'utf-8')
print(response.text)
# cbr = parse(text)

# rates['date'] = cbr['ValCurs']['@Date']