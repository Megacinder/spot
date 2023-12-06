params = {
    'id': 25, 'dag_id': 'ma_complex_dyn', 'name': 'ma_complex_dyn',
    'emails': ['mihail.ahremenko@eglobal-group.com'], 'email_subject': '',
    'email_content': 'The account begin and end balances report for the period 20230101 - 20231031 is ready',
    'config': {
        'company': {'bvi': 1, 'svg': 2},
        'from_dt': 20230101, 'to_dt': 20231031,
        'file_name': 'account_balances-20230101-20231031',
        'file_path': '/tmp/ma_complex_dyn/manual__20231113T105945.7211190000'
    }
}

config = params['config']
sql_par = list()

for k, v in config["company"].items():
    sql_par.append({
        "company_id": str(v),
        "company_name": k,
        **{k: v for k, v in config.items() if k != "company"},
    })

for par in sql_par:
    par["file_name"] = par["company_name"] + '_' + par["file_name"]


for i in sql_par:
    print(i)
