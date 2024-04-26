from pendulum import today, from_format


def modify_dates(params):
    if 'from_dt' not in params['config']:
        params['config']['from_dt'] = int(today().subtract(days=7).format('YYYYMMDD'))
    if 'to_dt' not in params['config']:
        params['config']['to_dt'] = int(today().subtract(days=1).format('YYYYMMDD'))
    return params


def modify_email_content(params):
    from_dt = from_format(str(params['config']['from_dt']), 'YYYYMMDD').format('YYYY-MM-DD')
    to_dt = from_format(str(params['config']['to_dt']), 'YYYYMMDD').format('YYYY-MM-DD')
    if not params['email_content']:
        params['email_content'] = (
            f"AML Suspicious buy and sale operations report for period "
            f"{from_dt} - {to_dt}"
        )
    return params


def modify_constant_values(params):
    min_volume = 'min_volume'
    diff_between_buy_and_sell_ss = 'diff_between_buy_and_sell_ss'

    if min_volume not in params['config']:
        params['config'][min_volume] = 0.1
    if diff_between_buy_and_sell_ss not in params['config']:
        params['config'][diff_between_buy_and_sell_ss] = 60
    return params


def modify_report_params(params):
    params = modify_dates(params)
    params = modify_email_content(params)
    params = modify_constant_values(params)
    return params


PARAM = {'config': {}, 'email_content': None}

params = modify_report_params(PARAM)

print(params)