
def get_triggering_dags(db_adapter, dss_rule_id, model_type):
    print('get_triggering_dags|dss_rule_id : ', dss_rule_id)
    print('get_triggering_dags|model_type : ', model_type)
    dag_info = []
    if model_type is 'wrf':
        wrf_rule_info = db_adapter.get_wrf_rule_info_by_id(dss_rule_id)
        if wrf_rule_info:
            target_models = wrf_rule_info['target_model'].split(',')
            for target_model in target_models:
                print('target_model : ', target_model)
                dag_id = 'wrf_{}_{}_dag'.format(wrf_rule_info['version'], target_model)
                payload = {'run': wrf_rule_info['run'], 'hour': wrf_rule_info['hour'],
                           'ignore_previous_run': wrf_rule_info['ignore_previous_run'],
                           'check_gfs_data_availability': wrf_rule_info['check_gfs_data_availability']}
                dag_info.append({'dag_id': dag_id, 'payload': payload})
        else:
            print('No wrf rules found.')
    elif model_type is 'hechms':
        hechms_rule_info = db_adapter.get_wrf_rule_info_by_id(dss_rule_id)
        if hechms_rule_info:
            target_models = hechms_rule_info['target_model'].split(',')
            for target_model in target_models:
                print('target_model : ', target_model)
                dag_id = 'wrf_{}_dag'.format(target_model)
                payload = {'forecast_days': hechms_rule_info['forecast_days'],
                           'observed_days': hechms_rule_info['observed_days'],
                           'init_run': hechms_rule_info['init_run'],
                           'no_forecast_continue': hechms_rule_info['no_forecast_continue'],
                           'no_observed_continue': hechms_rule_info['no_observed_continue'],
                           'rainfall_data_from': hechms_rule_info['rainfall_data_from'],
                           'ignore_previous_run': hechms_rule_info['ignore_previous_run']}
                dag_info.append({'dag_id': dag_id, 'payload': payload})
        else:
            print('No hechms rules found.')
    elif model_type is 'flo2d':
        flo2d_rule_info = db_adapter.get_wrf_rule_info_by_id(dss_rule_id)
        if flo2d_rule_info:
            target_models = flo2d_rule_info['target_model'].split(',')
            for target_model in target_models:
                print('target_model : ', target_model)
                dag_id = 'wrf_{}_dag'.format(target_model)
                payload = {'forecast_days': flo2d_rule_info['forecast_days'],
                           'observed_days': flo2d_rule_info['observed_days'],
                           'no_forecast_continue': flo2d_rule_info['no_forecast_continue'],
                           'no_observed_continue': flo2d_rule_info['no_observed_continue'],
                           'raincell_data_from': flo2d_rule_info['raincell_data_from'],
                           'inflow_data_from': flo2d_rule_info['inflow_data_from'],
                           'outflow_data_from': flo2d_rule_info['outflow_data_from'],
                           'ignore_previous_run': flo2d_rule_info['ignore_previous_run']}
                dag_info.append({'dag_id': dag_id, 'payload': payload})
        else:
            print('No flo2d rules found.')
    else:
        print('Undefined model_type type|model_type : ', model_type)
    return dag_info

