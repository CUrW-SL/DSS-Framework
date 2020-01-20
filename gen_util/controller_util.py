
def get_triggering_dags(db_adapter, dss_rule_id, model_type):
    """
    create dag name and dag info for relevant weather models.
    :param db_adapter:
    :param dss_rule_id: int/string
    :param model_type: string, 'wrf'/'hechms'/'flo2d'
    :return: [{'dag_name': dag_name, 'payload': payload},{},{}...]
    """
    print('get_triggering_dags|dss_rule_id : ', dss_rule_id)
    print('get_triggering_dags|model_type : ', model_type)
    dag_info = []
    if model_type is 'wrf':
        wrf_rule_info = db_adapter.get_eligible_wrf_rule_info_by_id(dss_rule_id)
        print('get_triggering_dags|wrf_rule_info : ', wrf_rule_info)
        if wrf_rule_info:
            target_models = wrf_rule_info['target_model'].split(',')
            for target_model in target_models:
                target_model = target_model.strip()
                print('target_model : ', target_model)
                dag_name = 'wrf_{}_{}_dag'.format(wrf_rule_info['version'], target_model)
                payload = {'id': wrf_rule_info['id'], 'run': wrf_rule_info['run'], 'hour': wrf_rule_info['hour'],
                           'ignore_previous_run': wrf_rule_info['ignore_previous_run'],
                           'check_gfs_data_availability': wrf_rule_info['check_gfs_data_availability'],
                           'accuracy_rule': wrf_rule_info['accuracy_rule'],
                           'rule_details': wrf_rule_info['rule_details']}
                dag_info.append({'dag_name': dag_name, 'payload': payload})
        else:
            print('No wrf rules found.')
    elif model_type is 'hechms':
        hechms_rule_info = db_adapter.get_eligible_hechms_rule_info_by_id(dss_rule_id)
        if hechms_rule_info:
            target_models = hechms_rule_info['target_model'].split(',')
            for target_model in target_models:
                target_model = target_model.strip()
                print('target_model : ', target_model)
                dag_name = 'hechms_{}_dag'.format(target_model)
                payload = {'id': hechms_rule_info['id'], 'forecast_days': hechms_rule_info['forecast_days'],
                           'observed_days': hechms_rule_info['observed_days'],
                           'init_run': hechms_rule_info['init_run'],
                           'no_forecast_continue': hechms_rule_info['no_forecast_continue'],
                           'no_observed_continue': hechms_rule_info['no_observed_continue'],
                           'rainfall_data_from': hechms_rule_info['rainfall_data_from'],
                           'ignore_previous_run': hechms_rule_info['ignore_previous_run'],
                           'accuracy_rule': hechms_rule_info['accuracy_rule'],
                           'rule_details': hechms_rule_info['rule_details']}
                dag_info.append({'dag_name': dag_name, 'payload': payload})
        else:
            print('No hechms rules found.')
    elif model_type is 'flo2d':
        flo2d_rule_info = db_adapter.get_eligible_flo2d_rule_info_by_id(dss_rule_id)
        if flo2d_rule_info:
            target_models = flo2d_rule_info['target_model'].split(',')
            for target_model in target_models:
                target_model = target_model.strip()
                print('target_model : ', target_model)
                dag_name = 'flo2d_{}_dag'.format(target_model)
                payload = {'id': flo2d_rule_info['id'], 'forecast_days': flo2d_rule_info['forecast_days'],
                           'observed_days': flo2d_rule_info['observed_days'],
                           'no_forecast_continue': flo2d_rule_info['no_forecast_continue'],
                           'no_observed_continue': flo2d_rule_info['no_observed_continue'],
                           'raincell_data_from': flo2d_rule_info['raincell_data_from'],
                           'inflow_data_from': flo2d_rule_info['inflow_data_from'],
                           'outflow_data_from': flo2d_rule_info['outflow_data_from'],
                           'ignore_previous_run': flo2d_rule_info['ignore_previous_run'],
                           'accuracy_rule': flo2d_rule_info['accuracy_rule'],
                           'rule_details': flo2d_rule_info['rule_details']}
                dag_info.append({'dag_name': dag_name, 'payload': payload})
        else:
            print('No flo2d rules found.')
    else:
        print('Undefined model_type type|model_type : ', model_type)
    print('get_triggering_dags|dag_info : ', dag_info)
    return dag_info


def update_workflow_routine_status(db_adapter):
    print('update_workflow_routine_status')
    running_routines = db_adapter.get_workflow_routines(2)
    print('update_workflow_routine_status|running_routines : ', running_routines)
    if len(running_routines) > 0:
        for running_routine in running_routines:
            wrf_completed = False
            hechms_completed = False
            flo2d_completed = False
            wrf_error = False
            hechms_error = False
            flo2d_error = False
            print('update_workflow_routine_status|running_routine : ', running_routine)
            routine_id = running_routine['id']
            wrf_rule_id = running_routine['dss1']
            hechms_rule_id = running_routine['dss2']
            flo2d_rule_id = running_routine['dss3']

            if wrf_rule_id == 0 or wrf_rule_id == '0':
                wrf_completed = True
            else:
                wrf_rule_info = db_adapter.get_wrf_rule_status_by_id(wrf_rule_id)
                if wrf_rule_info is not None:
                    wrf_rule_status = wrf_rule_info['status']
                    if (wrf_rule_status == 3) or (wrf_rule_status == '3'):
                        wrf_completed = True
                    elif (wrf_rule_status == 4) or (wrf_rule_status == '4'):
                        wrf_error = True
            print('update_workflow_routine_status|wrf_completed : ', wrf_completed)
            if hechms_rule_id == 0 or hechms_rule_id == '0':
                hechms_completed = True
            else:
                hechms_rule_info = db_adapter.get_hechms_rule_status_by_id(hechms_rule_id)
                if hechms_rule_info is not None:
                    hechms_rule_status = hechms_rule_info['status']
                    if (hechms_rule_status == 3) or (hechms_rule_status == '3'):
                        hechms_completed = True
                    elif (hechms_rule_status == 4) or (hechms_rule_status == '4'):
                        hechms_error = True
            print('update_workflow_routine_status|hechms_completed : ', hechms_completed)
            if flo2d_rule_id == 0 or flo2d_rule_id == '0':
                flo2d_completed = True
            else:
                flo2d_rule_info = db_adapter.get_flo2d_rule_status_by_id(flo2d_rule_id)
                if flo2d_rule_info is not None:
                    flo2d_rule_status = flo2d_rule_info['status']
                    if (flo2d_rule_status == 3) or (flo2d_rule_status == '3'):
                        flo2d_completed = True
                    elif (flo2d_rule_status == 4) or (flo2d_rule_status == '4'):
                        flo2d_error = True
            print('update_workflow_routine_status|flo2d_completed : ', flo2d_completed)
            if wrf_completed and hechms_completed and flo2d_completed:
                db_adapter.update_workflow_routing_status(3, routine_id)
                print('routine has completed.')
            elif wrf_error and hechms_error and flo2d_error:
                db_adapter.update_workflow_routing_status(3, routine_id)
                print('routine has completed.')
            elif wrf_error and hechms_completed and flo2d_completed:
                db_adapter.update_workflow_routing_status(3, routine_id)
                print('routine has completed.')
            elif wrf_completed and hechms_error and flo2d_completed:
                db_adapter.update_workflow_routing_status(3, routine_id)
                print('routine has completed.')
            elif wrf_completed and hechms_completed and flo2d_error:
                db_adapter.update_workflow_routing_status(3, routine_id)
                print('routine has completed.')
            else:
                print('routine hasn\'t completed.')
    else:
        print('No running workflows.')


def get_triggering_variable_dags(variable_routines):
    dag_info = []
    if len(variable_routines) > 0:
        for variable_routine in variable_routines:
            dag_name = variable_routine['dag_name']
            payload = variable_routine
            dag_info.append({'dag_name': dag_name, 'payload': payload})
    else:
        print('No triggering_variable_dags found.')
    return dag_info


# [{'id': 1, 'dag_name': 'dynamic_dag1', 'schedule': '*/10 * * * *', 'timeout': '"{"hours":0,"minutes":5,"seconds":0}"'}]
def get_triggering_external_bash_dags(external_routines):
    dag_info = []
    if len(external_routines) > 0:
        print('get_triggering_external_bash_dags|external_routines : ', external_routines)
        # for variable_routine in variable_routines:
        #     dag_name = variable_routine['dag_name']
        #     payload = variable_routine
        #     dag_info.append({'dag_name': dag_name, 'payload': payload})
    else:
        print('No triggering_variable_dags found.')
    return dag_info


def set_running_state(db_adapter, routine_id):
    print('set_running_state|routine_id: ', routine_id)
    db_adapter.update_workflow_routing_status(2, routine_id)


def set_variable_routine_running_state(db_adapter, routine_id):
    print('set_variable_routine_running_state|routine_id: ', routine_id)
    db_adapter.update_variable_routing_status(2, routine_id)


def is_matched(expression):
    """
    Finds out how balanced an expression is.
    With a string containing only brackets.

    >>> is_matched('[]()()(((([])))')
    False
    >>> is_matched('[](){{{[]}}}')
    True
    """
    opening = tuple('({[')
    closing = tuple(')}]')
    mapping = dict(zip(opening, closing))
    queue = []

    for letter in expression:
        if letter in opening:
            queue.append(mapping[letter])
        elif letter in closing:
            if not queue or letter != queue.pop():
                return False
    return not queue
