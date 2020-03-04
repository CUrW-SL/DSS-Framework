import json


def get_triggering_dynamic_dags(routines):
    dag_info = []
    if len(routines) > 0:
        print('get_triggering_dynamic_dags|routines : ', routines)
        for routine in routines:
            dag_name = routine['dag_name']
            payload = routine
            dag_info.append({'dag_name': dag_name, 'payload': payload})
    else:
        print('No triggering_dags found.')
    return dag_info


def get_all_dynamic_dag_routines(dss_adapter):
    query = 'select id, dag_name, schedule, timeout from dss.dynamic_routine;'
    print('get_all_dynamic_dag_routines|query : ', query)
    results = dss_adapter.get_multiple_result(query)
    routines = []
    if results is not None:
        for result in results:
            print('get_all_external_bash_routines|result : ', result)
            routines.append({'id': result[0], 'dag_name': result[1], 'schedule': result[2],
                             'timeout': json.loads(result[3])})
    print('get_all_dynamic_dag_routines|routines : ', routines)
    return routines


def get_dynamic_dag_tasks(dss_adapter, dag_id):
    sql_query = 'select id, task_name, task_type, task_content, input_params, timeout ' \
                'from dss.dynamic_workflow where active=1 and owner_dag_id={} order by task_order asc;'.format(dag_id)
    print('get_dynamic_dag_tasks|sql_query : ', sql_query)
    results = dss_adapter.get_multiple_result(sql_query)
    print('get_dynamic_dag_tasks|results : ', results)
    dag_tasks = []
    if results is not None:
        for result in results:
            if result[4]:
                dag_tasks.append({'id': result[0], 'task_name': result[1], 'task_type': result[2], 'task_content': result[3],
                                  'input_params': json.loads(result[4]), 'timeout': json.loads(result[5])})
            else:
                dag_tasks.append({'id': result[0], 'task_name': result[1], 'task_type': result[2],
                                  'task_content': result[3], 'input_params': result[4], 'timeout': json.loads(result[5])})
    print('get_dynamic_dag_tasks|dag_tasks : ', dag_tasks)
    return dag_tasks


def get_trigger_target_dag(dss_adapter, dag_rule_id, task_name):
    sql_query = 'select id, task_name, task_type, task_content, input_params, timeout ' \
                'from dss.dynamic_workflow where active=1 and owner_dag_id={} and task_name=\'{}\' ;'.format(dag_rule_id,
                                                                                                         task_name)
    print('get_trigger_target_dag|sql_query : ', sql_query)
    result = dss_adapter.get_single_row(sql_query)
    print('get_trigger_target_dag|result : ', result)
    if result is not None:
        return {'id': result[0], 'task_name': result[1], 'task_type': result[2], 'task_content': result[3],
         'input_params': json.loads(result[4]), 'timeout': json.loads(result[5])}
    else:
        print('get_trigger_target_dag|no results')
        return None


def get_pump_trigger_payload(dss_adapter, id_list):
    rule_list = dss_adapter.get_pump_operating_rules(id_list)
    for rule in rule_list:
        print('get_pump_trigger_payload|rule : ', rule)
        logic = rule['logic']
        if dss_adapter.evaluate_rule_logic(logic):
            flo2d_rule_id = rule['flo2d_rule']
            flo2d_rule = dss_adapter.get_flo2d_rule_info_by_id(flo2d_rule_id)
            print('get_pump_trigger_payload|flo2d_rule : ', flo2d_rule)
            return flo2d_rule
    return None



