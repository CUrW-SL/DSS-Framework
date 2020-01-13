def get_next_pump_configurations(routines):
    dag_info = []
    success_routines = evaluate_configuration_logics(routines)
    if len(success_routines) > 0:
        for success_routine in success_routines:
            dag_name = success_routine['dag_name']
            payload = success_routine
            dag_info.append({'dag_name': dag_name, 'payload': payload})
    else:
        print('No triggering_pump_dags found.')
    return dag_info


def evaluate_configuration_logics(routines):
    print('evaluate_configuration_logics|routines : ', routines)
    return routines
