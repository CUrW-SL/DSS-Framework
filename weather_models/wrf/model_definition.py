import sys

sys.path.insert(0, '/home/uwcc-admin/git/DSS-Framework/db_util')
# sys.path.insert(0, '/home/hasitha/PycharmProjects/DSS-Framework/db_util')
from dss_db import RuleEngineAdapter

namelist_input_template = '/home/curw/git/DSS-Framework/docker/cloud/namelist/template/template_namelist.input'
namelist_wps_template = '/home/curw/git/DSS-Framework/docker/cloud/namelist/template/template_namelist.wps'


# namelist_input_template = '/home/hasitha/PycharmProjects/DSS-Framework/docker/cloud/namelist/template/template_namelist.input'
# namelist_wps_template = '/home/hasitha/PycharmProjects/DSS-Framework/docker/cloud/namelist/template/template_namelist.wps'


def get_namelist_wps_config(dss_adapter, config_id, template_path):
    config_data = dss_adapter.get_namelist_wps_configs(config_id)
    content = None
    print('get_namelist_wps_config|config_data : ', config_data)
    if config_data is not None:
        print('get_namelist_wps_configs|config_data : ', config_data)
        with open(template_path, 'r') as file:
            content = file.read()
            content = format_content(content, config_data)
            print('content : ', content)
    return content


def get_namelist_input_config(dss_adapter, config_id, template_path):
    config_data = dss_adapter.get_namelist_input_configs(config_id)
    content = None
    print('get_namelist_input_config|config_data : ', config_data)
    if config_data is not None:
        print('get_namelist_input_config|config_data : ', config_data)
        with open(template_path, 'r') as file:
            content = file.read()
            content = format_content(content, config_data)
            print('content : ', content)
    return content


def format_content(content, config_data):
    for key, value in config_data.items():
        key_str = '{{{}}}'.format(key.upper())
        content = content.replace(key_str, str(value))
    return content


def create_namelist_wps_file(dss_adapter, config_id, template_path, file_location):
    try:
        new_content = get_namelist_wps_config(dss_adapter, config_id, template_path)
        if new_content is not None:
            with open(file_location, 'w') as file:
                file.write(new_content)
        return True
    except Exception as e:
        print('create_namelist_wps_file|Exception : ', str(e))
        return False


def create_namelist_input_file(dss_adapter, config_id, template_path, file_location):
    try:
        new_content = get_namelist_input_config(dss_adapter, config_id, template_path)
        if new_content is not None:
            with open(file_location, 'w') as file:
                file.write(new_content)
        return True
    except Exception as e:
        print('create_namelist_input_file|Exception : ', str(e))
        return False


if __name__ == "__main__":
    db_config = {'mysql_user': 'admin', 'mysql_password': 'floody', 'mysql_host': '35.227.163.211', 'mysql_db': 'dss',
                 'log_path': '/home/hasitha/PycharmProjects/DSS-Framework/log'}
    adapter = RuleEngineAdapter.get_instance(db_config)
    print(adapter)
    # get_namelist_wps_config(adapter, 1, namelist_wps_template)
    # get_namelist_input_config(adapter, 1, namelist_input_template)
    namelist_input_file = '/home/hasitha/PycharmProjects/DSS-Framework/output/namelist.input'
    create_namelist_input_file(adapter, 1, namelist_input_template, namelist_input_file)
