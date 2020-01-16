import pandas as pd


def get_cell_mapping(sim_connection, flo2d_model):
    if 'flo2d_150' == flo2d_model:
        query = 'select grid_id,obs1,obs2,obs3,fcst from curw_sim.grid_map_flo2d_raincell where grid_id like \"flo2d_150_%\";'
    elif 'flo2d_250' == flo2d_model:
        query = 'select grid_id,obs1,obs2,obs3,fcst from curw_sim.grid_map_flo2d_raincell where grid_id like \"flo2d_250_%\";'
    print('get_cell_mapping|query : ', query)
    rows = get_multiple_result(sim_connection, query)
    print('get_cell_mapping|rows : ', rows)
    cell_map = pd.DataFrame(data=rows, columns=['grid_id', 'obs1', 'obs2', 'obs3', 'fcst'])
    print('get_cell_mapping|cell_map : ', cell_map)


def select_distinct_observed_stations(obs_connection, flo2d_model):
    if 'flo2d_150' == flo2d_model:
        query = 'select distinct(obs1) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%" union ' \
                'select distinct(obs2) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%" union ' \
                'select distinct(obs3) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_150_%";'
    elif 'flo2d_250' == flo2d_model:
        query = 'select distinct(obs1) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%" union ' \
                'select distinct(obs2) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%" union ' \
                'select distinct(obs3) from curw_sim.grid_map_flo2d_raincell where grid_id like "flo2d_250_%";'
    print('select_distinct_observed_stations|query : ', query)
    rows = get_multiple_result(obs_connection, query)
    # print('select_distinct_observed_stations|rows : ', rows)
    id_list = []
    for row in rows:
        id_list.append(row['obs1'])
    print('select_distinct_observed_stations|id_list : ', id_list)
    return id_list


def get_single_result(sim_connection, query):
    cur = sim_connection.cursor()
    cur.execute(query)
    row = cur.fetchone()
    return row


def get_multiple_result(sim_connection, query):
    cur = sim_connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows
