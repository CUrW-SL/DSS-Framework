import numpy as np
import pkg_resources
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point
from shapely.geometry import shape
import geopandas as gpd
import pandas as pd
import os


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.

    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.

    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of input vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")

    new_regions = []
    new_vertices = vor.vertices.tolist()

    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()

    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]

        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue

        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue

            # Compute the missing endpoint of an infinite ridge

            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius

            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]

        # finish
        new_regions.append(new_region.tolist())

    return new_regions, np.asarray(new_vertices)


def get_voronoi_polygons(points_dict, shape_file, shape_attribute=None, output_shape_file=None, add_total_area=True):
    """
    :param points_dict: dict of points {'id' --> [lon, lat]}
    :param shape_file: shape file path of the area
    :param shape_attribute: attribute list of the interested region [key, value]
    :param output_shape_file: if not none, a shape file will be created with the output
    :param add_total_area: if true, total area shape will also be added to output
    :return:
    geo_dataframe with voronoi polygons with columns ['id', 'lon', 'lat','area', 'geometry'] with last row being the area of the
    shape file
    """
    if shape_attribute is None:
        shape_attribute = ['OBJECTID', 1]

    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]

    ids = [p if type(p) == str else np.asscalar(p) for p in points_dict.keys()]
    points = np.array(list(points_dict.values()))[:, :2]

    vor = Voronoi(points)
    regions, vertices = _voronoi_finite_polygons_2d(vor)

    data = []
    for i, region in enumerate(regions):
        polygon = Polygon([tuple(x) for x in vertices[region]])
        if polygon.intersects(shape_polygon):
            intersection = polygon.intersection(shape_polygon)
            data.append({'id': ids[i], 'lon': vor.points[i][0], 'lat': vor.points[i][1], 'area': intersection.area,
                         'geometry': intersection
                         })
    if add_total_area:
        data.append({'id': '__total_area__', 'lon': shape_polygon.centroid.x, 'lat': shape_polygon.centroid.y,
                     'area': shape_polygon.area, 'geometry': shape_polygon})

    df = gpd.GeoDataFrame(data, columns=['id', 'lon', 'lat', 'area', 'geometry'], crs=shape_df.crs)

    if output_shape_file is not None:
        df.to_file(output_shape_file)

    return df


class ObservationMean:
    def __init__(self, shape_file):
        #self.shape_file = get_resource_path('resources/shap_files/kelani_basin/kelani_basin.shp')
        self.shape_file = shape_file
        self.percentage_factor = 100

    def calc_station_fraction(self, stations, precision_decimal_points=3):
        """
        Given station lat lon points must reside inside the KB shape, otherwise could give incorrect results.
        :param stations: dict of station_name: [lon, lat] pairs
        :param precision_decimal_points: int
        :return: dict of station_id: area percentage
        """

        if stations is None:
            raise ValueError("'stations' cannot be null.")

        station_list = stations.keys()
        if len(station_list) <= 0:
            raise ValueError("'stations' cannot be empty.")

        station_fractions = {}
        if len(station_list) < 3:
            for station in station_list:
                station_fractions[station] = np.round(self.percentage_factor / len(station_list),
                                                      precision_decimal_points)
            return station_fractions

        station_fractions = {}
        total_area = 0

        # calculate the voronoi/thesian polygons w.r.t given station points.
        voronoi_polygons = get_voronoi_polygons(points_dict=stations, shape_file=self.shape_file, add_total_area=True)

        for row in voronoi_polygons[['id', 'area']].itertuples(index=False, name=None):
            id = row[0]
            area = np.round(row[1], precision_decimal_points)
            station_fractions[id] = area
            # get_voronoi_polygons calculated total might not equal to sum of the rest, thus calculating total.
            if id != '__total_area__':
                total_area += area
        total_area = np.round(total_area, precision_decimal_points)

        for station in station_list:
            if station in station_fractions:
                station_fractions[station] = np.round(
                    (station_fractions[station] * self.percentage_factor) / total_area, precision_decimal_points)
            else:
                station_fractions[station] = np.round(0.0, precision_decimal_points)

        return station_fractions

    def calc_kb_mean(self, timerseries_dict, normalizing_factor='H', filler=0.0, precision_decimal_points=3):
        """
        :param timeseries: dict of (station_name: dict_inside) pairs. dict_inside should have
            ('lon_lat': [lon, lat]) and ('timeseries': pandas df with time(index), value columns)
        :param normalizing_factor: resampling factor, should be one of pandas resampling type
            (ref_link: http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)
        :param filler value for missing values occur when normalizing
        :return: kub mean timeseries, [[time, value]...]
        """

        stations = {}
        timerseries_list = []
        for key in timerseries_dict.keys():
            stations[key] = timerseries_dict[key]['lon_lat']
            # Resample given set of timeseries.
            tms = timerseries_dict[key]['timeseries'].astype('float')
            # Rename coulmn_name 'value' to its own staion_name.
            tms = tms.rename(axis='columns', mapper={'value': key})
            timerseries_list.append(tms)

        if len(timerseries_list) <= 0:
            raise ValueError('Empty timeseries_dict given.')
        elif len(timerseries_list) == 1:
            matrix = timerseries_list[0]
        else:
            matrix = timerseries_list[0].join(other=timerseries_list[1:len(timerseries_list)], how='outer')

        # Note:
        # After joining resampling+sum does not work properly. Gives NaN and sum that is not correct.
        # Therefore resamplig+sum is done for each timeseries. If this issue could be solved,
        # then resampling+sum could be carried out after joining.

        # Fill in missing values after joining into one timeseries matrix.
        matrix.fillna(value=np.round(filler, precision_decimal_points), inplace=True, axis='columns')

        station_fractions = self.calc_station_fraction(stations)
        # print('----------------------------------station_fractions : ', station_fractions)
        # Make sure only the required station weights remain in the station_fractions, else raise ValueError.
        matrix_station_list = list(matrix.columns.values)
        weights_station_list = list(station_fractions.keys())
        invalid_stations = [key for key in weights_station_list if key not in matrix_station_list]
        for key in invalid_stations:
            station_fractions.pop(key, None)
        if not len(matrix_station_list) == len(station_fractions.keys()):
            raise ValueError('Problem in calculated station weights.', stations, station_fractions)

        # Prepare weights to calc the kub_mean.
        weights = pd.DataFrame.from_dict(data=station_fractions, orient='index', dtype='float')
        weights = weights.divide(self.percentage_factor, axis='columns')

        kb_mean = matrix.dot(weights).sum(axis='columns')
        kb_mean_timeseries = kb_mean.to_frame(name='value')
        return kb_mean_timeseries


