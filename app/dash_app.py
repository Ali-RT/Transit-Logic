#!/usr/bin/env python3
"""
    Dash app for Insight DE project
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.2'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
import plotly.express as px
from dash.exceptions import PreventUpdate
import geohash
from geojson import Feature, FeatureCollection, Polygon

# Connect to PostgreSQL database
host = "ec2-52-14-39-59.us-east-2.compute.amazonaws.com"
# host = "127.0.0.1"
port = "5432"
database = "postgres"
user = "postgres"
password = "project2020"
conn = psycopg2.connect(dbname=database, user=user, password=password, host=host, port=port)
cursor = conn.cursor()


def geohash_polygon(gh):
    """
    Creates a polygon object given geohash alphanumeric string
    Args:
        gh: Geohash string
    """
    b = geohash.bbox(gh)
    box = [(b['w'], b['s']), (b['w'], b['n']), (b['e'], b['n']), (b['e'], b['s'],), (b['w'], b['s'])]
    return Polygon([box])


def create_collection(geohashes):
    """
    Creates a features collection of polgons
    Args:
        geohashes: a list of geohashes
    """
    polys = []
    for gh in geohashes:
        polys.append(Feature(geometry=geohash_polygon(gh), properties={"id": gh}))
    geos = FeatureCollection(polys)
    return geos


def generate_kpi(operator, geo, v_type, broken_kpi, avail_kpi, input_year, input_day, hr_flag):
    """
    Generate dataframe from SQL query
    Args:
        operator: operators' name or all
        geo: geohash level 6, 7, and 8
        v_type: vehicle type
        broken_kpi: if the vehicle is broken or not
        avail_kpi: if the vehicle is available or not
        input_year: year
        input_day: day
        hr_flag: if the result aggregated by hour
    """
    if hr_flag:
        if operator == "All":
            postgreSQL_select_Query = "SELECT date_trunc('hour', tmstmp) h,"\
                                      + geo + " AS geo, operator, COUNT(DISTINCT id) AS count FROM locations_table WHERE extract(year from tmstmp) = "\
                                      + str(input_year) + " AND extract(day from tmstmp) = "\
                                      + str(input_day) + " AND is_disabled = " + str(broken_kpi) + "AND is_reserved= "\
                                      + str(avail_kpi) + " GROUP BY h, operator, " + geo + ""

        else:
            postgreSQL_select_Query = "SELECT date_trunc('hour', tmstmp) h," + geo + " AS geo, COUNT(DISTINCT id) AS count FROM locations_table WHERE extract(year from tmstmp) = "\
                                      + str(input_year) + " AND extract(day from tmstmp) = "\
                                      + str(input_day) + " AND operator LIKE '%" + operator + "%' AND type LIKE '%"\
                                      + v_type + "%' AND is_disabled = " + str(broken_kpi) + "AND is_reserved= "\
                                      + str(avail_kpi) + " GROUP BY h, " + geo + ""
    else:

        if operator == "All":
            postgreSQL_select_Query = "SELECT "\
                                      + geo + " AS geo, operator, COUNT(DISTINCT id) AS count FROM locations_table WHERE extract(year from tmstmp) = "\
                                      + str(input_year) + " AND extract(day from tmstmp) = "\
                                      + str(input_day) + " AND is_disabled = " + str(broken_kpi) + "AND is_reserved= "\
                                      + str(avail_kpi) + " GROUP BY operator, " + geo + ""

        else:
            postgreSQL_select_Query = "SELECT " + geo + " AS geo, COUNT(DISTINCT id) AS count FROM locations_table WHERE extract(year from tmstmp) = "\
                                      + str(input_year) + " AND extract(day from tmstmp) = "\
                                      + str(input_day) + " AND operator LIKE '%" + operator + "%' AND type LIKE '%"\
                                      + v_type + "%' AND is_disabled = " + str(broken_kpi) + "AND is_reserved= "\
                                      + str(avail_kpi) + " GROUP BY " + geo + ""

    # print(postgreSQL_select_Query)
    cursor.execute(postgreSQL_select_Query)
    df = pd.read_sql_query(postgreSQL_select_Query, conn)
    # print(df)
    return df


def generate_graph(dataframe):
    """
    Generate plotly graph from a dataframe
    Args:
        dataframe: a pandas dataframe
    """
    geos = create_collection(dataframe['geo'])
    if "operator" in dataframe.keys():

        fig = px.choropleth_mapbox(dataframe, geojson=geos, color="operator",
                                   locations="geo", featureidkey="properties.id",
                                   center={"lat": 38.897841, "lon": -77.036594},
                                   mapbox_style="carto-positron", zoom=11)
    else:
        fig = px.choropleth_mapbox(dataframe, geojson=geos, color="count",
                                   locations="geo", featureidkey="properties.id",
                                   center={"lat": 38.897841, "lon": -77.036594},
                                   mapbox_style="carto-positron", zoom=11)

    fig.update_geos(fitbounds="locations", visible=True)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig


original_dataframe = generate_kpi('All', 'geo7', 'scooter', 0, 0, 2020, 21, 0)
original_fig = generate_graph(original_dataframe)


px.set_mapbox_access_token(open(".mapbox_token").read())

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

markdown_text_header = '''
# **T**ransit **L**ogic
'''

app.layout = html.Div(children=[

    dcc.Markdown(children=markdown_text_header, style={'textAlign': 'center'}),
    html.H3(
        children='Improving micro mobility asset utilization in real-time',
        style={'textAlign': 'center'}
    ),

    html.Div([
        html.Div([
            html.Label('Operators'),
            dcc.Dropdown(
                id='operators-dropdown',
                options=[
                    {'label': 'Bird', 'value': 'Bird'},
                    {'label': 'Helbiz', 'value': 'Helbiz'},
                    {'label': 'Lime', 'value': 'Lime'},
                    {'label': 'Lyft', 'value': 'Lyft'},
                    {'label': 'Skip', 'value': 'Skip'},
                    {'label': 'Spin', 'value': 'Spin'},
                    {'label': 'All', 'value': 'All'}], )], className='one column'),
        html.Div([
            html.Label('Geohash Precision Level'),
            dcc.Dropdown(
                id='geohashes-dropdown',
                options=[
                    {'label': 'Level 6', 'value': 'geo6'},
                    {'label': 'Level 7', 'value': 'geo7'},
                    {'label': 'Level 8', 'value': 'geo8'}])], className='two columns'),
        html.Div([
            html.Label('Vehicle Type'),
            dcc.Dropdown(
                id='type-dropdown',
                options=[
                    {'label': 'Bike', 'value': 'bike'},
                    {'label': 'Scooter', 'value': 'scooter'}])], className='two columns'),

        html.Div([
            html.Label('KPI:Broken'),
            dcc.Dropdown(
                id='kpi-broken-dropdown',
                options=[
                    {'label': 'Broken', 'value': 1},
                    {'label': 'Not Broken', 'value': 0}])], className='two columns'),

        html.Div([
            html.Label('KPI:Available'),
            dcc.Dropdown(
                id='kpi-avail-dropdown',
                options=[
                    {'label': 'Available', 'value': 0},
                    {'label': 'Not Available', 'value': 1}])], className='two columns'),

        html.Div([
            html.I("Date range:YYYY-MM-DD"),
            html.Br(),
            dcc.Input(
                id="input-year", type="number", placeholder="year",
                min=2020, max=2025, step=1,
            ),
            dcc.Input(
                id="input-day", type="number", placeholder="day",
                min=1, max=31, step=1,
            )], className='two columns'),

        html.Div([
            html.Label('By hour'),
            dcc.Dropdown(
                id='agg-hr',
                options=[
                    {'label': 'Off', 'value': 0},
                    {'label': 'On', 'value': 1}])], className='one column')

    ]),


    html.Div([
        html.Button('Submit', id='button', style={'width': '30%'})
    ], style={'textAlign': 'center'}),

    # html.H5(id='display-selected-input',
    #         children='',
    #         style={'textAlign': 'center'}),

    dcc.Graph(
        id="map",
        figure=original_fig),
])


# @app.callback(
#     Output(component_id='display-selected-input', component_property='children'),
#     [Input(component_id='operators-dropdown', component_property='value'),
#      Input(component_id='geohashes-dropdown', component_property='value'),
#      Input(component_id='type-dropdown', component_property='value'),
#      Input(component_id='kpi-broken-dropdown', component_property='value'),
#      Input(component_id='kpi-avail-dropdown', component_property='value'),
#      Input(component_id='input-year', component_property='value'),
#      Input(component_id='input-day', component_property='value'),
#      Input(component_id='agg-hr', component_property='value'),
#      Input(component_id='button', component_property='n_clicks')])
# def set_display_items(operator, geo, v_type, broken_kpi, avail_kpi, start_date, end_date, hr_flag, n_clicks):
#     if n_clicks is None:
#         raise PreventUpdate
#     else:
#         return 'You\'re querying {}, {}, {}, {}, {} {} {} {}."'.format(broken_kpi, avail_kpi, geo, v_type, operator, start_date, end_date, hr_flag)


@app.callback(
    Output('map', 'figure'),
    [Input(component_id='operators-dropdown', component_property='value'),
     Input(component_id='geohashes-dropdown', component_property='value'),
     Input(component_id='type-dropdown', component_property='value'),
     Input(component_id='kpi-broken-dropdown', component_property='value'),
     Input(component_id='kpi-avail-dropdown', component_property='value'),
     Input(component_id="input-year", component_property="value"),
     Input(component_id="input-day", component_property="value"),
     Input(component_id='agg-hr', component_property='value'),
     Input(component_id='button', component_property='n_clicks')])
def set_map_display(operator, geo, v_type, broken_kpi, avail_kpi, input_year, input_day, hr_flag, n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        dataframe = generate_kpi(operator, geo, v_type, broken_kpi, avail_kpi, input_year, input_day, hr_flag)
        return generate_graph(dataframe)


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
