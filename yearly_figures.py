import os
import re
import sys

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import requests
from plotly.subplots import make_subplots

import tableau

WORKBOOK = 'Annual Report 2024'

# extra luids for now
# total_cs_quantity = tableau.find_view_luid(view_name='Total CS Ave Quantity', workbook_name=workbook)
# total_cs_gender = tableau.find_view_luid(view_name='Total CS by Gender', workbook_name=workbook)
# total_cs_age = tableau.find_view_luid(view_name='Total CS by Agegroup', workbook_name=workbook)
# total_cs_presc_county = tableau.find_view_luid(view_name='Total CS by Prescriber County', workbook_name=workbook)
# opi_pills = tableau.find_view_luid(view_name='Opi Pills Dispensed', workbook_name=workbook)
# androgen_dispensed = tableau.find_view_luid(view_name='Androgens', workbook_name=workbook)

def human_format(num):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

def county_data():
    # ---
    # county data
    # ---
    print('generating county data...')
    counties = requests.get('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json').json()

    total_cs_pat_county = tableau.find_view_luid(view_name='Total CS by Patient County', workbook_name=WORKBOOK)
    opi_disp_pat_county = tableau.find_view_luid(view_name='Total Opi by Patient County', workbook_name=WORKBOOK)
    bup_disp_pat_county = tableau.find_view_luid(view_name='Total Bup by Patient County', workbook_name=WORKBOOK)
    benzo_dis_pat_county = tableau.find_view_luid(view_name='Total Benzo by Patient County', workbook_name=WORKBOOK)
    stim_disp_pat_county = tableau.find_view_luid(view_name='Total Stimulant by Patient County', workbook_name=WORKBOOK)
    andro_disp_pat_county = tableau.find_view_luid(view_name='Androgens by Patient County', workbook_name=WORKBOOK)

    rx_pat_county = (
        tableau.lazyframe_from_view_id(total_cs_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count')
        )
    )

    opi_pat_county = (
        tableau.lazyframe_from_view_id(opi_disp_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('opi_rx_count')
        )
    )

    bup_pat_county = (
        tableau.lazyframe_from_view_id(bup_disp_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('bup_rx_count')
        )
    )

    benzo_pat_county = (
        tableau.lazyframe_from_view_id(benzo_dis_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('benzo_rx_count')
        )
    )

    stim_pat_county = (
        tableau.lazyframe_from_view_id(stim_disp_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('stim_rx_count')
        )
    )

    andro_pat_county = (
        tableau.lazyframe_from_view_id(andro_disp_pat_county, infer_schema_length=100).collect()
        .select(
            pl.col('Orig Patient County').str.to_uppercase().alias('county'),
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('andro_rx_count')
        )
    )

    fips_txt = requests.get('https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt').content.decode()
    sections = re.split(r'-+\s+-+', fips_txt)

    def parse_fips_data(section:str, col_names:list) -> pl.DataFrame:
        data_dict = {col_names[0]: [], col_names[1]: []}
        for line in section.strip().splitlines():
            if not line:
                break
            row = re.split(r'\s{2,}', line.strip())
            data_dict[col_names[0]].append(row[0])
            data_dict[col_names[1]].append(row[1])
        return pl.DataFrame(data_dict)

    fips_state = parse_fips_data(sections[1], ['fips', 'state'])
    fips = parse_fips_data(sections[2], ['fips', 'county'])

    CENSUS_API_KEY = os.environ.get('CENSUS_API_KEY', 'CENSUS_API_KEY missing from .env file')
    url = 'https://api.census.gov/data/2023/acs/acs5'   # may need to update as new census completed 2028
    params = {
        'get':'B01003_001E',
        'for':'county:*',
        'in':f'state:{fips_state.filter(pl.col('state') == 'ARIZONA')['fips'].item()}',
        'key':CENSUS_API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        county_pop_dict = {'county':[], 'population':[]}
        counties = data[1:]
        for county in counties:
            population, state, county = county
            county_pop_dict['population'].append(int(population))
            county_pop_dict['county'].append(f'{state}{county}')
            county_pop_df = pl.DataFrame(county_pop_dict)
    else:
        sys.exit(f'error: {response.status_code}, {response.text}')

    pop = (
        county_pop_df.join(fips, left_on='county', right_on='fips', how='left', coalesce=True)
        .select(
            pl.col('county_right').str.to_uppercase().str.strip_suffix(' COUNTY').alias('county'),
            pl.col('population')
        )
    )
    print(pop)

    pat_county_rates = (
        rx_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('rx_count') / pl.col('population')) * pl.lit(1000)).alias('rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    opi_pat_county_rates = (
        opi_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('opi_rx_count') / pl.col('population')) * pl.lit(1000)).alias('opi_rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    benzo_pat_county_rates = (
        benzo_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('benzo_rx_count') / pl.col('population')) * pl.lit(1000)).alias('benzo_rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    stim_pat_county_rates = (
        stim_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('stim_rx_count') / pl.col('population')) * pl.lit(1000)).alias('stim_rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    bup_pat_county_rates = (
        bup_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('bup_rx_count') / pl.col('population')) * pl.lit(1000)).alias('bup_rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    andro_pat_county_rates = (
        andro_pat_county.join(pop, on='county', how='left', coalesce=True)
        .join(fips, on='county', how='left', coalesce=True)
        .with_columns(
            ((pl.col('andro_rx_count') / pl.col('population')) * pl.lit(1000)).alias('andro_rx_per1000'),
        )
        .rename(
            {'county':'patient_county'}
        )
    )

    opi_bup_benz_stim = (
        opi_pat_county_rates
        .join(bup_pat_county_rates, on=['patient_county', 'year_filled'], how='left', coalesce=True)
        .join(benzo_pat_county_rates, on=['patient_county', 'year_filled'], how='left', coalesce=True)
        .join(stim_pat_county_rates, on=['patient_county', 'year_filled'], how='left', coalesce=True)
        .join(andro_pat_county_rates, on=['patient_county', 'year_filled'], how='left', coalesce=True)
        .join(pat_county_rates, on=['patient_county', 'year_filled'], how='left', coalesce=True)
        .sort('year_filled')
    )
    opi_bup_benz_stim.write_clipboard()
    print('obbs written to clipboard')

    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=(
            'all cs', 'opioids', 'benzodiazepines', 'stimulants', 'buprenorphine', 'androgens'
        ),
        vertical_spacing=0.04,
        horizontal_spacing=.005,
        specs=[
            [{'type': 'geo'}, {'type': 'geo'}, {'type': 'geo'}],
            [{'type': 'geo'}, {'type': 'geo'}, {'type': 'geo'}]
        ]
    )

    metrics = [('rx_per1000','rx_count'), ('opi_rx_per1000','opi_rx_count'), ('benzo_rx_per1000','benzo_rx_count'), ('stim_rx_per1000','stim_rx_count'), ('bup_rx_per1000','bup_rx_count'), ('andro_rx_per1000','andro_rx_count')]
    positions = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2,3)]
    for (row, col), metric in zip(positions, metrics):
        obbs_year = opi_bup_benz_stim.filter(pl.col('year_filled') == pl.col('year_filled').min())
        fig.add_trace(go.Choropleth(
            geojson=counties,
            locations=obbs_year['fips'],
            z=obbs_year[metric[0]],
            marker_opacity=1,
            marker_line_width=0,
            showscale=(row == 1 and col == 1),
            colorbar_showticklabels=False,
            hovertext=obbs_year['patient_county'],
            hoverinfo="text+z",
            customdata=obbs_year[[metric[1], 'population']],
            hovertemplate=(
                '<b>%{hovertext}</b><br>'
                f'{metric[0]}: %{{z:.2f}}<br>'
                f'{metric[1]}: %{{customdata[0]:.3s}}<br>'
                'population: %{customdata[1]:.3s}<br>'
                '<extra></extra>'
            )
        ), row=row, col=col)

    frames = []
    for year in opi_bup_benz_stim['year_filled'].unique().sort():
        obbs_year = opi_bup_benz_stim.filter(pl.col('year_filled') == year)
        frame_data = []
        for metric in metrics:
            frame_data.append(go.Choropleth(
                locations=obbs_year['fips'],
                z=obbs_year[metric[0]],
                colorbar_showticklabels=False,
                customdata=obbs_year[[metric[1], 'population', 'year_filled']],
            ))
        frame = go.Frame(data=frame_data, name=str(year))
        frames.append(frame)

    fig.frames = frames

    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=10),
        updatemenus=[{
            'type': 'buttons',
            'showactive': True,
            'x': 0.1,
            'xanchor': 'left',
            'y': -0.05,
            'yanchor': 'top',
            'direction': 'left',
            'buttons': [
                {
                    'label': '▶',
                    'method': 'animate',
                    'args': [None, {'frame': {'duration': 1000, 'redraw': True}, 'fromcurrent': True}]
                },
                {
                    'label': '⏸',
                    'method': 'animate',
                    'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate'}]
                }
            ]
        }],
        sliders=[{
            'active': 0,
            'x': 0.2,
            'len': 0.8,
            'steps': [
                {
                    'args': [[frame.name], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate'}],
                    'label': frame.name,
                    'method': 'animate'
                }
                for frame in frames
            ],
            'transition': {'duration': 0},
            'currentvalue': {
                'prefix': 'year: ',
                'visible': True,
                'xanchor': 'right'
            }
        }]
    )

    for row, col in positions:
        fig.update_geos(projection_type='mercator', fitbounds='locations', row=row, col=col)

    fig.write_html('data/charts/county_map_combined.html', config={'displayModeBar':False} ,include_plotlyjs='cdn')

    # county_rate_line = px.line(opi_bup_benz_stim, x='year_filled', y='rx_per1000', color='patient_county', color_discrete_sequence=px.colors.qualitative.Light24, title='cs prescription rate by patient county')
    # county_rate_line.write_image('data/charts/county_rates.png')
    # county_rate_line.write_html('data/charts/county_rates.html', include_plotlyjs='cdn')

    opi_bup_county_rate_bubble = px.scatter(
        opi_bup_benz_stim,
        x='opi_rx_per1000',
        y='bup_rx_per1000',
        size='population',
        color='patient_county',
        color_discrete_sequence=px.colors.qualitative.Light24,
        animation_frame='year_filled',
        animation_group='patient_county',
        title='opioid vs buprenorphine prescription rate by patient county'
    )
    opi_bup_county_rate_bubble.update_traces(marker=dict(sizemin=5))
    opi_bup_county_rate_bubble.write_html('data/charts/opi_bup_county_rate_bubble.html', include_plotlyjs='cdn')

    # opi_rx_county_rate_bubble = px.scatter(
    #     opi_bup_benz_stim,
    #     x='rx_per1000',
    #     y='opi_rx_per1000',
    #     size='population',
    #     color='patient_county',
    #     color_discrete_sequence=px.colors.qualitative.Light24,
    #     animation_frame='year_filled',
    #     animation_group='patient_county',
    #     title='all cs vs opioid prescription rate by patient county'
    # )
    # opi_rx_county_rate_bubble.update_traces(marker=dict(sizemin=5))
    # opi_rx_county_rate_bubble.write_html('data/charts/opi_rx_county_rate_bubble.html', include_plotlyjs='cdn')
    #
    # county_rate_map = px.choropleth_map(
    #     data_frame=opi_bup_benz_stim,
    #     geojson=counties,
    #     locations='fips',
    #     color='rx_per1000',
    #     map_style='carto-positron',
    #     center = {"lat": 34.2744, "lon": -111.6602},
    #     opacity=1,
    #     zoom=5,
    #     hover_data={'patient_county':True, 'rx_per1000':':.2f', 'rx_count':':,d', 'population':':,d', 'fips':False},
    #     title='cs prescription rate by patient county',
    #     animation_frame='year_filled'
    # )
    # county_rate_map.write_html('data/charts/county_map.html', include_plotlyjs='cdn')
    #
    # opi_county_rate_map = px.choropleth_map(
    #     data_frame=opi_bup_benz_stim,
    #     geojson=counties,
    #     locations='fips',
    #     color='opi_rx_per1000',
    #     map_style='carto-positron',
    #     center = {"lat": 34.2744, "lon": -111.6602},
    #     opacity=1,
    #     zoom=5,
    #     hover_data={'patient_county':True, 'opi_rx_per1000':':.2f', 'rx_count':':,d', 'population':':,d', 'fips':False},
    #     title='opi prescription rate by patient county',
    #     animation_frame='year_filled'
    # )
    # opi_county_rate_map.write_html('data/charts/opi_county_map.html', include_plotlyjs='cdn')
    #
    # benzo_county_rate_map = px.choropleth_map(
    #     data_frame=opi_bup_benz_stim,
    #     geojson=counties,
    #     locations='fips',
    #     color='benzo_rx_per1000',
    #     map_style='carto-positron',
    #     center = {"lat": 34.2744, "lon": -111.6602},
    #     opacity=1,
    #     zoom=5,
    #     hover_data={'patient_county':True, 'benzo_rx_per1000':':.2f', 'rx_count':':,d', 'population':':,d', 'fips':False},
    #     title='benzo prescription rate by patient county',
    #     animation_frame='year_filled'
    # )
    # benzo_county_rate_map.write_html('data/charts/benzo_county_map.html', include_plotlyjs='cdn')
    #
    # stim_county_rate_map = px.choropleth_map(
    #     data_frame=opi_bup_benz_stim,
    #     geojson=counties,
    #     locations='fips',
    #     color='stim_rx_per1000',
    #     map_style='carto-positron',
    #     center = {"lat": 34.2744, "lon": -111.6602},
    #     opacity=1,
    #     zoom=5,
    #     hover_data={'patient_county':True, 'benzo_rx_per1000':':.2f', 'rx_count':':,d', 'population':':,d', 'fips':False},
    #     title='stim prescription rate by patient county',
    #     animation_frame='year_filled'
    # )
    # stim_county_rate_map.write_html('data/charts/stim_county_map.html', include_plotlyjs='cdn')
    #
    # bup_county_rate_map = px.choropleth_map(
    #     data_frame=opi_bup_benz_stim,
    #     geojson=counties,
    #     locations='fips',
    #     color='bup_rx_per1000',
    #     map_style='carto-positron',
    #     center = {"lat": 34.2744, "lon": -111.6602},
    #     opacity=1,
    #     zoom=5,
    #     hover_data={'patient_county':True, 'bup_rx_per1000':':.2f', 'rx_count':':,d', 'population':':,d', 'fips':False},
    #     title='buprenorphine prescription rate by patient county',
    #     animation_frame='year_filled'
    # )
    # bup_county_rate_map.write_html('data/charts/bup_county_map.html', include_plotlyjs='cdn')

    print('county data complete')

def cs_dispensed():
    # ---
    # cs dispensed
    # ---
    print('generating total cs charts...')

    total_cs = tableau.find_view_luid(view_name='Total CS Dispensed', workbook_name=WORKBOOK)

    cs_disp = (
        tableau.lazyframe_from_view_id(total_cs, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count')
        )
        .sort('year_filled')
    )

    cs_disp_line = px.line(cs_disp, x='year_filled', y='rx_count', title='cs dispensations', range_y=[0,22000000])
    cs_disp_line.write_image('data/charts/total_cs.png')
    cs_disp_line.write_html('data/charts/total_cs.html', include_plotlyjs='cdn')
    print('total cs charts complete')


def cs_by_sched():
    # ---
    # cs by sched
    # ---

    print('generating cs by sched...')

    total_cs_sched = tableau.find_view_luid(view_name='Total CS Drug schedule', workbook_name=WORKBOOK)

    cs_disp_sched = (
        tableau.lazyframe_from_view_id(total_cs_sched, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.col('Drug Schedule').alias('drug_schedule')
        )
        .sort('year_filled')
    )

    cs_disp_sched_tree_map = px.treemap(cs_disp_sched, path=[px.Constant('all drugs'), 'year_filled', 'drug_schedule'], values='rx_count', color='drug_schedule')
    cs_disp_sched_tree_map.update_traces(marker=dict(cornerradius=5))
    cs_disp_sched_tree_map.write_html('data/charts/cs_disp_sched_tree_map.html', include_plotlyjs='cdn')
    print('cs by sched complete')

def obs():
    # ---
    # opi, benzo, stims
    # ---
    print('generating opi benzo stims...')

    opi_dispensed = tableau.find_view_luid(view_name='Opi Dispensed', workbook_name=WORKBOOK)
    benzo_dispensed = tableau.find_view_luid(view_name='Benzo Dispensed', workbook_name=WORKBOOK)
    stim_dispensed = tableau.find_view_luid(view_name='Stimulant Dispensed', workbook_name=WORKBOOK)

    opi = (
        tableau.lazyframe_from_view_id(opi_dispensed, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.lit('opioid').alias('drug')
        )
        .sort('year_filled')
    )

    benzo = (
        tableau.lazyframe_from_view_id(benzo_dispensed, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.lit('benzodiazepine').alias('drug')
        )
        .sort('year_filled')
    )

    stims = (
        tableau.lazyframe_from_view_id(stim_dispensed, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.lit('stimulant').alias('drug')
        )
        .sort('year_filled')
    )

    x1 = benzo['year_filled'].max()
    x0 = x1 - 1
    y0 = benzo.filter(pl.col('year_filled') == (pl.col('year_filled').max() - 1))['rx_count'].item()
    y1 = benzo.filter(pl.col('year_filled') == pl.col('year_filled').max())['rx_count'].item()
    delta = human_format(y1 - y0)

    layout = dict(
        hoversubplots='axis',
        title=dict(text='dispensations by drug type'),
        hovermode='x',
        grid=dict(rows=3, columns=1),
        shapes=[
            dict(type='rect',
                x0=x0, y0=y0, x1=x1, y1=y1,
                fillcolor='MediumPurple',
                line_color='MediumPurple',
                opacity=0.25,
                xref='x', yref='y2'
            )
        ],
        annotations=[
            dict(
                x=x1 - 0.01,
                y=y0 + (y1 - y0) * 0.03,
                text=f'benzodiazepine increase: {delta}',
                showarrow=False,
                font=dict(color='MediumPurple', size=10),
                align='right',
                xanchor='right',
                yanchor='bottom',
                xref='x', yref='y2'
            )
        ]
    )

    data = [
        go.Scatter(x=opi['year_filled'], y = opi['rx_count'], xaxis='x', yaxis='y', name='opioid', hovertemplate='%{y:.3s}'),
        go.Scatter(x=benzo['year_filled'], y = benzo['rx_count'], xaxis='x', yaxis='y2', name='benzodiazepine', hovertemplate='%{y:.3s}'),
        go.Scatter(x=stims['year_filled'], y = stims['rx_count'], xaxis='x', yaxis='y3', name='stimulant', hovertemplate='%{y:.3s}'),
    ]

    obs_stacked = go.Figure(data=data, layout=layout)

    obs_stacked.write_image('data/charts/obs_stacked.png')
    obs_stacked.write_html('data/charts/obs_stacked.html')
    print('opi benzo stims generated')

def benzo_oos():
    # ---
    # benzo oos
    # ---
    print('generating benzo oos...')

    benzo_disp_oos = tableau.find_view_luid(view_name='Benzo AZ?', workbook_name=WORKBOOK)

    benzo_oos = (
        tableau.lazyframe_from_view_id(benzo_disp_oos, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.col('Prescriber AZ ?').alias('presc_az')
        )
        .sort('year_filled')
    )

    benzo_oos_fig = px.line(benzo_oos, x='year_filled', y='rx_count', color='presc_az', title='benzodiazepine dispensations by prescriber state', hover_data={'presc_az':True, 'year_filled':True, 'rx_count':':.3s'})
    # benzo_oos_fig.add_vrect(x0=benzo_oos['year_filled'].max() - 1, x1=benzo_oos['year_filled'].max(),
    #                         annotation_text='increase in out of state rx', annotation_position='bottom right',
    #                         fillcolor='red', opacity=0.25, line_width=0)
    layout = dict(
        hoversubplots='axis',
        hovermode='x'
    )
    benzo_oos_fig.update_layout(layout)

    x1 = benzo_oos['year_filled'].max()
    x0 = x1 - 1
    y0 = benzo_oos.filter((pl.col('year_filled') == (pl.col('year_filled').max() - 1)) & pl.col('presc_az').not_())['rx_count'].item()
    y1 = benzo_oos.filter((pl.col('year_filled') == pl.col('year_filled').max()) & pl.col('presc_az').not_())['rx_count'].item()

    delta = human_format(y1 - y0)

    benzo_oos_fig.add_shape(type='rect',
        x0=x0, y0=y0, x1=x1, y1=y1,
        fillcolor='MediumPurple',
        line_color='MediumPurple',
        opacity=0.25
    )
    benzo_oos_fig.update_shapes(dict(xref='x', yref='y'))
    benzo_oos_fig.add_annotation(
        x=x1 - 0.01,
        y=y0 + (y1 - y0) * 0.03,
        text=f'out of state increase: {delta}',
        showarrow=False,
        font=dict(color='MediumPurple', size=10),
        align='right',
        xanchor='right',
        yanchor='bottom',
    )
    benzo_oos_fig.write_html('data/charts/benzo_oos.html', include_plotlyjs='cdn')
    benzo_oos_fig.write_image('data/charts/benzo_oos.png')

    print('benzo oos complete')

def andro_oos():
    # ---
    # andro oos
    # ---
    print('generating andro oos...')

    andro_disp_oos = tableau.find_view_luid(view_name='Androgens AZ?', workbook_name=WORKBOOK)

    andro_oos = (
        tableau.lazyframe_from_view_id(andro_disp_oos, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.col('Prescriber AZ ?').alias('presc_az')
        )
        .sort('year_filled')
    )

    andro_oos_fig = px.line(andro_oos, x='year_filled', y='rx_count', color='presc_az', title='androgen dispensations by prescriber state', hover_data={'presc_az':True, 'year_filled':True, 'rx_count':':.3s'})
    layout = dict(
        hoversubplots='axis',
        hovermode='x'
    )
    andro_oos_fig.update_layout(layout)

    x1 = andro_oos['year_filled'].max()
    x0 = x1 - 1
    y0 = andro_oos.filter((pl.col('year_filled') == (pl.col('year_filled').max() - 1)) & pl.col('presc_az').not_())['rx_count'].item()
    y1 = andro_oos.filter((pl.col('year_filled') == pl.col('year_filled').max()) & pl.col('presc_az').not_())['rx_count'].item()
    y2 = andro_oos.filter((pl.col('year_filled') == (pl.col('year_filled').max() - 1)) & pl.col('presc_az'))['rx_count'].item()
    y3 = andro_oos.filter((pl.col('year_filled') == pl.col('year_filled').max()) & pl.col('presc_az'))['rx_count'].item()

    delta = human_format(y1 - y0)
    delta2 = human_format(y3 - y2)

    andro_oos_fig.add_vrect(x0=x0, x1=x1, fillcolor='orange', opacity=0.25, line_width=0, annotation_text=f'out of state increase: {delta}', annotation_position='bottom right', annotation_font_color='red', annotation_font_size=10)
    andro_oos_fig.add_vrect(x0=x0, x1=x1, fillcolor='orange', opacity=0, line_width=0, annotation_text=f'in state state increase: {delta2}', annotation_position='top left', annotation_font_color='green', annotation_font_size=10)
    andro_oos_fig.write_html('data/charts/andro_oos.html', include_plotlyjs='cdn')
    andro_oos_fig.write_image('data/charts/andro_oos.png')

    print('andro oos complete')

def bup():
    # ---
    # buprenorphine
    # ---
    print('generating buprenorphine...')

    bup_dispensed = tableau.find_view_luid(view_name='Bup Dispensed', workbook_name=WORKBOOK)

    bup_rx = (
        tableau.lazyframe_from_view_id(bup_dispensed, infer_schema_length=100).collect()
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
        )
        .sort('year_filled')
    )

    bup_fig = px.line(bup_rx, x='year_filled', y='rx_count', title='buprenorphine dispensations by year', hover_data={'year_filled':True, 'rx_count':':.3s'})
    bup_fig.write_html('data/charts/bup.html', include_plotlyjs='cdn')
    bup_fig.write_image('data/charts/bup.png')

    print('buprenorphine complete')

def opi_pills():
    print('generating opi_pills...')
    opi_pills = tableau.find_view_luid(view_name='Opi Pills Dispensed', workbook_name=WORKBOOK)

    pills = (
        tableau.lazyframe_from_view_id(opi_pills, infer_schema_length=100)
        .select(
            pl.col('Year of Filled At').alias('year_filled'),
            pl.col('Prescription Count').str.replace_all(',','').cast(pl.Int32).alias('rx_count'),
            pl.col('Quantity').str.replace_all(',','').cast(pl.Float32).round(0).alias('pills_count'),
        )
        .with_columns(
            (pl.col('pills_count') / pl.col('rx_count')).alias('pills_per_rx')
        )
        .sort('year_filled')
        .collect()
    )

    opi_pp = px.line(pills, x='year_filled', y='pills_per_rx', title='opioid pills per dispensation', hover_data={'year_filled':True, 'pills_per_rx':':.3s'})
    opi_pp.write_html('data/charts/opi_pp.html', include_plotlyjs='cdn')

    layout = dict(
        hoversubplots='axis',
        title=dict(text='opioid pills and dispensations'),
        hovermode='x',
        grid=dict(rows=3, columns=1)
    )

    data = [
        go.Scatter(x=pills['year_filled'], y = pills['rx_count'], xaxis='x', yaxis='y', name='dispensations', hovertemplate='%{y:.3s}'),
        go.Scatter(x=pills['year_filled'], y = pills['pills_count'], xaxis='x', yaxis='y2', name='pills', hovertemplate='%{y:.3s}'),
        go.Scatter(x=pills['year_filled'], y = pills['pills_per_rx'], xaxis='x', yaxis='y3', name='pills per dispensation', hovertemplate='%{y:.3s}'),
    ]

    opi_pills_per = go.Figure(data=data, layout=layout)
    opi_pills_per.write_html('data/charts/opi_pp_stacked.html', include_plotlyjs='cdn')
    print('opi_pills complete')



def main():
    county_data()
    cs_dispensed()
    # cs_by_sched() # not interesting this year
    obs()
    benzo_oos()
    andro_oos()
    bup()
    opi_pills()

if __name__  == '__main__':
    main()
