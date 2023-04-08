import os
import time
import math
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from google.oauth2 import service_account
from google.cloud import bigquery


credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


st.set_page_config(
  page_title="Product Telemetry Dashboard",
  page_icon=":bar_chart:",
  layout="wide"                 
)

# Set default start and end dates
first_date='2023-03-09'
current_date=datetime.today().strftime('%Y-%m-%d')

# Sidebar
st.sidebar.header("Telemetry Statistics")
main_level = st.sidebar.selectbox(
		"Choose option",
		options=["Usage statistics","Parameter statistics"]
	)

start_date = st.sidebar.date_input(
	label='start date',
	value=datetime.strptime(first_date,'%Y-%m-%d').date(),
	min_value=datetime.strptime(first_date,'%Y-%m-%d').date()
)

end_date = st.sidebar.date_input(
	label='end date',
	value=datetime.today(),
	min_value=start_date,
)

dateidx_df = pd.DataFrame(index=pd.date_range(start_date,end_date))
weekends = [date for date in dateidx_df.index if date.weekday() in [5,6]]

first_sunday = False
if len(weekends):
	first_sunday = True if weekends[0].weekday() == 6 else False

weekend_ranges = [[] for _ in range(math.ceil(len(weekends)/2))]
if first_sunday:
	weekend_ranges[0].append(weekends[0])

for idx, date in enumerate(weekends):
	if first_sunday:
		if idx==0:
			continue
		weekend_ranges[int((idx-1)/2)+1].append(date)
	else:
	  weekend_ranges[int(idx/2)].append(date)

@st.cache_data(ttl=60*60*24)
def get_data() -> pd.DataFrame:
	data_dict = {
		'pipeline_extract': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_deploy`",
													credentials=credentials
												)	,
		'pipeline_load': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.pipeline_load`",
													credentials=credentials
												)	,
		'pipeline_normalize': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.pipeline_normalize`",
													credentials=credentials
												)	,
		'pipeline_run': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.pipeline_run`",
													credentials=credentials
												)	,
		'command_deploy': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_deploy`",
													credentials=credentials
												)	,
		'command_init': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_init`",
													credentials=credentials
												)	,
		'command_list_pipelines': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_list_pipelines`",
													credentials=credentials
												)	,
		'command_pipeline': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_pipeline`",
													credentials=credentials
												)	,
		'command_telemetry': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_telemetry`",
													credentials=credentials
												)	,
		'command_telemetry_switch': pd.read_gbq(
													"select * from `dlthub-analytics.python_dlt.command_telemetry_switch`",
													credentials=credentials
												)	,
	}	
	# data_dict = {
	# 	'pipeline_extract': pd.read_csv(os.path.join('data','pipeline_extract.csv')),
	# 	'pipeline_load': pd.read_csv(os.path.join('data','pipeline_load.csv')),
	# 	'pipeline_normalize': pd.read_csv(os.path.join('data','pipeline_normalize.csv')),
	# 	'pipeline_run': pd.read_csv(os.path.join('data','pipeline_run.csv')),
	# 	'command_deploy': pd.read_csv(os.path.join('data','command_deploy.csv')),
	# 	'command_init': pd.read_csv(os.path.join('data','command_init.csv')),
	# 	'command_list_pipelines': pd.read_csv(os.path.join('data','command_list_pipelines.csv')),
	# 	'command_pipeline': pd.read_csv(os.path.join('data','command_pipeline.csv')),
	# 	'command_telemetry': pd.read_csv(os.path.join('data','command_telemetry.csv')),
	# 	'command_telemetry_switch': pd.read_csv(os.path.join('data','command_telemetry_switch.csv'))
	# }	

	df = pd.concat(data_dict[key] for key in data_dict.keys())
	df['date'] = pd.to_datetime(df['timestamp']).dt.date
	df['idx'] = df.apply(
		lambda row: row['transaction_id'] if row['event_category']=='pipeline' else row['id'], 
		axis=1
	)
	return df.reset_index()

complete_df = get_data()


if main_level == "Usage statistics":
	st.header("Usage Statistics")

	main_df = complete_df[complete_df['date'] < end_date][complete_df['date'] > start_date]
	pipeline_df = main_df[main_df["event_category"]=="pipeline"]
	command_df = main_df[main_df["event_category"]=="command"]

	statistics_table_dict = {
		"Number of calls": {
			"total": main_df['idx'].nunique(),
			"pipeline": pipeline_df['idx'].nunique(),
			"command": command_df['idx'].nunique()
		}
	}
	index_order_dict = {"total": 0, "pipeline": 1, "command": 2}
	statistics_table = pd.DataFrame(statistics_table_dict).sort_index(
		key=lambda x: x.map(index_order_dict)
	)
	st.sidebar.markdown("#### dlt usage statistics")
	st.sidebar.table(statistics_table)

	left_column, right_column = st.columns(2)

	df_selection = main_df.groupby('date')['idx'].nunique()
	df_selection = dateidx_df.join(df_selection,how='left').fillna(0) \
				     	.reset_index().rename(columns={'index':'date'})
	fig_total_timeseries = px.line(
 		df_selection,
 		x='date',
 		y='idx',
 		title='Daily dlt usage since the start of product telemetry',
 		width=515,
 		height=500,
 		labels={
 			"idx": "number of dlt calls"
 		}
 	)

	for weekend_range in weekend_ranges:
		if len(weekend_range) == 2:
		    fig_total_timeseries.add_vrect(
		        x0=weekend_range[0]-pd.Timedelta(hours=12),
		        x1=weekend_range[1]+pd.Timedelta(hours=12),
		        fillcolor="black",
		        opacity=0.1,
		        line_width=0
		    )
		else:
		    fig_total_timeseries.add_vrect(
		        x0=weekend_range[0]-pd.Timedelta(hours=12),
		        x1=weekend_range[0]+pd.Timedelta(hours=12),
		        fillcolor="black",
		        opacity=0.1,
		        line_width=0
		    )

	left_column.plotly_chart(fig_total_timeseries)

	main_df['docker'] = main_df['context_exec_info'].apply(lambda x: 'docker' in x)
	main_df['codespaces'] = main_df['context_exec_info'].apply(lambda x: 'codespaces' in x)
	main_df['notebook'] = main_df['context_exec_info'].apply(lambda x: 'notebook' in x)
	main_df['colab'] = main_df['context_exec_info'].apply(lambda x: 'colab' in x)


	docker_df = pd.DataFrame(main_df[main_df['docker']])
	if len(docker_df):
		docker_df = docker_df.groupby('date')['idx'].nunique()
	docker_df = dateidx_df.join(docker_df,how='left').fillna(0).rename(columns={'idx':'docker'})

	codespaces_df = pd.DataFrame(main_df[main_df['codespaces']])
	if len(codespaces_df):
		codespaces_df = codespaces_df.groupby('date')['idx'].nunique()
	codespaces_df = dateidx_df.join(codespaces_df,how='left').fillna(0).rename(columns={'idx':'codespaces'})

	notebook_df = pd.DataFrame(main_df[main_df['notebook']])
	if len(notebook_df):
		notebook_df = notebook_df.groupby('date')['idx'].nunique()
	notebook_df = dateidx_df.join(notebook_df,how='left').fillna(0).rename(columns={'idx':'notebook'})

	colab_df = pd.DataFrame(main_df[main_df['colab']])
	if len(colab_df):
		colab_df = colab_df.groupby('date')['idx'].nunique()
	colab_df = dateidx_df.join(colab_df,how='left').fillna(0).rename(columns={'idx':'colab'})

	environment_df = pd.concat([docker_df, codespaces_df, notebook_df, colab_df], axis=1)

	fig_environment_timeseries = px.line(
			environment_df,
			# x='index',
			# y='value',
			title='Daily dlt usage for each environemnt',
			width=515,
			height=500,
			labels={
				"index": "date",
				"value": "number of dlt calls"
			}
	)
	fig_environment_timeseries.update_layout(
		legend = dict(
			orientation="h",
			entrywidth=70,
			yanchor="bottom",
			y=1.0,
			xanchor="right",
			x=1.0,
			title=None
		)
	)
	right_column.plotly_chart(fig_environment_timeseries)

	left_column, right_column = st.columns(2)

	pipeline_df_selection = pipeline_df.groupby('date')['idx'].nunique()
	pipeline_df_selection = dateidx_df.join(pipeline_df_selection,how='left').fillna(0) \
				     			.rename(columns={'idx':'pipeline'})	
	command_df_selection = command_df.groupby('date')['idx'].nunique()
	command_df_selection = dateidx_df.join(command_df_selection,how='left').fillna(0) \
				     			.rename(columns={'idx':'command_line'})	
	pipeline_command_df = pd.concat([pipeline_df_selection,command_df_selection],axis=1)

	fig_pipeline_command_timeseries = px.line(
		pipeline_command_df,
		title="Daily number of pipeline and command line calls",
		width=515,
		height=500,
		labels={
			"index":"date",
			"value":"number of dlt calls"
		}
	)
	fig_pipeline_command_timeseries.update_layout(
		legend = dict(
			orientation="h",
			entrywidth=70,
			yanchor="bottom",
			y=1.0,
			xanchor="right",
			x=1.0,
			title=None
		)
	)

	for weekend_range in weekend_ranges:
		if len(weekend_range) == 2:
		    fig_pipeline_command_timeseries.add_vrect(
		        x0=weekend_range[0]-pd.Timedelta(hours=12),
		        x1=weekend_range[1]+pd.Timedelta(hours=12),
		        fillcolor="black",
		        opacity=0.1,
		        line_width=0
		    )
		else:
		    fig_pipeline_command_timeseries.add_vrect(
		        x0=weekend_range[0]-pd.Timedelta(hours=12),
		        x1=weekend_range[0]+pd.Timedelta(hours=12),
		        fillcolor="black",
		        opacity=0.1,
		        line_width=0
		    )
	left_column.plotly_chart(fig_pipeline_command_timeseries)

	pipeline_command_df['weekend'] = pipeline_command_df.index.map(
		lambda date: 'Weekend' if date.weekday() in [5,6] else 'Weekday'
	)
	run_type_df = pipeline_command_df.groupby('weekend').sum().T\
					.reset_index().rename(columns={"index":"run_type"})

	fig_weekend_bar = px.bar(
		run_type_df,
		x='run_type',
		y=['Weekday','Weekend'],
		barmode='group',
		width=515,
		height=500,
		title='Comparison of pipeline and command line calls over weekends and weekdays',
		labels={
			"value": "number of dlt calls"
		},
	)
	fig_weekend_bar.update_layout(
		legend = dict(
			orientation="h",
			entrywidth=70,
			yanchor="bottom",
			y=1.0,
			xanchor="right",
			x=1.0,
			title=None
		)
	)
	right_column.plotly_chart(fig_weekend_bar)



if main_level == "Parameter statistics":
	st.header("Parameter Statistics")
	main_df = complete_df[complete_df['date'] < end_date][complete_df['date'] > start_date]
	main_df['destination_name'] = main_df['destination_name'].replace('duckdb7','duckdb')

	df_selection = main_df.groupby(['date','destination_name'])['idx'].nunique().reset_index()

	duckdb_df = dateidx_df.join(df_selection[df_selection['destination_name']=='duckdb'] \
					.set_index('date'),how='left').fillna(0).rename(columns={'idx':'duckdb'})
	bigquery_df = dateidx_df.join(df_selection[df_selection['destination_name']=='bigquery'] \
					.set_index('date'),how='left').fillna(0).rename(columns={'idx':'bigquery'})
	postgres_df = dateidx_df.join(df_selection[df_selection['destination_name']=='postgres'] \
					.set_index('date'),how='left').fillna(0).rename(columns={'idx':'postgres'})
	redshift_df = dateidx_df.join(df_selection[df_selection['destination_name']=='redshift'] \
					.set_index('date'),how='left').fillna(0).rename(columns={'idx':'redshift'})

	destinations_df = pd.concat(
		[duckdb_df,bigquery_df,postgres_df,redshift_df],axis=1
	)[['duckdb','bigquery','postgres','redshift']].reset_index().rename(columns={'index':'date'})

	fig_destinations_timeseries = px.line(
		destinations_df,
		x='date',
		y=['duckdb','bigquery','postgres','redshift'],
		title="Daily usage of destinations in dlt calls",
		width=515,
		height=500,
		labels={
			"index":"date",
			"value":"number of dlt calls"
		}
	)

	fig_destinations_timeseries.update_layout(
		legend = dict(
			orientation="h",
			entrywidth=70,
			yanchor="bottom",
			y=1.0,
			xanchor="right",
			x=1.0,
			title=None
		)
	)

	fig_destinations_bar = px.bar(
		destinations_df.sum(), 
		orientation="h",
		title="Comparison of the usage of different destinations",
		width=515,
		height=500,
		labels={
			"index":"destinations",
			"value":"number of dlt calls"
		}
	)

	fig_destinations_bar.update_layout(
		yaxis = dict(
			categoryorder="total ascending"
		),
		showlegend=False
	)

	st.sidebar.markdown("#### dlt calls by destinations")

	destinations_sum_table = pd.DataFrame(destinations_df.sum())
	destinations_sum_table.columns = ["Number of calls"]

	total_dlt_calls_table = pd.DataFrame({"Number of calls": {"total": main_df['idx'].nunique()}})
	destinations_sum_table = pd.concat([total_dlt_calls_table, destinations_sum_table])

	destinations_sum_table["Number of calls"] = destinations_sum_table["Number of calls"].map(int)
	st.sidebar.table(destinations_sum_table)

	left_column, right_column = st.columns(2)
	left_column.plotly_chart(fig_destinations_timeseries)
	right_column.plotly_chart(fig_destinations_bar)

	main_df = complete_df[complete_df['date'] < end_date][complete_df['date'] > start_date]
	main_df = main_df[main_df['event_category']=="command"]
	df_selection = main_df.groupby('event_name')['idx'].nunique().reset_index()\
						  .sort_values(by='idx',ascending=False)
	fig_command_name_bar = px.bar(
		df_selection, x='event_name', y='idx'
	)
	fig_command_name_bar = px.pie(
		df_selection, values='idx', names='event_name',
		title="Distribution of different dlt commands"
	)

	init_df = main_df[main_df['event_name']=='init']
	top_5_sources = init_df['pipeline_name'].value_counts().head().index
	init_df['source'] = init_df['pipeline_name'].apply(
		lambda x: x if x in top_5_sources else 'other'
	)

	df_selection = init_df['source'].value_counts()
	df_selection = df_selection.reset_index().rename(columns={'index':'source','source':'frequency'})
	fig_source_distribution = px.bar(
		df_selection, x='source', y='frequency',
		title="Comparison of the usage of top dlt sources in the init command",
		labels={
			"frequency": "number of dlt calls"
		}
	)
	fig_source_distribution.update_layout(
		xaxis={
			'categoryorder': 'array',
			'categoryarray': [
				*[value for value in df_selection['source'].values if value != "other"], "other"
			]
		}
	)

	st.sidebar.markdown("#### dlt init calls by source")
	df_selection_sum = df_selection.sum().reset_index().loc[1:1]
	df_selection_sum = df_selection_sum.rename(columns={"index": "source", 0: "frequency"})
	df_selection_sum["source"] = "total init calls"
	df_selection_combined = pd.concat([df_selection_sum,df_selection])
	index_order_dict = {
		"total init calls": 0,
		"chess": 1,
		"strapi": 2,
		"pipedrive": 3,
		"github": 4,
		"google_sheets": 5,
		"other": 6
	}
	df_selection_combined = df_selection_combined.sort_values(
		by="source", key=lambda x: x.map(index_order_dict)
	)
	df_selection_combined.columns = ["source", "Number of calls"]
	st.sidebar.table(df_selection_combined.set_index("source"))

	left_column, right_column = st.columns(2)
	left_column.plotly_chart(fig_command_name_bar)
	right_column.plotly_chart(fig_source_distribution)

