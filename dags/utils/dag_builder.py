import airflow
import json
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

class DagBuilder:

	def __init__(self, event_name):
		self.EXECUTION_TIMEOUT=timedelta(days=12)
		self.event_name = event_name
		self.s3_bucket = Variable.get("s3_bucket")
		self.jobs_s3_path = Variable.get("jobs_s3_path")
		self.emr_log_uri = Variable.get("emr_log_uri")
		self.ec2_subnet_id = Variable.get("ec2_subnet_id")
		self.airflow_params = Variable.get(f"{self.event_name}_parameters", deserialize_json=True, default_var='undefined')


	def get_run_dates(self):
		"""
		# expecting one of 4 options to be passed
		# yesterday - obvious, returns yesterday's date
		# early_date::later_date - :: returns list of each day starting from early_date going through later_date (or vice versa)
		# date1,date2,date3,etc - , returns list of each date in the comma-separated list
		# date1 - returns the single date
		#
		# date format passed should be YYYY-MM-DD
		"""
		param_dates = self.airflow_params['run_dates']
		dates = []

		if param_dates == 'yesterday':
			yesterday = datetime.now() - timedelta(days=1)
			dates.append({
				'year': str(yesterday.year),
				'month': str('%02d'%yesterday.month),
				'day': str('%02d'%yesterday.day)
			})

		elif '::' in param_dates:
			date_split = param_dates.split('::')
			if date_split[0] > date_split[1]:
				latest = datetime.strptime(date_split[0], '%Y-%m-%d')
				earliest = datetime.strptime(date_split[1], '%Y-%m-%d')
			else:
				latest = datetime.strptime(date_split[1], '%Y-%m-%d')
				earliest = datetime.strptime(date_split[0], '%Y-%m-%d')
			date_delta = (latest - earliest).days
			for i in range(date_delta + 1):
				new_date = (latest - timedelta(days=i)).strftime('%Y-%m-%d')
				requested_date_array = new_date.strip().split('-')
				dates.append({
					'year': requested_date_array[0],
					'month': requested_date_array[1],
					'day': requested_date_array[2]
				})

		elif ',' in param_dates:
			for param_date in param_dates.split(','):
				requested_date_array = param_date.strip().split('-')
				dates.append({
					'year': requested_date_array[0],
					'month': requested_date_array[1],
					'day': requested_date_array[2]
				})

		elif '-' in param_dates:
			requested_date_array = param_dates.strip().split('-')
			dates.append({
				'year': requested_date_array[0],
				'month': requested_date_array[1],
				'day': requested_date_array[2]
				})
		else:
			yesterday = datetime.now() - timedelta(days=1)
			dates.append({
				'year': str(yesterday.year),
				'month': str('%02d'%yesterday.month),
				'day': str('%02d'%yesterday.day)
			})

		return dates


	def build_default_args(self):
		return {
			'owner': 'rt-data',
			'start_date': airflow.utils.dates.days_ago(2),
			'email': ['daniel.soleiman@roosterteeth.com', 'dave@roosterteeth.com'],
			'email_on_failure': True
		}


	def build_overrides(self, master_count=1, core_count=2, instance_type='m5.xlarge'):
		# protect against invalid instance types
		if instance_type not in self.get_valid_instance_types():
			instance_type = 'm5.xlarge'

		return {
			'Name': f'{self.event_name}_cluster',
			'ReleaseLabel': 'emr-5.30.1',
			'LogUri': f's3n://{self.emr_log_uri}',
			'Applications': [
			{
				'Name': 'Spark'
			},
		],
			'Instances': {
				'Ec2SubnetId': self.ec2_subnet_id,
				'InstanceGroups': [
					{
						'Name': "Master nodes",
						'Market': 'ON_DEMAND',
						'InstanceRole': 'MASTER',
						'InstanceType': instance_type,
						'InstanceCount': master_count,
					},
					{
						'Name': "Slave nodes",
						'Market': 'ON_DEMAND',
						'InstanceRole': 'CORE',
						'InstanceType': instance_type,
						'InstanceCount': core_count,
					}
				],
				'KeepJobFlowAliveWhenNoSteps': False,
				'TerminationProtected': False
			},
			'VisibleToAllUsers': True,
			'JobFlowRole': 'EMR_EC2_DefaultRole',
			'ServiceRole': 'EMR_DefaultRole'
		}


	def build_steps(self, run_date_overrides = None):
		if run_date_overrides:
			run_dates = run_date_overrides
		else:
			run_dates = self.get_run_dates()
		steps = []
		for date in run_dates:
			steps.append({
				'Name': self.event_name,
				'ActionOnFailure': 'TERMINATE_CLUSTER',
				'HadoopJarStep': {
					'Jar': 'command-runner.jar',
					'Args': ['spark-submit', f'{self.jobs_s3_path}/{self.event_name}.py', date['year'], date['month'], date['day']],
				},
			})
		return steps


	def build_columns(self, column_list):
		return {
			'select_columns': (',').join([f"s.{x}" for x in column_list]),
			'insert_columns': (',').join([f'"{x}"' for x in column_list]),
			'original_columns': column_list
		}


	def build_event_jobs(self, event_names, job_path):
		output = {}
		for event in event_names:
			file = f'{job_path}/{event}.json'
			with open(file, 'r') as f:
				output[event] = json.load(f)

		return output


	def get_valid_instance_types(self):
		return [
			'm1.small',
			'm1.medium',
			'm1.large',
			'm1.xlarge',
			'm2.xlarge',
			'm2.2xlarge',
			'm2.4xlarge',
			'm3.xlarge',
			'm3.2xlarge',
			'm4.large',
			'm4.xlarge',
			'm4.2xlarge',
			'm4.4xlarge',
			'm4.10xlarge',
			'm4.16xlarge',
			'm5.xlarge',
			'm5.2xlarge',
			'm5.4xlarge',
			'm5.8xlarge',
			'm5.12xlarge',
			'm5.16xlarge',
			'm5.24xlarge',
			'm5a.xlarge',
			'm5a.2xlarge',
			'm5a.4xlarge',
			'm5a.8xlarge',
			'm5a.12xlarge',
			'm5a.16xlarge',
			'm5a.24xlarge',
			'm5d.xlarge',
			'm5d.2xlarge',
			'm5d.4xlarge',
			'm5d.8xlarge',
			'm5d.12xlarge',
			'm5d.16xlarge',
			'm5d.24xlarge',
			'm5zn.xlarge',
			'm5zn.2xlarge',
			'm5zn.3xlarge',
			'm5zn.6xlarge',
			'm5zn.12xlarge',
			'm6g.xlarge',
			'm6g.2xlarge',
			'm6g.4xlarge',
			'm6g.8xlarge',
			'm6g.12xlarge',
			'm6g.16xlarge',
			'm6gd.xlarge',
			'm6gd.2xlarge',
			'm6gd.4xlarge',
			'm6gd.8xlarge',
			'm6gd.12xlarge',
			'm6gd.16xlarge'
		]
