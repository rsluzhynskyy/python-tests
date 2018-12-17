import boto3
import sys
import click
import botocore


session = None

# session = boto3.Session(profile_name="default")
# ec2 = session.resource('ec2')

@click.group()
@click.option('--profile', default=None, help="Specify AWS profile. OPTIONAL")
def cli(profile):
    """Manages snapshots"""
    global session
    if profile is None:
        profile = 'default'
    session = boto3.Session(profile_name=profile)


def get_instances(project):
    ec2 = session.resource('ec2')    
    instances = []
    if project:
    	filters = [{'Name': 'tag:Project', 'Values':[project]}]
    	instances = ec2.instances.filter(Filters=filters)
    else:
    	instances = ec2.instances.all()

    return instances
         

def has_pending_snapshot(volume):
    snapshots = list(volume.snapshots.all())
    return snapshots and snapshots[0].state == 'pending'


@cli.group('volumes')
def volumes():
	"""Commands for instances"""
@volumes.command('list')
@click.option('--project', default=None,
	help="Only instances for project (tag Project:<name>)")

def list_volumes(project):
	"List volumes"
	instances = get_instances(project)
	for i in instances:
		for v in i.volumes.all():
			print(', '.join((
				v.id,
                v.state,
                str(v.size),
                str(v.create_time)
				)))
	return 


@cli.group('snapshots')
def snapshots():
	"""Commands for snapshots"""
@snapshots.command('list')
@click.option('--project', default=None,
	help="Only instances for project (tag Project:<name>)")
@click.option('--all', 'list_all', default=False, is_flag=True,
	help="List all snapshots, not just most recent")

def list_snapshots(project,list_all):
    "List EC2 snapshots"
    instances = get_instances(project)
    for i in instances:
        for v in i.volumes.all():
            for s in v.snapshots.all():
                print(', '.join((
                	s.id,
                	s.volume_id,
                	s.progress,
                	s.state,
                	s.start_time.strftime("%c")
                	)))

                if s.state == 'completed' and not list_all: break



@cli.group('instances')
def instances():
	"""Commands for instances"""

@instances.command('list')
@click.option('--project', default=None,
	help="Only instances for project (tag Project:<name>)")

def list_instances(project):
    "List EC2 instances"
    instances = get_instances(project)

    for i in instances:
    	tags = { t['Key']: t['Value'] for t in i.tags or [] }
    	print(', '.join((
            i.id,
            i.instance_type,
            i.placement['AvailabilityZone'],
            i.state['Name'],
            i.public_dns_name,
            tags.get('Project', '<no project>')
	    	)))
    return


@instances.command('stop')
@click.option('--project', default=None,
	help="Only instances for project (tag Project:<name>)")

@click.option('--force', 'force', default=False, is_flag=True,
    help="Force reboot")

def stop_instances(project,force):
    "Stop EC2 instances"
    instances = get_instances(project)

    if not force and not project:
        print("ERR: You need to specify --force or --project option in order to reboot instances")
        exit(2)

    for i in instances:
    	print("Stopping {0}...".format(i.id))
    	try:
    		i.stop()
    	except botocore.exceptions.ClientError as e:
    		print ("Could not stop {0} ".format(i.id) + str(e))
    		continue
    return


@instances.command('reboot')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")

@click.option('--force', 'force', default=False, is_flag=True,
    help="Force reboot")

def reboot_instances(project,force):
    "Reboot EC2 instances"
    instances = get_instances(project)
    if not force and not project:
        print("ERR: You need ot specify --force or --project option in order to reboot instances")
        exit(2)

    for i in instances:
        print("Rebooting {0}...".format(i.id))
        try:
            i.reboot()
        except botocore.exceptions.ClientError as e:
            print ("Could not reboot {0} ".format(i.id) + str(e))
            continue
    return

@instances.command('start')
@click.option('--project', default=None,
	help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force', default=False, is_flag=True,
    help="Force reboot")

def start_instances(project,force):
    "Start EC2 instances"
    instances = get_instances(project)

    if not force and not project:
        print("ERR: You need ot specify --force or --project option in order to start instances")
        exit(2)

    for i in instances:
    	print("Starting {0}...".format(i.id))
    	try:
    		i.start()
    	except botocore.exceptions.ClientError as e:
    	    print ("Could not start {0} ".format(i.id) + str(e))
    	    continue

    return

@instances.command('create_snapshot')
@click.option('--project', default=None,
    help="Only instances for project (tag Project:<name>)")
@click.option('--force', 'force', default=False, is_flag=True,
    help="Force reboot")


def create_snapshot(project, force):
    "Create snapshots"
    instances = get_instances(project)

    if not force and not project:
        print("ERR: You need ot specify --force or --project option in order to start instances")
        exit(2)

    for i in instances:
        print("Stopping {0}...".format(i.id))
        i.stop()
        i.wait_until_stopped()

        for v in i.volumes.all():
            if has_pending_snapshot(v):
                print("Skipping")
                continue
            print("Creating snapshot of {0}".format(v.id))
            v.create_snapshot(Description="Created by Roman")

        i.start()
        i.wait_until_running()
    print("Job is done")
    return


if __name__ == '__main__':
    cli()
