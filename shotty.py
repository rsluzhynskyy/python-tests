import boto3
import sys
import click
import botocore
import datetime


session = None
instance = None

# session = boto3.Session(profile_name="default")
# ec2 = session.resource('ec2')

@click.group()
@click.option('--profile', default=None, help="Specify AWS profile. OPTIONAL")
@click.option('--region', default=None, help="Specify AWS region. OPTIONAL")
def cli(profile, region):
    """Manages snapshots"""
    global session, instance
    if profile is None:
        profile = 'default'
    session = boto3.Session(profile_name=profile, region_name=region)


def get_instances(project,instance):
    ec2 = session.resource('ec2')    
    instances = []
    if project:
        filters = [{'Name': 'tag:Project', 'Values':[project]}]
        instances = ec2.instances.filter(Filters=filters)
    if instance:
        instance = [instance]
        instances = ec2.instances.filter(InstanceIds=instance)
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
@click.option('--instance', default=None,
    help="Only for specified instance-id")

def list_volumes(project, instance):
    "List volumes"

    instances = get_instances(project, instance)
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
@click.option('--instance', default=None,
    help="Only for specified instance-id")


def list_snapshots(project,list_all,instance):
    "List EC2 snapshots"
    instances = get_instances(project,instance)
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
    instances = get_instances(project,None)

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
@click.option('--instance', default=None,
    help="Only for specified instance-id")

def stop_instances(project,force,instance):
    "Stop EC2 instances"
    instances = get_instances(project,instance)

    if not force and not project and not instance:
        print("ERR: You need to specify --force or --project or --instance option in order to reboot instances")
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
@click.option('--instance', default=None,
    help="Only for specified instance-id")

def reboot_instances(project,force,instance):
    "Reboot EC2 instances"
    instances = get_instances(project,instance)
    if not force and not project and not instance:
        print("ERR: You need ot specify --force or --project or --instance option in order to reboot instances")
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
@click.option('--instance', default=None,
    help="Only for specified instance-id")

def start_instances(project,force,instance):
    "Start EC2 instances"
    instances = get_instances(project,instance)

    if not force and not project and not instance:
        print("ERR: You need ot specify --force or --project or --instance option in order to start instances")
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
@click.option('--instance', default=None,
    help="Only for specified instance-id")
@click.option('--age', default=None, type=int,
    help="Create snapshot only if last snapshot older than <age>")

def create_snapshot(project, force, instance, age):
    "Create snapshots"
    instances = get_instances(project,instance)

    if not force and not project and not instance:
        print("ERR: You need to specify --force or --project or --instance option in order to start instances")
        exit(2)
    
    if age:
        for i in instances:
            instance_state = i.state["Name"]
            for v in i.volumes.all():
                if not list(v.snapshots.all()):
                    print("Stopping {0}...".format(i.id))
                    i.stop()
                    i.wait_until_stopped()
                    if has_pending_snapshot(v):
                        print("Skipping")
                        continue
                    print("Creating snapshot of {0}".format(v.id))
                    try:
                        v.create_snapshot(Description="Created by Roman")
                    except botocore.exceptions.ClientError as e:
                        print ("Could not create snapshot for {0} ".format(v.id) + str(e))
                        continue
                else:
                    snapshot_iterator = sorted(v.snapshots.all(), key=lambda ss:ss.start_time, reverse=True)[0]
                    timeLimit = datetime.datetime.now().replace(microsecond=0) - datetime.timedelta(days=age)
                    lastSnapTime = datetime.datetime.strptime(str(snapshot_iterator.start_time)[:-6], '%Y-%m-%d %H:%M:%S')
                    print("latest snapshot for instance {0}, volume{1} was done {2}".format(i.id, v.id, snapshot_iterator.start_time))
                    if timeLimit > lastSnapTime:
                        print(i.id, v.id, snapshot_iterator.start_time)
                        print("Stopping {0}...".format(i.id))
                        i.stop()
                        i.wait_until_stopped()
                        if has_pending_snapshot(v):
                            print("Skipping")
                            continue
                        print("Creating snapshot of {0}".format(v.id))
                        try:
                            v.create_snapshot(Description="Created by Roman")
                        except botocore.exceptions.ClientError as e:
                            print ("Could not create snapshot for {0} ".format(v.id) + str(e))
                            continue
            if instance_state == "running":
                i.start()
                i.wait_until_running()
        print("Job is done")
        return


    else:

        for i in instances:
            instance_state = i.state["Name"]
            print("Stopping {0}...".format(i.id))
            i.stop()
            i.wait_until_stopped()

            for v in i.volumes.all():
                if has_pending_snapshot(v):
                    print("Skipping")
                    continue
                print("Creating snapshot of {0}".format(v.id))
                try:
                    v.create_snapshot(Description="Created by Roman")
                except botocore.exceptions.ClientError as e:
                    print ("Could not create snapshot for {0} ".format(v.id) + str(e))
                    continue

            if instance_state == "running":
                i.start()
                i.wait_until_running()
        print("Job is done")
        return


if __name__ == '__main__':
    cli()
