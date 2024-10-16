import boto3
import os

def lambda_handler(event, context):
    try:
        source_account_id = os.environ['SOURCE_ACCOUNT_ID']
        target_account_id = os.environ['TARGET_ACCOUNT_ID']
        source_region = os.environ['SOURCE_REGION']
        sns_topic_arn = os.environ['SNS_TOPIC_ARN']  # ARN del tema SNS donde se enviará la notificación

        rds_client = boto3.client('rds', region_name=source_region)
        sns_client = boto3.client('sns', region_name=source_region)
        
        # Obtener todos los snapshots
        snapshots = rds_client.describe_db_snapshots(SnapshotType='manual')['DBSnapshots']
        
        print("Snapshots disponibles:")
        for snapshot in snapshots:
            print(snapshot['DBSnapshotIdentifier'], snapshot['Status'])

        # Filtrar snapshots copiados disponibles y seleccionar el más reciente por fecha de creación
        copied_snapshots = [s for s in snapshots if s['Status'] == 'available']
        
        if not copied_snapshots:
            raise Exception("No se encontraron snapshots copiados disponibles para compartir.")
        
        latest_copied_snapshot = max(copied_snapshots, key=lambda x: x['SnapshotCreateTime'])
        snapshot_identifier = latest_copied_snapshot['DBSnapshotIdentifier']
        print(f"Último snapshot copiado identificado: {snapshot_identifier}")

        # Compartir el snapshot con la cuenta de destino
        print("Compartiendo snapshot")
        rds_client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot_identifier,
            AttributeName='restore',
            ValuesToAdd=[target_account_id]
        )
        print(f"Snapshot {snapshot_identifier} compartido con la cuenta {target_account_id}")

        # Enviar notificación SNS
        message = f"Snapshot {snapshot_identifier} compartido exitosamente con la cuenta {target_account_id}"
        sns_client.publish(TopicArn=sns_topic_arn, Message=message)
        print(f"Notificación enviada al tema SNS: {sns_topic_arn}")

        return message

    except Exception as e:
        print(f"Error: {e}")
        return str(e)