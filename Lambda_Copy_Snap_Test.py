import boto3
import re
import os

def lambda_handler(event, context):
    try:
        source_db_instance_id = os.environ['SOURCE_DB_INSTANCE_ID']
        source_account_id = os.environ['SOURCE_ACCOUNT_ID']
        source_region = os.environ['SOURCE_REGION']
        
        rds_client = boto3.client('rds', region_name=source_region)
        
        # Obtener el último snapshot disponible
        snapshots = rds_client.describe_db_snapshots(DBInstanceIdentifier=source_db_instance_id)['DBSnapshots']
        available_snapshots = [s for s in snapshots if s['Status'] == 'available']
        
        if not available_snapshots:
            raise Exception(f"No se encontraron snapshots disponibles para la instancia DB {source_db_instance_id}")

        latest_snapshot = max(available_snapshots, key=lambda x: x['SnapshotCreateTime'])
        snapshot_identifier = latest_snapshot['DBSnapshotIdentifier']
        print(f"Último snapshot identificado: {snapshot_identifier}")

        # Crear el nuevo identificador para el snapshot copiado
        def next_copy_identifier(identifier):
            base_name = identifier.rsplit('-copy', 1)[0]
            match = re.search(r'-copy(\d+)$', identifier)
            if match:
                number = int(match.group(1))
                number += 1
            else:
                number = 1
            return f"{base_name}-copy{number}"

        # Obtener los snapshots copiados existentes para generar el próximo identificador único
        existing_copied_snapshots = [s['DBSnapshotIdentifier'] for s in available_snapshots if '-copy' in s['DBSnapshotIdentifier']]
        if existing_copied_snapshots:
            last_copied_snapshot = max(existing_copied_snapshots, key=lambda x: re.search(r'-copy([A-Z])(\d+)$', x) is not None)
            new_snapshot_identifier = next_copy_identifier(last_copied_snapshot)
        else:
            new_snapshot_identifier = next_copy_identifier(snapshot_identifier)
        
        # Validar el identificador del snapshot
        def validate_identifier(identifier):
            if not re.match(r'^[a-zA-Z]', identifier):
                identifier = 'a' + identifier
            identifier = re.sub(r'[^a-zA-Z0-9-]', '', identifier)
            if identifier.endswith('-'):
                identifier = identifier.rstrip('-')
            identifier = re.sub(r'--+', '-', identifier)
            return identifier
        
        # Intentar generar un identificador único si ya existe
        while new_snapshot_identifier in existing_copied_snapshots:
            new_snapshot_identifier = next_copy_identifier(new_snapshot_identifier)

        new_snapshot_identifier = validate_identifier(new_snapshot_identifier)
        print(f"Nuevo identificador para el snapshot copiado: {new_snapshot_identifier}")

        # Copiar el snapshot
        print("Copiando snapshot")
        copy_response = rds_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=latest_snapshot['DBSnapshotArn'],
            TargetDBSnapshotIdentifier=new_snapshot_identifier,
            SourceRegion=source_region
        )
        print(f"Snapshot copiado: {copy_response['DBSnapshot']['DBSnapshotIdentifier']}")

        return f"Snapshot copiado exitosamente: {new_snapshot_identifier}"

    except Exception as e:
        print(f"Error: {e}")
        return str(e)
    