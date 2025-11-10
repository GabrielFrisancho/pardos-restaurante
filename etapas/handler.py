import json
import boto3
from datetime import datetime
from shared.database import DynamoDB
from shared.events import EventBridge

dynamodb = DynamoDB()
events = EventBridge()

def iniciar_etapa(event, context):
    """
    Endpoint para que el restaurante inicie manualmente una etapa
    """
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        assigned_to = body.get('assignedTo', 'Sistema')
        
        # Registrar inicio de etapa
        timestamp = datetime.utcnow().isoformat()
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': 'IN_PROGRESS',
            'startedAt': timestamp,
            'assignedTo': assigned_to,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        
        dynamodb.put_item('steps', step_record)
        
        # Actualizar orden con etapa actual
        dynamodb.update_item(
            table_name='orders',
            key={
                'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
                'SK': 'METADATA'
            },
            update_expression="SET currentStep = :step, updatedAt = :now",
            expression_values={
                ':step': stage,
                ':now': timestamp
            }
        )
        
        # Publicar evento
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'assignedTo': assigned_to,
                'timestamp': timestamp
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} iniciada',
                'stepRecord': step_record
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def completar_etapa(event, context):
    """
    Endpoint para que el restaurante complete una etapa
    """
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        
        # Buscar etapa activa
        response = dynamodb.query(
            table_name='steps',
            key_condition='PK = :pk AND begins_with(SK, :sk)',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': f"STEP#{stage}"
            }
        )
        
        if not response.get('Items'):
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Etapa no encontrada'})
            }
        
        # Completar la etapa más reciente
        latest_step = max(response['Items'], key=lambda x: x['startedAt'])
        timestamp = datetime.utcnow().isoformat()
        
        dynamodb.update_item(
            table_name='steps',
            key={
                'PK': latest_step['PK'],
                'SK': latest_step['SK']
            },
            update_expression="SET #s = :status, finishedAt = :finished",
            expression_names={'#s': 'status'},
            expression_values={
                ':status': 'COMPLETED',
                ':finished': timestamp
            }
        )
        
        # Publicar evento
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'startedAt': latest_step['startedAt'],
                'completedAt': timestamp,
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} completada',
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def procesar_evento_etapa(event, context):
    """
    Procesa eventos de etapas desde EventBridge
    """
    try:
        detail = event['detail']
        order_id = detail['orderId']
        tenant_id = detail['tenantId']
        
        if event['detail-type'] == 'StageStarted':
            # Lógica para StageStarted
            print(f"Etapa iniciada: {detail}")
            
        elif event['detail-type'] == 'StageCompleted':
            # Lógica para StageCompleted  
            print(f"Etapa completada: {detail}")
            
        return {'status': 'processed'}
        
    except Exception as e:
        print(f"Error procesando evento: {str(e)}")
        return {'status': 'error', 'error': str(e)}

def calcular_duracion(inicio, fin):
    """Calcula duración entre dos timestamps"""
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())