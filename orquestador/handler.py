import json
import boto3
import uuid
from datetime import datetime, timedelta
from shared.database import DynamoDB
from shared.events import EventBridge

stepfunctions = boto3.client('stepfunctions')
dynamodb = DynamoDB()
events = EventBridge()

def iniciar_orquestacion(event, context):
    """
    Inicia la orquestación cuando llega un evento OrderCreated
    """
    try:
        detail = event['detail']
        order_id = detail['orderId']
        tenant_id = detail['tenantId']
        
        # Iniciar Step Function
        state_machine_arn = "arn:aws:states:us-east-1:213965374161:stateMachine:PardosOrderWorkflow"
        
        execution_response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"{order_id}-{uuid.uuid4()}",
            input=json.dumps({
                'orderId': order_id,
                'tenantId': tenant_id,
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        
        # Actualizar orden con execution ARN
        dynamodb.update_item(
            table_name='orders',
            key={
                'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
                'SK': 'METADATA'
            },
            update_expression="SET executionArn = :arn, workflowStatus = :status",
            expression_values={
                ':arn': execution_response['executionArn'],
                ':status': 'WORKFLOW_STARTED'
            }
        )
        
        # Publicar evento
        events.publish_event(
            source="pardos.orquestador",
            detail_type="WorkflowStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'executionArn': execution_response['executionArn']
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Orquestación iniciada',
                'executionArn': execution_response['executionArn']
            })
        }
        
    except Exception as e:
        print(f"Error en orquestación: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def ejecutar_step_function(event, context):
    """
    Ejecuta lógica específica para cada etapa del Step Function
    """
    try:
        order_id = event['orderId']
        tenant_id = event['tenantId']
        current_stage = event.get('currentStage', 'COOKING')
        
        # Lógica específica por etapa
        if current_stage == 'COOKING':
            return manejar_cocina(order_id, tenant_id)
        elif current_stage == 'PACKAGING':
            return manejar_empaque(order_id, tenant_id)
        elif current_stage == 'DELIVERY':
            return manejar_entrega(order_id, tenant_id)
        else:
            return {'status': 'COMPLETED'}
            
    except Exception as e:
        print(f"Error en step function: {str(e)}")
        return {'status': 'FAILED', 'error': str(e)}

def manejar_cocina(order_id, tenant_id):
    """Lógica específica para etapa de cocina"""
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted", 
        detail={
            'orderId': order_id,
            'tenantId': tenant_id,
            'stage': 'COOKING',
            'timestamp': datetime.utcnow().isoformat()
        }
    )
    return {'status': 'IN_PROGRESS', 'nextStage': 'PACKAGING'}

def manejar_empaque(order_id, tenant_id):
    """Lógica específica para etapa de empaque"""
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted",
        detail={
            'orderId': order_id,
            'tenantId': tenant_id, 
            'stage': 'PACKAGING',
            'timestamp': datetime.utcnow().isoformat()
        }
    )
    return {'status': 'IN_PROGRESS', 'nextStage': 'DELIVERY'}

def manejar_entrega(order_id, tenant_id):
    """Lógica específica para etapa de entrega"""
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted",
        detail={
            'orderId': order_id,
            'tenantId': tenant_id,
            'stage': 'DELIVERY', 
            'timestamp': datetime.utcnow().isoformat()
        }
    )
    return {'status': 'IN_PROGRESS', 'nextStage': 'COMPLETED'}