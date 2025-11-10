import json
import boto3
import uuid
from datetime import datetime
from shared.database import DynamoDB
from shared.events import EventBridge

stepfunctions = boto3.client('stepfunctions')
dynamodb = DynamoDB()
events = EventBridge()

def iniciar_orquestacion(event, context):
    try:
        detail = event['detail']
        order_id = detail.get('orderId')
        customer_id = detail.get('customerId')
        tenant_id = "pardos"
        
        print(f"Iniciando orquestacion para orden: {order_id}, cliente: {customer_id}")
        
        if not order_id:
            print("Error: orderId no encontrado en el evento")
            return {'status': 'ERROR', 'reason': 'orderId missing'}
        
        state_machine_arn = "arn:aws:states:us-east-1:213965374161:stateMachine:PardosOrderWorkflow"
        
        execution_response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"pardos-{order_id}-{uuid.uuid4().hex[:8]}",
            input=json.dumps({
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'timestamp': datetime.utcnow().isoformat(),
                'currentStage': 'COOKING'
            })
        )
        
        print(f"Step Function iniciada: {execution_response['executionArn']}")
        
        actualizar_estado_pedido(tenant_id, order_id, 'COOKING')
        
        events.publish_event(
            source="pardos.orquestador",
            detail_type="WorkflowStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'COOKING',
                'executionArn': execution_response['executionArn'],
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Orquestacion iniciada exitosamente',
                'orderId': order_id,
                'currentStage': 'COOKING',
                'executionArn': execution_response['executionArn']
            })
        }
        
    except Exception as e:
        print(f"Error en orquestacion: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def actualizar_estado_pedido(tenant_id, order_id, nuevo_estado):
    try:
        print(f"Actualizando estado del pedido {order_id} a {nuevo_estado}")
    except Exception as e:
        print(f"Error actualizando estado del pedido: {str(e)}")
