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
    """
    Inicia la orquestación cuando api-clientes crea un pedido
    """
    try:
        detail = event['detail']
        order_id = detail.get('orderId')
        customer_id = detail.get('customerId')
        tenant_id = "pardos"  # Default tenant
        
        print(f"Recibido evento OrderCreated: {order_id}, cliente: {customer_id}")
        
        if not order_id:
            print("Error: orderId no encontrado en el evento")
            return {'status': 'ERROR', 'reason': 'orderId missing'}
        
        # Iniciar Step Function
        state_machine_arn = "arn:aws:states:us-east-1:213965374161:stateMachine:PardosOrderWorkflow"
        
        execution_response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=f"{order_id}-{uuid.uuid4().hex[:8]}",
            input=json.dumps({
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        
        print(f"Step Function iniciada: {execution_response['executionArn']}")
        
        # Publicar evento de workflow iniciado
        events.publish_event(
            source="pardos.orquestador",
            detail_type="WorkflowStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'executionArn': execution_response['executionArn'],
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Orquestación iniciada exitosamente',
                'orderId': order_id,
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
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        current_stage = event.get('currentStage', 'COOKING')
        
        print(f"Ejecutando etapa {current_stage} para orden {order_id}")
        
        # Lógica específica por etapa
        if current_stage == 'COOKING':
            return manejar_cocina(order_id, tenant_id, customer_id)
        elif current_stage == 'PACKAGING':
            return manejar_empaque(order_id, tenant_id, customer_id)
        elif current_stage == 'DELIVERY':
            return manejar_entrega(order_id, tenant_id, customer_id)
        else:
            return {'status': 'COMPLETED'}
            
    except Exception as e:
        print(f"Error en step function: {str(e)}")
        return {'status': 'FAILED', 'error': str(e)}

def manejar_cocina(order_id, tenant_id, customer_id):
    """Lógica específica para etapa de cocina"""
    timestamp = datetime.utcnow().isoformat()
    
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted", 
        detail={
            'orderId': order_id,
            'tenantId': tenant_id,
            'customerId': customer_id,
            'stage': 'COOKING',
            'timestamp': timestamp
        }
    )
    
    print(f"Etapa COOKING iniciada para orden {order_id}")
    return {'status': 'IN_PROGRESS', 'nextStage': 'PACKAGING'}

def manejar_empaque(order_id, tenant_id, customer_id):
    """Lógica específica para etapa de empaque"""
    timestamp = datetime.utcnow().isoformat()
    
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted",
        detail={
            'orderId': order_id,
            'tenantId': tenant_id, 
            'customerId': customer_id,
            'stage': 'PACKAGING',
            'timestamp': timestamp
        }
    )
    
    print(f"Etapa PACKAGING iniciada para orden {order_id}")
    return {'status': 'IN_PROGRESS', 'nextStage': 'DELIVERY'}

def manejar_entrega(order_id, tenant_id, customer_id):
    """Lógica específica para etapa de entrega"""
    timestamp = datetime.utcnow().isoformat()
    
    events.publish_event(
        source="pardos.orquestador",
        detail_type="StageStarted",
        detail={
            'orderId': order_id,
            'tenantId': tenant_id,
            'customerId': customer_id,
            'stage': 'DELIVERY', 
            'timestamp': timestamp
        }
    )
    
    print(f"Etapa DELIVERY iniciada para orden {order_id}")
    return {'status': 'IN_PROGRESS', 'nextStage': 'COMPLETED'}
