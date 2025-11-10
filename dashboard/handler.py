import json
from datetime import datetime, timedelta
from shared.database import DynamoDB

dynamodb = DynamoDB()

def obtener_resumen(event, context):
    """
    Obtiene resumen general para el dashboard
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        
        # Obtener métricas básicas
        total_pedidos = obtener_total_pedidos(tenant_id)
        pedidos_hoy = obtener_pedidos_hoy(tenant_id)
        pedidos_activos = obtener_pedidos_activos(tenant_id)
        tiempo_promedio = obtener_tiempo_promedio(tenant_id)
        
        resumen = {
            'totalPedidos': total_pedidos,
            'pedidosHoy': pedidos_hoy,
            'pedidosActivos': pedidos_activos,
            'tiempoPromedioEntrega': tiempo_promedio,
            'ultimaActualizacion': datetime.utcnow().isoformat()
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(resumen)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_metricas(event, context):
    """
    Obtiene métricas detalladas para gráficos
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        
        metricas = {
            'pedidosPorEstado': obtener_pedidos_por_estado(tenant_id),
            'tiemposPorEtapa': obtener_tiempos_por_etapa(tenant_id),
            'pedidosUltimaSemana': obtener_pedidos_ultima_semana(tenant_id),
            'productosPopulares': obtener_productos_populares(tenant_id)
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(metricas)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_pedidos(event, context):
    """
    Obtiene lista de pedidos para el dashboard
    """
    try:
        tenant_id = event.get('queryStringParameters', {}).get('tenantId', 'pardos')
        limit = int(event.get('queryStringParameters', {}).get('limit', 50))
        
        response = dynamodb.query(
            table_name='orders',
            key_condition='PK = :pk AND SK = :sk',
            expression_values={
                ':pk': f"TENANT#{tenant_id}#ORDER",
                ':sk': 'METADATA'
            },
            limit=limit,
            scan_index_forward=False  # Más recientes primero
        )
        
        pedidos = response.get('Items', [])
        
        # Enriquecer con información de etapas
        for pedido in pedidos:
            pedido['etapas'] = obtener_etapas_pedido(tenant_id, pedido['orderId'])
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pedidos': pedidos,
                'total': len(pedidos)
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# Funciones auxiliares
def obtener_total_pedidos(tenant_id):
    # Implementar conteo total de pedidos
    return 150

def obtener_pedidos_hoy(tenant_id):
    # Implementar conteo de pedidos de hoy
    return 25

def obtener_pedidos_activos(tenant_id):
    # Implementar conteo de pedidos activos
    return 8

def obtener_tiempo_promedio(tenant_id):
    # Implementar cálculo de tiempo promedio
    return 45  # minutos

def obtener_pedidos_por_estado(tenant_id):
    # Implementar distribución por estado
    return {
        'CREATED': 5,
        'COOKING': 3, 
        'PACKAGING': 2,
        'DELIVERY': 3,
        'DELIVERED': 137
    }

def obtener_tiempos_por_etapa(tenant_id):
    # Implementar tiempos promedio por etapa
    return {
        'COOKING': 15,
        'PACKAGING': 5,
        'DELIVERY': 25
    }

def obtener_pedidos_ultima_semana(tenant_id):
    # Implementar pedidos de la última semana
    return [25, 30, 28, 32, 35, 40, 38]

def obtener_productos_populares(tenant_id):
    # Implementar productos más populares
    return [
        {'producto': 'Pollo a la Brasa', 'cantidad': 120},
        {'producto': 'Chicha Morada', 'cantidad': 95},
        {'producto': 'Ensalada Fresca', 'cantidad': 80}
    ]

def obtener_etapas_pedido(tenant_id, order_id):
    # Obtener historial de etapas de un pedido
    response = dynamodb.query(
        table_name='steps',
        key_condition='PK = :pk AND begins_with(SK, :sk)',
        expression_values={
            ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
            ':sk': 'STEP#'
        }
    )
    return response.get('Items', [])