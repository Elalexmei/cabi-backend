from flask import Flask, request, jsonify, render_template_string
import sys
import os
import mysql.connector 
import clickhouse_connect
import threading
import os.path as osp 


# --- INICIO DE LA CORRECCIÓN DE RUTA ---

# Obtener la ruta absoluta del directorio actual 
current_dir = osp.dirname(osp.abspath(__file__))
parent_dir = osp.join(current_dir, '..')
sys.path.insert(0, parent_dir)


from ejecutor import MasterQueryExecutor, get_user_logger, close_user_logger, get_all_sessions_report, get_error_sessions_report


app = Flask(__name__)

# ====================================================================
# SISTEMA DE INICIALIZACIÓN LAZY THREAD-SAFE
# ====================================================================

# Estado global del sistema (Permanece igual, pero gestionará ClickHouse)
_sistema_estado = {
    'inicializado': False,
    'inicializando': False,
    'ejecutor': None,
    'table_metadata': None,
    'error_inicializacion': None,
    'lock': threading.Lock()
}


# Función auxiliar para obtener el sistema actual (No necesita cambios)
def get_sistema_actual():
    global _sistema_estado
    if not _sistema_estado['inicializado'] and not _sistema_estado['inicializando']:
        # Iniciar la inicialización en un hilo separado para no bloquear la primera solicitud
        threading.Thread(target=inicializar_sistema_lazy).start()
        # Devuelve None temporalmente mientras se inicializa
        return None, None, "Sistema en proceso de inicialización (ClickHouse)..."
    return _sistema_estado['ejecutor'], _sistema_estado['table_metadata'], _sistema_estado['error_inicializacion']

# La función forzar_reinicializacion también debe limpiar el clickhouse_manager
def forzar_reinicializacion():
    global _sistema_estado
    with _sistema_estado['lock']:
        # Cerrar el ejecutor anterior si existe
        if _sistema_estado['ejecutor']:
            if hasattr(_sistema_estado['ejecutor'], 'clickhouse_manager') and _sistema_estado['ejecutor'].clickhouse_manager:
                print("🔒 Cerrando conexión ClickHouse anterior...")
                _sistema_estado['ejecutor'].clickhouse_manager.close_connection()
            # Cierra el logger
            close_user_logger("flask_app")
            
        # Resetear el estado
        _sistema_estado['inicializado'] = False
        _sistema_estado['inicializando'] = False
        _sistema_estado['ejecutor'] = None
        _sistema_estado['table_metadata'] = None
        _sistema_estado['error_inicializacion'] = None
        
        # Volver a inicializar
        return inicializar_sistema_lazy()


def get_clickhouse_config(): 
    """Configuración ClickHouse estándar"""
    return {
        # Usamos variables de entorno específicas para ClickHouse
        "host": os.getenv("CLICKHOUSE_HOST", "amj0c9lgbe.us-west-2.aws.clickhouse.cloud"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8443")), # Puerto estándar ClickHouse
        "database": os.getenv("CLICKHOUSE_DATABASE", "datos_imperiales"),
        "username": os.getenv("CLICKHOUSE_USER", "default"), # En ClickHouse es 'username'
        "password": os.getenv("CLICKHOUSE_PASSWORD", "c1.i4f8KmZ5HP"),
        "secure": True,
        "verify": False
    }


def get_latest_table():
    """
    🔍 BUSCAR AUTOMÁTICAMENTE LA TABLA MÁS RECIENTE EN CLICKHOUSE
    """
    try:
        ch_config = get_clickhouse_config()
        
        # Conexión ClickHouse
        connection = clickhouse_connect.get_client(
            host=ch_config['host'],
            port=ch_config['port'],
            database=ch_config['database'],
            username=ch_config['username'],
            password=ch_config['password'],
        )
        
        # Buscamos en 'tablas_metadata' (si existe, es la fuente más fiable)
        metadata_result = connection.query("""
            SELECT 
                table_name, 
                original_filename, 
                total_rows, 
                total_columns, 
                dimensions_count, 
                metrics_count, 
                analyzed_at
            FROM tablas_metadata 
            ORDER BY analyzed_at DESC
            LIMIT 1
        """)

        metadata = None
        if metadata_result.result_rows:
            row = metadata_result.result_rows[0]
            # Convertimos los datos al formato esperado
            metadata = {
                'table_name': row[0],
                'original_filename': row[1],
                'total_rows': int(row[2]),
                'total_columns': int(row[3]),
                'dimensions_count': int(row[4]),
                'metrics_count': int(row[5]),
                'analyzed_at': str(row[6]) # Convertir DateTime a string
            }
        
        connection.close()
        return metadata
        
    except Exception as e:
        print(f"❌ Error buscando tabla en ClickHouse: {e}")
        # Intentamos cerrar la conexión si está abierta
        if 'connection' in locals():
            connection.close()
        return None


def inicializar_sistema_lazy():
    """
    🚀 INICIALIZACIÓN LAZY THREAD-SAFE ADAPTADA A CLICKHOUSE
    """
    global _sistema_estado
    
    with _sistema_estado['lock']:
        # Si ya está inicializado o en proceso
        if _sistema_estado['inicializado'] or _sistema_estado['inicializando']:
            return _sistema_estado['ejecutor'], _sistema_estado['table_metadata'], _sistema_estado['error_inicializacion']

        # Marcar como inicializando
        _sistema_estado['inicializando'] = True
        print("🚀 INICIALIZACIÓN LAZY: Sistema ClickHouse con Session Manager...")
        
        try:
            flask_user_id = "flask_app"
            logger = get_user_logger(flask_user_id)
            logger.dev_log("🚀 Flask app iniciando en modo ClickHouse", "flask", "info")
            
            # Buscar tabla más reciente
            print("🔍 Buscando tabla ClickHouse automáticamente...")
            latest_table = get_latest_table()
            
            if not latest_table:
                error_msg = "No se encontraron tablas de metadatos en ClickHouse (tablas_metadata)."
                print(f"❌ {error_msg}")
                logger.dev_log(f"❌ Error: {error_msg}", "flask", "error")
                
                _sistema_estado['error_inicializacion'] = error_msg
                _sistema_estado['inicializando'] = False
                return None, None, error_msg
            
            table_name = latest_table['table_name']
            ch_config = get_clickhouse_config() # <-- ¡OBTENER CONFIG DE CLICKHOUSE!
            
            print(f"✅ Tabla ClickHouse encontrada: {table_name}")
            
            # 🎯 CREAR EJECUTOR
            print("🎯 Creando MasterQueryExecutor y conectando a ClickHouse...")
            ejecutor = MasterQueryExecutor(user_id=flask_user_id)
            
            # 🎯 ¡CAMBIO CLAVE! USAR set_clickhouse_mode
            ejecutor.set_clickhouse_mode(ch_config, table_name)
            
            # Verificar conexión ClickHouse
            if ejecutor.clickhouse_manager and ejecutor.clickhouse_manager.is_connected:
                print(f"🗄️ Conectado exitosamente a ClickHouse: {table_name}")
                logger.dev_log(f"✅ Conexión ClickHouse exitosa", "flask", "info")

                # Marcar como inicializado exitosamente
                _sistema_estado['ejecutor'] = ejecutor
                _sistema_estado['table_metadata'] = latest_table
                _sistema_estado['inicializado'] = True
                _sistema_estado['error_inicializacion'] = None
                _sistema_estado['inicializando'] = False
                
                logger.dev_log("✅ Inicialización Flask (ClickHouse) completada", "flask", "info")
                print("✅ INICIALIZACIÓN LAZY COMPLETADA CON CLICKHOUSE")
                return ejecutor, latest_table, None
            else:
                error_msg = "Error conectando a ClickHouse desde Flask"
                print(f"❌ {error_msg}")
                logger.dev_log(f"❌ Error de conexión ClickHouse", "flask", "error")
                
                _sistema_estado['error_inicializacion'] = error_msg
                _sistema_estado['inicializando'] = False
                return None, None, error_msg
                
        except Exception as e:
            error_msg = f"Error inicializando ejecutor (ClickHouse) desde Flask: {e}"
            print(f"❌ {error_msg}")
            
            if 'logger' in locals():
                logger.log_exception(e, component="flask", context="Durante inicialización lazy de Flask (ClickHouse)")
            
            _sistema_estado['error_inicializacion'] = error_msg
            _sistema_estado['inicializando'] = False
            return None, None, error_msg


def create_adaptive_card_with_table(query_results):
    """
    Crea el JSON de la Tarjeta Adaptable con el elemento 'Table' (v1.5).
    Asume que query_results es una lista de diccionarios.
    """
    if not query_results:
        return {
            "type": "AdaptiveCard",
            "version": "1.5",
            "body": [
                {"type": "TextBlock", "text": "Consulta sin resultados", "wrap": True}
            ]
        }
    
    # 1. Obtener los nombres de las columnas (cabecera)
    column_names = list(query_results[0].keys())
    
    # 2. Definir las columnas
    # Nota: Usamos "auto" para que las columnas se ajusten al contenido/espacio disponible
    columns = [{"width": "auto"} for _ in column_names]
    
    # 3. Definir las filas (usando la cabecera en la primera fila)
    rows = []

    for i, row_dict in enumerate(query_results):
        data_cells = [
            # Se usa negrita para la primera fila (cabecera) y normal para el resto.
            {"type": "TableCell", "items": [{"type": "TextBlock", "text": str(row_dict[name]), "weight": "bolder" if i == 0 else "default", "wrap": True}]}
            for name in column_names
        ]
        rows.append({"type": "TableRow", "cells": data_cells})

    # 4. Construir la Tarjeta Adaptable
    adaptive_card_json = {
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "📊 Resultados de la Consulta",
                "size": "Medium",
                "weight": "bolder"
            },
            {
                "type": "Table",
                "columns": columns,
                "rows": rows,
                # firstRowAsHeader se establece en False porque ya incluimos la cabecera manualmente
                "firstRowAsHeader": False, 
                "showGridLines": True
            }
        ]
    }

    # Nota: Para Teams, este JSON se envuelve como un attachment.
    # Para Postman, enviamos directamente este payload de la tarjeta.
    return adaptive_card_json


def get_sistema_actual():
    """
    📋 OBTENER ESTADO ACTUAL DEL SISTEMA
    Inicializa si es necesario
    """
    if not _sistema_estado['inicializado'] and not _sistema_estado['inicializando']:
        return inicializar_sistema_lazy()
    else:
        return _sistema_estado['ejecutor'], _sistema_estado['table_metadata'], _sistema_estado['error_inicializacion']


def forzar_reinicializacion():
    """
    🔄 FORZAR REINICIALIZACIÓN DEL SISTEMA
    Útil si hay errores o cambios en la base de datos
    """
    global _sistema_estado
    
    with _sistema_estado['lock']:
        print("🔄 Forzando reinicialización del sistema...")
        
        # Cerrar conexiones existentes si las hay
        if _sistema_estado['ejecutor'] and hasattr(_sistema_estado['ejecutor'], 'mysql_manager'):
            try:
                _sistema_estado['ejecutor'].mysql_manager.close()
            except:
                pass
        
        # Resetear estado
        _sistema_estado['inicializado'] = False
        _sistema_estado['inicializando'] = False
        _sistema_estado['ejecutor'] = None
        _sistema_estado['table_metadata'] = None
        _sistema_estado['error_inicializacion'] = None
        
        # Reinicializar
        return inicializar_sistema_lazy()


# ====================================================================
# PLANTILLA HTML CON SISTEMA DE RETROALIMENTACIÓN
# ====================================================================


MYSQL_AUTO_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>CABI V0.0.5 - MySQL Automático LAZY</title>
    <style>
        body { font-family: Arial; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1650px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: #2c3e50; color: white; padding: 15px; margin: -20px -20px 20px -20px; border-radius: 10px 10px 0 0; }
        .status { background: #e8f5e8; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #4caf50; }
        .mysql-info { background: #e3f2fd; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #2196f3; }
        
        .messages { 
            border: 1px solid #ddd; 
            height: 500px; 
            overflow-y: scroll; 
            padding: 15px; 
            margin: 10px 0; 
            background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
            border-radius: 10px;
        }
        
        .input-area { 
            margin: 15px 0; 
            display: flex; 
            gap: 10px; 
            align-items: center;
        }
        input[type="text"] { 
            flex: 1; 
            padding: 12px 20px; 
            border: 2px solid #ddd; 
            border-radius: 25px; 
            outline: none; 
            font-size: 14px;
        }
        input[type="text"]:focus { border-color: #007bff; }
        
        button { padding: 12px 20px; margin: 5px; border: none; border-radius: 5px; cursor: pointer; font-weight: bold; }
        .btn-primary { background: #007bff; color: white; border-radius: 25px; }
        .btn-success { background: #28a745; color: white; }
        .btn-warning { background: #ffc107; color: #212529; }
        .btn-primary:hover { background: #0056b3; }
        .btn-success:hover { background: #218838; }
        .btn-warning:hover { background: #e0a800; }
        
        .message { 
            margin: 12px 0; 
            display: flex; 
            align-items: flex-start;
            clear: both;
        }
        
        .message.user { 
            justify-content: flex-end; 
            margin-left: 20%;
        }
        
        .message.system { 
            justify-content: flex-start; 
            margin-right: 20%;
        }
        
    .message-bubble {
        max-width: 100%;
        padding: 12px 16px;
        border-radius: 18px;
        word-wrap: break-word;
        position: relative;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        font-size: 14px;
        line-height: 1.4;
    }
        
        .message.user .message-bubble {
            background: #007bff;
            color: white;
            border-bottom-right-radius: 4px;
            margin-left: auto;
        }
        
        .message.system .message-bubble {
            background: #e9ecef;
            color: #333;
            border-bottom-left-radius: 4px;
            margin-right: auto;
        }
        
        .message.error .message-bubble {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f1aeb5;
        }
        
        .message.success .message-bubble {
            background: #d1f2eb;
            color: #0c5460;
            border: 1px solid #a7e3d0;
        }
        
        .message.warning .message-bubble {
            background: #fff3cd;
            color: #856404;
            border: 1px solid #ffeaa7;
        }
        
        .message {
            animation: slideIn 0.3s ease-in-out;
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .dots {
            animation: blink 1.4s infinite;
        }
        
        @keyframes blink {
            0%, 80%, 100% { opacity: 1; }
            40% { opacity: 0.3; }
        }

        .error-container {
            background: #f8d7da;
            color: #721c24;
            padding: 20px;
            border-radius: 10px;
            border: 1px solid #f1aeb5;
            text-align: center;
        }
        
        .loading-container {
            background: #fff3cd;
            color: #856404;
            padding: 20px;
            border-radius: 10px;
            border: 1px solid #ffeaa7;
            text-align: center;
        }
    
        .status {
            position: relative;
            overflow: hidden;
        }
        
        .status button {
            transition: all 0.3s ease;
        }
        
        .status button:hover {
            transform: scale(1.05);
        }
        
        .error-container button {
            background: #ffc107;
            color: #212529;
            border: none;
            padding: 10px 15px;
            border-radius: 5px;
            cursor: pointer;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        
        .error-container button:hover {
            background: #e0a800;
            transform: scale(1.05);
        }

        /* Estilos para retroalimentación */
        .feedback-container {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 15px;
            margin: 10px 0;
            display: none;
            animation: fadeIn 0.3s ease-in-out;
        }

        .feedback-container.show {
            display: block;
        }

        .feedback-buttons {
            display: flex;
            gap: 15px;
            justify-content: center;
            margin: 10px 0;
        }

        .btn-feedback {
            padding: 10px 25px;
            border: 2px solid transparent;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            transition: all 0.3s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .btn-satisfied {
            background: #d4edda;
            color: #155724;
            border-color: #c3e6cb;
        }

        .btn-satisfied:hover {
            background: #c3e6cb;
            transform: scale(1.05);
        }

        .btn-unsatisfied {
            background: #f8d7da;
            color: #721c24;
            border-color: #f5c6cb;
        }

        .btn-unsatisfied:hover {
            background: #f5c6cb;
            transform: scale(1.05);
        }

        /* Modal para comentarios */
        .feedback-modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
            animation: fadeIn 0.3s ease-in-out;
        }

        .feedback-modal.show {
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .feedback-modal-content {
            background-color: white;
            padding: 30px;
            border-radius: 15px;
            width: 90%;
            max-width: 500px;
            box-shadow: 0 5px 25px rgba(0,0,0,0.2);
            animation: slideIn 0.3s ease-out;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        @keyframes slideIn {
            from { transform: translateY(-50px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        .feedback-modal-header {
            margin-bottom: 20px;
        }

        .feedback-modal-header h3 {
            margin: 0;
            color: #721c24;
        }

        .feedback-textarea {
            width: 100%;
            min-height: 120px;
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 8px;
            font-size: 14px;
            resize: vertical;
            font-family: Arial, sans-serif;
        }

        .feedback-textarea:focus {
            outline: none;
            border-color: #007bff;
        }

        .feedback-modal-buttons {
            display: flex;
            gap: 10px;
            justify-content: flex-end;
            margin-top: 20px;
        }

        .feedback-stats {
            background: #e7f3ff;
            border: 1px solid #b8daff;
            border-radius: 8px;
            padding: 12px;
            margin: 10px 0;
            font-size: 13px;
            color: #004085;
        }
        
        
        /* Nuevo contenedor para respuestas con feedback */
        .message-bubble-with-feedback {
            display: flex;
            align-items: flex-start;
            gap: 10px;
        }

        .message-content {
            flex: 1;
        }

        /* Mini botones de feedback integrados */
        .feedback-inline {
            display: flex;
            flex-direction: column;
            gap: 5px;
            margin-left: 10px;
            opacity: 0;
            transition: opacity 0.3s ease;
        }

        .message:hover .feedback-inline {
            opacity: 1;
        }

        .feedback-inline.show {
            opacity: 1;
        }

        .btn-feedback-mini {
            padding: 5px 10px;
            border: 1px solid #ddd;
            border-radius: 15px;
            cursor: pointer;
            font-size: 12px;
            background: white;
            transition: all 0.2s ease;
            white-space: nowrap;
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .btn-feedback-mini:hover {
            transform: scale(1.05);
        }

        .btn-feedback-mini.satisfied {
            border-color: #28a745;
            color: #28a745;
        }

        .btn-feedback-mini.satisfied:hover {
            background: #d4edda;
        }

        .btn-feedback-mini.unsatisfied {
            border-color: #dc3545;
            color: #dc3545;
        }

        .btn-feedback-mini.unsatisfied:hover {
            background: #f8d7da;
        }

        /* Estado cuando ya se dio feedback */
        .feedback-inline.feedback-given {
            opacity: 0.5;
            pointer-events: none;
        }

        .feedback-inline.feedback-given .btn-feedback-mini.selected {
            opacity: 1;
            font-weight: bold;
        }

        .feedback-inline.feedback-given .btn-feedback-mini:not(.selected) {
            opacity: 0.3;
        }

        /* Eliminar el contenedor de feedback separado anterior */
        .feedback-container {
            display: none !important;
        }

        /* Indicador de feedback pendiente */
        .feedback-pending-indicator {
            position: absolute;
            top: -5px;
            right: -5px;
            width: 10px;
            height: 10px;
            background: #ffc107;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(1); opacity: 1; }
            50% { transform: scale(1.2); opacity: 0.7; }
            100% { transform: scale(1); opacity: 1; }
        }

        /* Tooltip para feedback */
        .feedback-tooltip {
            position: absolute;
            background: #333;
            color: white;
            padding: 5px 10px;
            border-radius: 5px;
            font-size: 11px;
            white-space: nowrap;
            top: -30px;
            right: 0;
            opacity: 0;
            transition: opacity 0.2s;
            pointer-events: none;
        }

        .feedback-inline:hover .feedback-tooltip {
            opacity: 1;
        }

        /* Ajuste para móviles */
        @media (max-width: 768px) {
            .feedback-inline {
                opacity: 1;
                margin-left: 5px;
            }
            
            .btn-feedback-mini {
                padding: 6px 12px;
                font-size: 13px;
            }
        }
        
        /* Estilos para el indicador de historial */
        .history-indicator {
            position: absolute;
            right: 60px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 12px;
            color: #6c757d;
            background: #f8f9fa;
            padding: 2px 8px;
            border-radius: 10px;
            opacity: 0;
            transition: opacity 0.2s ease;
            pointer-events: none;
        }

        .history-indicator.show {
            opacity: 1;
        }

        /* Ajustar input para acomodar el indicador */
        .input-area {
            position: relative;
        }

        /* Tooltip de historial */
        .history-tooltip {
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: #333;
            color: white;
            padding: 8px 12px;
            border-radius: 5px;
            font-size: 12px;
            white-space: nowrap;
            margin-bottom: 5px;
            opacity: 0;
            transition: opacity 0.2s;
            pointer-events: none;
        }

        .history-tooltip.show {
            opacity: 1;
        }

        .history-tooltip::after {
            content: '';
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            border: 5px solid transparent;
            border-top-color: #333;
        }

        /* Indicador visual cuando se navega por el historial */
        input[type="text"].navigating-history {
            background-color: #f0f8ff;
            border-color: #0056b3;
        }

        /* Lista de historial (opcional - para mostrar visualmente) */
        .history-dropdown {
            position: absolute;
            bottom: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #ddd;
            border-radius: 10px;
            margin-bottom: 5px;
            max-height: 200px;
            overflow-y: auto;
            display: none;
            box-shadow: 0 -2px 10px rgba(0,0,0,0.1);
        }

        .history-dropdown.show {
            display: block;
        }

        .history-item {
            padding: 10px 15px;
            cursor: pointer;
            border-bottom: 1px solid #f0f0f0;
            font-size: 14px;
            transition: background-color 0.2s;
        }

        .history-item:hover {
            background-color: #f8f9fa;
        }

        .history-item.selected {
            background-color: #e3f2fd;
        }

        .history-item-time {
            font-size: 10px;
            color: #6c757d;
            float: right;
        }

        /* Contador de historial */
        .history-counter {
            position: absolute;
            top: -25px;
            right: 10px;
            font-size: 11px;
            color: #6c757d;
            background: white;
            padding: 2px 8px;
            border-radius: 10px;
            border: 1px solid #ddd;
            opacity: 0;
            transition: opacity 0.2s;
        }

        .history-counter.show {
            opacity: 1;
        }

    </style> 

</head>
<body>
    <div class="container">
        <div class="header">
            <h1>CABI 0.0.5 </h1>
            <p>--- Inicialización bajo demanda --- </p>
        </div>
        
        <!-- Estado del sistema -->
        <div id="systemStatus">
            <div class="loading-container">
                <h3>⏳ Inicializando Sistema...</h3>
                <p>La primera consulta activará la inicialización automática</p>
            </div>
        </div>
        
        <!-- Área de mensajes -->
        <div class="messages" id="messages">
            <!-- Se llena con JavaScript -->
        </div>
        
        <!-- Input para consultas -->
        <div class="input-area">
            <input type="text" id="queryInput" placeholder="💬 Escribe tu primera consulta para inicializar el sistema...">
            <button onclick="sendQuery()" class="btn-primary" id="sendBtn">Inicializar y Consultar</button>
        </div>
        
        <div style="margin-top: 20px; font-size: 12px; color: #666;">
            <strong>🎯 CABI 0.0.5 | Herramienta en desarrollo | 
            <a href="/feedback-dashboard" target="_blank">📊 Dashboard de Feedback</a>
        </div>
    </div>

    <!-- Modal de retroalimentación -->
    <div id="feedbackModal" class="feedback-modal">
        <div class="feedback-modal-content">
            <div class="feedback-modal-header">
                <h3>👎 ¿Qué podríamos mejorar?</h3>
                <p>Tu comentario nos ayudará a mejorar las respuestas</p>
            </div>
            <textarea 
                id="feedbackComment" 
                class="feedback-textarea" 
                placeholder="Describe qué esperabas encontrar o qué información faltó en la respuesta..."
                maxlength="500"
            ></textarea>
            <small style="color: #666;">Máximo 500 caracteres</small>
            <div class="feedback-modal-buttons">
                <button onclick="closeFeedbackModal()" class="btn-warning">Cancelar</button>
                <button onclick="submitNegativeFeedback()" class="btn-primary">Enviar Feedback</button>
            </div>
        </div>
    </div>


    <script>
        let systemReady = false;
        let systemInitializing = false;

        // Variables para retroalimentación
        let lastQuery = '';
        let lastResponse = '';
        let awaitingFeedback = false;
        
        let messagesWithFeedback = new Map();
        let messageIdCounter = 0;


        function addMessage(text, type = 'system', includeFeedback = false) {
            const messages = document.getElementById('messages');
            const messageDiv = document.createElement('div');
            const messageId = `msg-${++messageIdCounter}`;
            messageDiv.className = `message ${type}`;
            messageDiv.id = messageId;
            
            const bubbleDiv = document.createElement('div');
            bubbleDiv.className = 'message-bubble';
            
            let icon = '';
            if (type === 'user') {
                icon = '👤 ';
            } else if (type === 'system') {
                icon = '🤖 ';
            } else if (type === 'error') {
                icon = '❌ ';
            } else if (type === 'success') {
                icon = '✅ ';
            } else if (type === 'warning') {
                icon = '⚠️ ';
            }
            
            const now = new Date();
            const timestamp = now.toLocaleTimeString('es-ES', {hour: '2-digit', minute:'2-digit'});
            
            // Si incluye feedback, crear estructura especial
            if (includeFeedback && type === 'system') {
                bubbleDiv.innerHTML = `
                    <div class="message-bubble-with-feedback">
                        <div class="message-content">
                            <div>${icon}${text}</div>
                            <div style="font-size: 10px; opacity: 0.7; margin-top: 5px;">${timestamp}</div>
                        </div>
                        <div class="feedback-inline" id="feedback-${messageId}">
                            <div class="feedback-tooltip">¿Te fue útil?</div>
                            <button onclick="submitInlineFeedback('${messageId}', true)" class="btn-feedback-mini satisfied" title="Útil">
                                👍
                            </button>
                            <button onclick="submitInlineFeedback('${messageId}', false)" class="btn-feedback-mini unsatisfied" title="No útil">
                                👎
                            </button>
                        </div>
                    </div>
                    <div class="feedback-pending-indicator" id="indicator-${messageId}"></div>
                `;
                
                // Guardar información del mensaje para feedback
                messagesWithFeedback.set(messageId, {
                    query: lastQuery,
                    response: text,
                    timestamp: now,
                    feedbackGiven: false
                });
                
                // Mostrar indicador y feedback después de un breve delay
                setTimeout(() => {
                    const feedbackDiv = document.getElementById(`feedback-${messageId}`);
                    if (feedbackDiv) {
                        feedbackDiv.classList.add('show');
                    }
                }, 500);
                
            } else {
                // Mensaje normal sin feedback
                bubbleDiv.innerHTML = `
                    <div>${icon}${text}</div>
                    <div style="font-size: 10px; opacity: 0.7; margin-top: 5px; text-align: right;">${timestamp}</div>
                `;
            }
            
            messageDiv.appendChild(bubbleDiv);
            messages.appendChild(messageDiv);
            messages.scrollTop = messages.scrollHeight;
            
            return messageId;
        }

        function addTypingIndicator() {
            const messages = document.getElementById('messages');
            const typingDiv = document.createElement('div');
            typingDiv.className = 'message system typing-indicator';
            typingDiv.id = 'typing';
            
            const bubbleDiv = document.createElement('div');
            bubbleDiv.className = 'message-bubble';
            bubbleDiv.innerHTML = `
                <div>🤖 Procesando<span class="dots">...</span></div>
            `;
            
            typingDiv.appendChild(bubbleDiv);
            messages.appendChild(typingDiv);
            messages.scrollTop = messages.scrollHeight;
        }

        function removeTypingIndicator() {
            const typing = document.getElementById('typing');
            if (typing) {
                typing.remove();
            }
        }

        function checkSystemStatus() {
            console.log('🔍 Verificando estado del sistema LAZY...');
            
            fetch('/table-info')
            .then(response => response.json())
            .then(data => {
                console.log('📊 Estado recibido:', data);
                updateSystemStatus(data);
            })
            .catch(error => {
                console.error('❌ Error verificando estado:', error);
                showConnectionError(error);
            });
        }


        function updateSystemStatus(data) {
            const statusDiv = document.getElementById('systemStatus');
            const sendBtn = document.getElementById('sendBtn');
            const queryInput = document.getElementById('queryInput');

            // 1. Manejar estado de Inicialización
            if (data.initializing) {
                statusDiv.innerHTML = `
                    <div class="loading-container">
                        <h3>⏳ Inicializando Sistema ClickHouse...</h3>
                        <p>Conectando a la tabla del TableAnalyzer...</p>
                        <p><small>Esto solo ocurre una vez por sesión</small></p>
                    </div>
                `;
                systemInitializing = true;
                systemReady = false;
                queryInput.disabled = true;
                sendBtn.disabled = true;
                sendBtn.textContent = 'Inicializando...';
                
            // 2. Manejar estado Listo (ClickHouse)
            // Se verifica el modo 'clickhouse'
            } else if (data.loaded && data.mode === 'clickhouse') { 
                
                const tableInfo = data.table_info || {};
                const chStatus = data.clickhouse_status || {};
                const metadata = data.table_metadata || {};
                
                // El bloque HTML usa la clase .mysql-info, pero lo adaptamos para ClickHouse
                statusDiv.innerHTML = `
                    <div class="status">
                        <strong>DB OPERATIVA</strong>
                    </div>
                    <div class="mysql-info">
                        <strong>Conexión:</strong> ${chStatus.connected ? 'Activa' : 'Inactiva'}<br>
                    </div>
                `;
                
                systemReady = true;
                systemInitializing = false;
                queryInput.disabled = false;
                sendBtn.disabled = false;
                sendBtn.textContent = 'Enviar';
                queryInput.placeholder = '💬 Escribe tu consulta aquí...';
                
                if (!document.getElementById('messages').querySelector('.message.success, .message.error')) {
                    addMessage('<strong>🎯 Sistema LAZY Inicializado</strong><br><br>✅ Conectado a ClickHouse.<br>💬 Listo para consultas', 'success');
                }
                
            // 3. Manejar estado de Error
            } else if (data.error) {
                statusDiv.innerHTML = `
                    <div class="error-container">
                        <h3>❌ Error de Inicialización LAZY</h3>
                        <p><strong>Error:</strong> ${data.error}</p>
                        <p><strong>Solución:</strong> Verifica que ClickHouse esté corriendo y que la tabla 'tablas_metadata' exista.</p>
                    </div>
                `;
                
                systemReady = false;
                systemInitializing = false;
                queryInput.disabled = true;
                sendBtn.disabled = true;
                
                addMessage(`❌ <strong>Error de inicialización:</strong><br>${data.error}`, 'error');
            }
        }


        function showConnectionError(error) {
            document.getElementById('systemStatus').innerHTML = `
                <div class="error-container">
                    <h3>❌ Error de Conexión</h3>
                    <p>No se pudo conectar al servidor backend</p>
                    <p><strong>Error:</strong> ${error.message}</p>
                </div>
            `;
            
            addMessage('❌ <strong>Error de conexión al backend</strong><br>Verifica que el servidor esté ejecutándose', 'error');
        }

        function sendQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                addMessage('❌ Escribe una consulta', 'error');
                return;
            }
            
            // Guardar la consulta actual
            lastQuery = query;
            
            // Si no está listo, indicar que se inicializará
            if (!systemReady && !systemInitializing) {
                addMessage('🎯 <strong>Primera consulta</strong> - Inicializando sistema automáticamente...', 'warning');
            }
            
            addMessage(`<strong>Consulta:</strong> ${query}`, 'user');
            addTypingIndicator();
            
            // Deshabilitar botón temporalmente
            document.getElementById('sendBtn').disabled = true;
            document.getElementById('sendBtn').textContent = 'Procesando...';
            
            fetch('/ask', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: query })
            })
            .then(response => response.json())
            .then(data => {
                removeTypingIndicator();
                
                if (data.success) {
                    // Si no estaba listo antes, actualizar estado
                    if (!systemReady) {
                        checkSystemStatus();
                    }
                    
                    addMessage(`<strong>✅ Consulta Exitosa</strong><br>📊 <strong>Resultados:</strong> ${data.row_count} filas<br>⏱️ <strong>Tiempo:</strong> ${data.duration_ms}ms`, 'success');
                    
                    if (data.formatted_results) {
                        addMessage(`<strong>📋 DATOS:</strong><pre style="white-space: pre-wrap; font-family: monospace; background: #f8f9fa; padding: 10px; border-radius: 4px; margin: 5px 0;">${data.formatted_results}</pre>`, 'system');
                        
                        // Guardar la respuesta y mostrar botones de feedback
                        lastResponse = data.formatted_results || '';
                        showFeedbackButtons();
                    }
                } else {
                    addMessage(`<strong>❌ Error:</strong> ${data.error}`, 'error');
                    if (data.suggestions && data.suggestions.length > 0) {
                        addMessage(`<strong>💡 Sugerencias:</strong><br>${data.suggestions.join('<br>')}`, 'system');
                    }
                }
                
                // Rehabilitar botón
                document.getElementById('sendBtn').disabled = false;
                document.getElementById('sendBtn').textContent = 'Enviar';
            })
            .catch(error => {
                removeTypingIndicator();
                addMessage(`<strong>❌ Error de conexión:</strong> ${error.message}`, 'error');
                
                // Rehabilitar botón
                document.getElementById('sendBtn').disabled = false;
                document.getElementById('sendBtn').textContent = 'Enviar';
            });
            
            document.getElementById('queryInput').value = '';
        }

        function forceReinit() {
            addMessage('🔄 <strong>Forzando reinicialización del sistema...</strong>', 'warning');
            
            fetch('/force-reinit', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    addMessage('✅ <strong>Reinicialización exitosa</strong>', 'success');
                    setTimeout(() => {
                        checkSystemStatus();
                    }, 1000);
                } else {
                    addMessage(`❌ <strong>Error en reinicialización:</strong> ${data.error}`, 'error');
                }
            })
            .catch(error => {
                addMessage(`❌ <strong>Error:</strong> ${error.message}`, 'error');
            });
        }

        // Funciones de retroalimentación
        function showFeedbackButtons() {
            // Crear o mostrar contenedor de feedback
            let feedbackDiv = document.getElementById('feedbackContainer');
            if (!feedbackDiv) {
                feedbackDiv = document.createElement('div');
                feedbackDiv.id = 'feedbackContainer';
                feedbackDiv.className = 'feedback-container';
                document.getElementById('messages').appendChild(feedbackDiv);
            }
            
            feedbackDiv.innerHTML = `
                <div style="text-align: center;">
                    <p style="margin: 5px 0; font-weight: bold;">¿La respuesta fue útil?</p>
                    <div class="feedback-buttons">
                        <button onclick="submitFeedback(true)" class="btn-feedback btn-satisfied">
                            👍 Sí, fue útil
                        </button>
                        <button onclick="submitFeedback(false)" class="btn-feedback btn-unsatisfied">
                            👎 No me satisface
                        </button>
                    </div>
                </div>
            `;
            
            feedbackDiv.classList.add('show');
            awaitingFeedback = true;
            
            // Auto-scroll para mostrar los botones
            const messages = document.getElementById('messages');
            messages.scrollTop = messages.scrollHeight;
        }

        function submitFeedback(satisfied) {
            if (!awaitingFeedback) return;
            
            if (satisfied) {
                // Feedback positivo - enviar directamente
                sendFeedbackToServer(satisfied, '');
            } else {
                // Feedback negativo - mostrar modal para comentario
                document.getElementById('feedbackModal').classList.add('show');
                document.getElementById('feedbackComment').focus();
            }
        }

        function sendFeedbackToServer(satisfied, comment) {
            fetch('/feedback', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query: lastQuery,
                    response: lastResponse,
                    satisfied: satisfied,
                    comment: comment
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Ocultar botones de feedback
                    const feedbackDiv = document.getElementById('feedbackContainer');
                    if (feedbackDiv) {
                        feedbackDiv.classList.remove('show');
                    }
                    
                    // Mostrar mensaje de confirmación
                    if (satisfied) {
                        addMessage('👍 ¡Gracias por tu feedback positivo!', 'success');
                    } else {
                        addMessage('👎 Gracias por tu feedback. Tu comentario ha sido registrado y nos ayudará a mejorar.', 'warning');
                        if (data.session_will_be_saved) {
                            addMessage('💾 La sesión se ha marcado para revisión.', 'system');
                        }
                    }
                    
                    awaitingFeedback = false;
                    updateFeedbackStats();
                } else {
                    addMessage('❌ Error al enviar feedback: ' + data.error, 'error');
                }
            })
            .catch(error => {
                addMessage('❌ Error de conexión al enviar feedback', 'error');
            });
        }

        function submitNegativeFeedback() {
            const comment = document.getElementById('feedbackComment').value.trim();
            if (!comment) {
                alert('Por favor, proporciona un comentario para ayudarnos a mejorar');
                return;
            }
            
            closeFeedbackModal();
            sendFeedbackToServer(false, comment);
        }

        function closeFeedbackModal() {
            document.getElementById('feedbackModal').classList.remove('show');
            document.getElementById('feedbackComment').value = '';
        }

        function updateFeedbackStats() {
            // Actualizar estadísticas de feedback si tienes un área para mostrarlas
            fetch('/feedback-summary')
            .then(response => response.json())
            .then(data => {
                if (data.total_feedback_received > 0) {
                    // Puedes mostrar estas estadísticas en algún lugar de la interfaz
                    console.log('Satisfacción:', data.satisfaction_rate);
                }
            })
            .catch(error => {
                console.error('Error obteniendo estadísticas de feedback:', error);
            });
        }

        // Cerrar modal al hacer clic fuera
        window.onclick = function(event) {
            const modal = document.getElementById('feedbackModal');
            if (event.target == modal) {
                closeFeedbackModal();
            }
        }

        // Prevenir envío de nueva consulta si hay feedback pendiente
        document.getElementById('queryInput').addEventListener('focus', function() {
            if (awaitingFeedback) {
                const feedbackDiv = document.getElementById('feedbackContainer');
                if (feedbackDiv) {
                    feedbackDiv.style.border = '2px solid #ffc107';
                    setTimeout(() => {
                        feedbackDiv.style.border = '1px solid #dee2e6';
                    }, 1000);
                }
            }
        });

        // Enviar con Enter
        document.getElementById('queryInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                sendQuery();
            }
        });

        // Inicializar al cargar la página (pero sin inicializar el backend)
        document.addEventListener('DOMContentLoaded', function() {
            console.log('🚀 Página cargada - En espera');
            addMessage('🎯 <strong>Modo En espera de consulta</strong><br>El sistema se inicializará automáticamente con tu primera consulta', 'system');
            checkSystemStatus();
        });
        

        // Nueva función para manejar feedback inline
        function submitInlineFeedback(messageId, satisfied) {
            const messageData = messagesWithFeedback.get(messageId);
            if (!messageData || messageData.feedbackGiven) return;
            
            // Marcar como feedback dado
            messageData.feedbackGiven = true;
            messagesWithFeedback.set(messageId, messageData);
            
            // Actualizar UI inmediatamente
            const feedbackDiv = document.getElementById(`feedback-${messageId}`);
            const indicator = document.getElementById(`indicator-${messageId}`);
            
            if (feedbackDiv) {
                feedbackDiv.classList.add('feedback-given');
                const buttons = feedbackDiv.querySelectorAll('.btn-feedback-mini');
                buttons.forEach(btn => {
                    if ((satisfied && btn.classList.contains('satisfied')) || 
                        (!satisfied && btn.classList.contains('unsatisfied'))) {
                        btn.classList.add('selected');
                    }
                });
            }
            
            if (indicator) {
                indicator.style.display = 'none';
            }
            
            if (satisfied) {
                // Feedback positivo - enviar directamente
                sendFeedbackToServer(satisfied, '', messageData.query, messageData.response);
            } else {
                // Feedback negativo - mostrar modal para comentario
                // Guardar contexto actual
                currentFeedbackContext = {
                    messageId: messageId,
                    query: messageData.query,
                    response: messageData.response
                };
                
                document.getElementById('feedbackModal').classList.add('show');
                document.getElementById('feedbackComment').focus();
            }
        }

        // Variable para contexto de feedback actual
        let currentFeedbackContext = null;

        // Modificar submitNegativeFeedback para usar el contexto
        function submitNegativeFeedback() {
            const comment = document.getElementById('feedbackComment').value.trim();
            if (!comment) {
                alert('Por favor, proporciona un comentario para ayudarnos a mejorar');
                return;
            }
            
            closeFeedbackModal();
            
            if (currentFeedbackContext) {
                sendFeedbackToServer(false, comment, 
                    currentFeedbackContext.query, 
                    currentFeedbackContext.response
                );
                currentFeedbackContext = null;
            }
        }

        // Modificar sendQuery para usar el nuevo sistema
        function sendQuery() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) {
                addMessage('❌ Escribe una consulta', 'error');
                return;
            }
            
            // Guardar la consulta actual
            lastQuery = query;
            
            // Ocultar feedback de mensajes anteriores que no se respondieron
            document.querySelectorAll('.feedback-inline.show').forEach(el => {
                if (!el.classList.contains('feedback-given')) {
                    el.classList.remove('show');
                }
            });
            
            // Si no está listo, indicar que se inicializará
            if (!systemReady && !systemInitializing) {
                addMessage('🎯 <strong>Primera consulta</strong> - Inicializando sistema automáticamente...', 'warning');
            }
            
            addMessage(`<strong>Consulta:</strong> ${query}`, 'user');
            addTypingIndicator();
            
            // Deshabilitar botón temporalmente
            document.getElementById('sendBtn').disabled = true;
            document.getElementById('sendBtn').textContent = 'Procesando...';
            
            fetch('/ask', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: query })
            })
            .then(response => response.json())
            .then(data => {
                removeTypingIndicator();
                
                if (data.success) {
                    // Si no estaba listo antes, actualizar estado
                    if (!systemReady) {
                        checkSystemStatus();
                    }
                    
                    addMessage(`<strong>✅ Consulta Exitosa</strong><br>📊 <strong>Resultados:</strong> ${data.row_count} filas<br>⏱️ <strong>Tiempo:</strong> ${data.duration_ms}ms`, 'success');
                    
                    if (data.formatted_results) {
                        // Agregar mensaje con feedback integrado
                        const responseText = `<strong>📋 DATOS:</strong><pre style="white-space: pre-wrap; font-family: monospace; background: #f8f9fa; padding: 10px; border-radius: 4px; margin: 5px 0;">${data.formatted_results}</pre>`;
                        
                        // Guardar la respuesta
                        lastResponse = data.formatted_results || '';
                        
                        // Agregar mensaje con opción de feedback
                        addMessage(responseText, 'system', true);
                    }
                } else {
                    addMessage(`<strong>❌ Error:</strong> ${data.error}`, 'error');
                    if (data.suggestions && data.suggestions.length > 0) {
                        addMessage(`<strong>💡 Sugerencias:</strong><br>${data.suggestions.join('<br>')}`, 'system');
                    }
                }
                
                // Rehabilitar botón
                document.getElementById('sendBtn').disabled = false;
                document.getElementById('sendBtn').textContent = 'Enviar';
            })
            .catch(error => {
                removeTypingIndicator();
                addMessage(`<strong>❌ Error de conexión:</strong> ${error.message}`, 'error');
                
                // Rehabilitar botón
                document.getElementById('sendBtn').disabled = false;
                document.getElementById('sendBtn').textContent = 'Enviar';
            });
            
            document.getElementById('queryInput').value = '';
        }

        // Modificar sendFeedbackToServer para aceptar parámetros específicos
        function sendFeedbackToServer(satisfied, comment, query, response) {
            fetch('/feedback', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query: query || lastQuery,
                    response: response || lastResponse,
                    satisfied: satisfied,
                    comment: comment
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Mostrar mensaje de confirmación
                    if (satisfied) {
                        addMessage('👍 ¡Gracias por tu feedback positivo!', 'success');
                    } else {
                        addMessage('👎 Gracias por tu feedback. Tu comentario ha sido registrado y nos ayudará a mejorar.', 'warning');
                        if (data.session_will_be_saved) {
                            addMessage('💾 La sesión se ha marcado para revisión.', 'system');
                        }
                    }
                    
                    updateFeedbackStats();
                } else {
                    addMessage('❌ Error al enviar feedback: ' + data.error, 'error');
                }
            })
            .catch(error => {
                addMessage('❌ Error de conexión al enviar feedback', 'error');
            });
        }

        // Función para limpiar indicadores de feedback no respondidos
        function cleanupPendingFeedback() {
            messagesWithFeedback.forEach((data, messageId) => {
                if (!data.feedbackGiven) {
                    const indicator = document.getElementById(`indicator-${messageId}`);
                    if (indicator) {
                        indicator.style.display = 'none';
                    }
                }
            });
        }

        // Limpiar feedback pendiente cada 5 minutos
        setInterval(cleanupPendingFeedback, 300000);

    </script>
    

    <script>
        // Sistema de historial de consultas BASADO EN SESIÓN
        class SessionQueryHistory {
            constructor(maxSize = 50) {
                this.history = [];
                this.currentIndex = -1;
                this.maxSize = maxSize;
                this.tempQuery = ''; // Para guardar la consulta actual al navegar
                this.isNavigating = false;
                this.sessionId = this.generateSessionId();
                this.sessionStartTime = new Date();
                
                // NO cargar desde localStorage - empezar siempre vacío
                console.log(`📜 Nueva sesión de historial iniciada: ${this.sessionId}`);
            }
            
            // Generar ID único de sesión
            generateSessionId() {
                return `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            }
            
            // Agregar consulta al historial
            add(query) {
                if (!query.trim()) return;
                
                // Evitar duplicados consecutivos
                if (this.history.length > 0 && this.history[this.history.length - 1].query === query) {
                    return;
                }
                
                // Agregar con timestamp y session info
                this.history.push({
                    query: query,
                    timestamp: new Date().toISOString(),
                    sessionId: this.sessionId,
                    index: this.history.length
                });
                
                // Limitar tamaño del historial
                if (this.history.length > this.maxSize) {
                    this.history.shift();
                }
                
                // Resetear índice
                this.currentIndex = this.history.length;
                
                // NO guardar en localStorage - mantener solo en memoria
                console.log(`📝 Consulta agregada al historial de sesión: "${query}" (Total: ${this.history.length})`);
            }
            
            // Navegar hacia atrás en el historial
            navigateUp(currentValue) {
                // Si es la primera vez que navegamos, guardar el valor actual
                if (!this.isNavigating && currentValue) {
                    this.tempQuery = currentValue;
                    this.isNavigating = true;
                    this.currentIndex = this.history.length;
                }
                
                if (this.currentIndex > 0) {
                    this.currentIndex--;
                    return this.history[this.currentIndex].query;
                }
                
                return null;
            }
            
            // Navegar hacia adelante en el historial
            navigateDown() {
                if (this.currentIndex < this.history.length - 1) {
                    this.currentIndex++;
                    return this.history[this.currentIndex].query;
                } else if (this.currentIndex === this.history.length - 1) {
                    // Volver a la consulta temporal
                    this.currentIndex = this.history.length;
                    this.isNavigating = false;
                    return this.tempQuery;
                }
                
                return null;
            }
            
            // Resetear navegación
            resetNavigation() {
                this.currentIndex = this.history.length;
                this.isNavigating = false;
                this.tempQuery = '';
            }
            
            // Obtener las últimas n consultas
            getRecent(n = 10) {
                return this.history.slice(-n).reverse();
            }
            
            // Buscar en el historial
            search(term) {
                return this.history.filter(item => 
                    item.query.toLowerCase().includes(term.toLowerCase())
                );
            }
            
            // Limpiar historial (al finalizar sesión)
            clear() {
                const totalQueries = this.history.length;
                this.history = [];
                this.currentIndex = -1;
                this.isNavigating = false;
                this.tempQuery = '';
                
                console.log(`🗑️ Historial de sesión limpiado. Total consultas en sesión: ${totalQueries}`);
                return totalQueries;
            }
            
            // Obtener información del estado actual
            getStatus() {
                return {
                    total: this.history.length,
                    currentIndex: this.currentIndex,
                    isNavigating: this.isNavigating,
                    position: this.currentIndex >= 0 && this.currentIndex < this.history.length ? 
                            `${this.currentIndex + 1}/${this.history.length}` : 'actual',
                    sessionId: this.sessionId,
                    sessionDuration: this.getSessionDuration()
                };
            }
            
            // Obtener duración de la sesión
            getSessionDuration() {
                const now = new Date();
                const duration = now - this.sessionStartTime;
                const minutes = Math.floor(duration / 60000);
                const seconds = Math.floor((duration % 60000) / 1000);
                return `${minutes}m ${seconds}s`;
            }
            
            // Obtener resumen de la sesión
            getSessionSummary() {
                return {
                    sessionId: this.sessionId,
                    startTime: this.sessionStartTime.toISOString(),
                    duration: this.getSessionDuration(),
                    totalQueries: this.history.length,
                    queries: this.history.map(h => h.query)
                };
            }
        }

        // Instanciar el historial de sesión
        let queryHistory = new SessionQueryHistory();

        // Variables para el manejo del historial
        let historyTooltipTimeout = null;

        // Función para mostrar el indicador de historial
        function showHistoryIndicator(text) {
            let indicator = document.getElementById('historyIndicator');
            if (!indicator) {
                indicator = document.createElement('div');
                indicator.id = 'historyIndicator';
                indicator.className = 'history-indicator';
                document.querySelector('.input-area').appendChild(indicator);
            }
            
            indicator.textContent = text;
            indicator.classList.add('show');
            
            // Ocultar después de 2 segundos
            clearTimeout(historyTooltipTimeout);
            historyTooltipTimeout = setTimeout(() => {
                indicator.classList.remove('show');
            }, 2000);
        }

        // Función para mostrar contador de posición
        function updateHistoryCounter() {
            let counter = document.getElementById('historyCounter');
            if (!counter) {
                counter = document.createElement('div');
                counter.id = 'historyCounter';
                counter.className = 'history-counter';
                document.querySelector('.input-area').appendChild(counter);
            }
            
            const status = queryHistory.getStatus();
            if (status.isNavigating) {
                counter.textContent = `📜 ${status.position}`;
                counter.classList.add('show');
            } else {
                counter.classList.remove('show');
            }
        }

        // Función para reiniciar el historial (cuando se reinicia el sistema)
        function resetSessionHistory() {
            const summary = queryHistory.getSessionSummary();
            const totalQueries = queryHistory.clear();
            
            // Crear nueva instancia
            queryHistory = new SessionQueryHistory();
            
            // Mostrar resumen de la sesión anterior
            if (totalQueries > 0) {
                addMessage(
                    `📊 <strong>Sesión anterior finalizada</strong><br>` +
                    `Total consultas: ${totalQueries}<br>` +
                    `Duración: ${summary.duration}`,
                    'system'
                );
            }
            
            console.log('📜 Historial de sesión reiniciado');
        }

        // Vincular el reinicio del historial con el reinicio del sistema
        const originalForceReinit = forceReinit;
        forceReinit = function() {
            // Limpiar historial antes de reiniciar
            resetSessionHistory();
            
            // Llamar a la función original
            originalForceReinit();
        };

        // Limpiar historial cuando se cierra/recarga la página
        window.addEventListener('beforeunload', function(e) {
            // Obtener resumen final
            const summary = queryHistory.getSessionSummary();
            
            // Log final (solo se verá en la consola si está abierta)
            console.log('🔚 Finalizando sesión:', summary);
            
            // Limpiar historial
            queryHistory.clear();
            
            // NO guardar nada en localStorage
        });

        // Modificar el input para agregar listeners de teclado
        document.addEventListener('DOMContentLoaded', function() {
            const queryInput = document.getElementById('queryInput');
            
            // Mostrar mensaje de nueva sesión
            console.log('🆕 Nueva sesión de historial iniciada');
            
            // Agregar manejador de teclas
            queryInput.addEventListener('keydown', function(e) {
                // Tecla flecha arriba
                if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    
                    const previousQuery = queryHistory.navigateUp(queryInput.value);
                    if (previousQuery !== null) {
                        queryInput.value = previousQuery;
                        queryInput.classList.add('navigating-history');
                        showHistoryIndicator('↑ Historial de sesión');
                        updateHistoryCounter();
                        
                        // Mover cursor al final
                        queryInput.setSelectionRange(queryInput.value.length, queryInput.value.length);
                    } else {
                        showHistoryIndicator('↑ Inicio del historial de sesión');
                    }
                }
                
                // Tecla flecha abajo
                else if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    
                    if (queryHistory.isNavigating) {
                        const nextQuery = queryHistory.navigateDown();
                        if (nextQuery !== null) {
                            queryInput.value = nextQuery;
                            if (!queryHistory.isNavigating) {
                                queryInput.classList.remove('navigating-history');
                                showHistoryIndicator('↓ Consulta actual');
                            } else {
                                showHistoryIndicator('↓ Siguiente en sesión');
                            }
                            updateHistoryCounter();
                            
                            // Mover cursor al final
                            queryInput.setSelectionRange(queryInput.value.length, queryInput.value.length);
                        }
                    }
                }
                
                // Escape para cancelar navegación
                else if (e.key === 'Escape' && queryHistory.isNavigating) {
                    e.preventDefault();
                    queryHistory.resetNavigation();
                    queryInput.value = queryHistory.tempQuery;
                    queryInput.classList.remove('navigating-history');
                    showHistoryIndicator('✖ Navegación cancelada');
                    updateHistoryCounter();
                }
                
                // Ctrl+H para mostrar/ocultar lista de historial
                else if (e.ctrlKey && e.key === 'h') {
                    e.preventDefault();
                    toggleHistoryDropdown();
                }
                
                // Cualquier otra tecla que modifique el contenido resetea la navegación
                else if (!e.ctrlKey && !e.altKey && !e.metaKey && 
                        e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete') {
                    if (queryHistory.isNavigating) {
                        queryHistory.resetNavigation();
                        queryInput.classList.remove('navigating-history');
                        updateHistoryCounter();
                    }
                }
            });
            
            // Al hacer focus, mostrar tip si hay historial
            queryInput.addEventListener('focus', function() {
                if (queryHistory.history.length > 0 && !queryHistory.isNavigating) {
                    showHistoryIndicator(`↑↓ ${queryHistory.history.length} consultas en esta sesión`);
                }
            });
            
            // Al perder focus, resetear navegación
            queryInput.addEventListener('blur', function() {
                setTimeout(() => {
                    if (queryHistory.isNavigating) {
                        queryHistory.resetNavigation();
                        queryInput.classList.remove('navigating-history');
                        updateHistoryCounter();
                    }
                }, 200);
            });
        });

        // Modificar la función sendQuery para agregar al historial
        const originalSendQuery = sendQuery;
        sendQuery = function() {
            const query = document.getElementById('queryInput').value.trim();
            
            // Agregar al historial solo si hay consulta válida
            if (query) {
                queryHistory.add(query);
                queryHistory.resetNavigation();
                document.getElementById('queryInput').classList.remove('navigating-history');
                updateHistoryCounter();
            }
            
            // Llamar a la función original
            originalSendQuery();
        };

        // Función para mostrar/ocultar dropdown de historial (opcional)
        function toggleHistoryDropdown() {
            let dropdown = document.getElementById('historyDropdown');
            if (!dropdown) {
                dropdown = document.createElement('div');
                dropdown.id = 'historyDropdown';
                dropdown.className = 'history-dropdown';
                document.querySelector('.input-area').appendChild(dropdown);
            }
            
            if (dropdown.classList.contains('show')) {
                dropdown.classList.remove('show');
            } else {
                // Llenar con historial reciente
                const recent = queryHistory.getRecent(10);
                if (recent.length > 0) {
                    dropdown.innerHTML = '<div style="padding: 8px 15px; background: #f0f0f0; font-weight: bold; font-size: 12px;">📜 Historial de Sesión Actual</div>' +
                        recent.map((item, index) => {
                        const time = new Date(item.timestamp).toLocaleTimeString('es-ES', {
                            hour: '2-digit',
                            minute: '2-digit'
                        });
                        return `
                            <div class="history-item" onclick="selectFromHistory('${item.query.replace(/'/g, "\\'")}')">
                                ${item.query}
                                <span class="history-item-time">${time}</span>
                            </div>
                        `;
                    }).join('');
                    dropdown.classList.add('show');
                } else {
                    showHistoryIndicator('📭 Sin historial en esta sesión');
                }
            }
        }

        // Función para seleccionar del dropdown
        function selectFromHistory(query) {
            document.getElementById('queryInput').value = query;
            document.getElementById('historyDropdown').classList.remove('show');
            document.getElementById('queryInput').focus();
        }

        // Cerrar dropdown al hacer clic fuera
        document.addEventListener('click', function(e) {
            const dropdown = document.getElementById('historyDropdown');
            if (dropdown && dropdown.classList.contains('show')) {
                if (!e.target.closest('.input-area')) {
                    dropdown.classList.remove('show');
                }
            }
        });

        // Función para obtener estadísticas del historial de sesión
        function getSessionHistoryStats() {
            const status = queryHistory.getStatus();
            const summary = queryHistory.getSessionSummary();
            
            return {
                sessionId: status.sessionId,
                duration: status.sessionDuration,
                totalQueries: status.total,
                queries: summary.queries
            };
        }

        // Agregar información del historial a las estadísticas
        const originalUpdateFeedbackStats = updateFeedbackStats;
        updateFeedbackStats = function() {
            originalUpdateFeedbackStats();
            
            // Agregar estadísticas del historial de sesión
            const stats = getSessionHistoryStats();
            console.log(`📜 Historial de sesión: ${stats.totalQueries} consultas (${stats.duration})`);
        };

        // Agregar indicador visual de sesión en la interfaz
        document.addEventListener('DOMContentLoaded', function() {
            // Crear indicador de sesión
            const sessionIndicator = document.createElement('div');
            sessionIndicator.style.cssText = `
                position: fixed;
                bottom: 10px;
                right: 10px;
                font-size: 11px;
                color: #6c757d;
                background: rgba(255,255,255,0.9);
                padding: 5px 10px;
                border-radius: 15px;
                border: 1px solid #ddd;
                z-index: 100;
            `;
            sessionIndicator.id = 'sessionIndicator';
            document.body.appendChild(sessionIndicator);
            
            // Actualizar indicador cada minuto
            function updateSessionIndicator() {
                const status = queryHistory.getStatus();
                sessionIndicator.innerHTML = `
                    📜 Sesión: ${status.total} consultas | ⏱️ ${status.sessionDuration}
                `;
            }
            
            updateSessionIndicator();
            setInterval(updateSessionIndicator, 60000); // Actualizar cada minuto
        });
    </script>
    
</body>
</html>
"""


# ====================================================================
# ENDPOINTS MODIFICADOS PARA INICIALIZACIÓN LAZY
# ====================================================================

@app.route('/')
def home():
    """Página principal con modo MySQL LAZY"""
    return MYSQL_AUTO_HTML


@app.route('/ask', methods=['POST'])
def ask():
    """
    🎯 ENDPOINT PRINCIPAL: Procesa consultas NLP en modo ClickHouse, 
    asegura la inicialización LAZY y devuelve resultados en formato JSON.
    """
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({
                'success': False,
                'error': 'Pregunta vacía'
            }), 400
        
        print(f"\n🎯 CONSULTA FLASK: '{question}'")
        
        # 🚀 INICIALIZACIÓN LAZY AUTOMÁTICA
        ejecutor, table_metadata, error = get_sistema_actual()
        
        if error or not ejecutor:
            # Error de inicialización (el sistema no está listo)
            return jsonify({
                'success': False,
                'error': f'Sistema no disponible o error de inicialización: {error}',
                'suggestion': 'Verifica la conexión ClickHouse y que existan tablas del TableAnalyzer'
            }), 500
        
        # 🎯 Verificar conexión ClickHouse (¡Clave para el flujo de trabajo!)
        if not ejecutor.clickhouse_manager or not ejecutor.clickhouse_manager.is_connected:
            if hasattr(ejecutor, 'logger'):
                ejecutor.logger.dev_log("❌ Conexión ClickHouse perdida durante consulta Flask", "flask", "error")
            
            return jsonify({
                'success': False,
                'error': 'Conexión ClickHouse perdida. Por favor, reinicie la app.',
                'suggestion': 'Usa el botón de reiniciar para reconectar'
            }), 500
        
        print(f"✅ Sistema LAZY listo - procesando consulta en ClickHouse...")
        
        # 🆕 REGISTRAR CONSULTA EN SESSION MANAGER
        if hasattr(ejecutor, 'logger'):
            ejecutor.logger.dev_log(f"🎯 Consulta Flask: {question}", "flask", "info")
        
        # Procesar consulta
        result = ejecutor.execute_complete_master_flow(question)
        
        if result.get('success', False):
            query_result = result.get('query_result', {})
            execution_result = result.get('execution_result', {})
            
            raw_data = execution_result.get('results', []) 
            card_json = create_adaptive_card_with_table(raw_data)

            if hasattr(ejecutor, 'logger'):
                ejecutor.logger.dev_log(f"✅ Consulta Flask exitosa: {execution_result.get('row_count', 0)} filas", "flask", "info")
            
            response = {
                'success': True,
                'original_input': question,
                'interpretation': query_result.get('interpretation', ''),
                'confidence': query_result.get('confidence', 0.0),
                'sql_query': execution_result.get('sql_executed', ''),
                'row_count': execution_result.get('row_count', 0),
                'formatted_results': result.get('formatted_output', ''),
                'duration_ms': result.get('duration_ms', 0),
                'execution_mode': 'clickhouse_session_manager',
                'adaptive_card_json': card_json  
            }
            print(f"✅ FLASK exitoso: {execution_result.get('row_count', 0)} filas. Card JSON adjunto.")
            return jsonify(response)
        else:
            error_msg = result.get('error', 'Error desconocido')
            print(f"❌ Error FLASK: {error_msg}")
            
            if hasattr(ejecutor, 'logger'):
                ejecutor.logger.dev_log(f"❌ Error consulta Flask: {error_msg}", "flask", "error")
            
            return jsonify({
                'success': False,
                'error': error_msg,
                'step_failed': result.get('step_failed', 'unknown'),
                'suggestions': result.get('suggestions', [])
            }), 400
            
    except Exception as e:
        print(f"❌ Error crítico en /ask FLASK: {str(e)}")
        try:
            ejecutor, _, _ = get_sistema_actual()
            if ejecutor and hasattr(ejecutor, 'logger'):
                ejecutor.logger.log_exception(e, component="flask", context="Error crítico en endpoint /ask")
        except:
            pass
        
        return jsonify({
            'success': False,
            'error': f'Error interno del servidor: {str(e)}'
        }), 500
        

@app.route('/feedback', methods=['POST'])
def user_feedback():
    """
    👍👎 ENDPOINT PARA RETROALIMENTACIÓN DEL USUARIO
    """
    try:
        data = request.get_json()
        query = data.get('query', '')
        response = data.get('response', '')
        satisfied = data.get('satisfied', True)
        comment = data.get('comment', '')
        
        print(f"\n{'👍' if satisfied else '👎'} FEEDBACK: Satisfecho={satisfied}")
        if comment:
            print(f"💬 Comentario: {comment}")
        
        # Obtener ejecutor actual
        ejecutor, _, error = get_sistema_actual()
        
        if error or not ejecutor:
            return jsonify({
                'success': False,
                'error': 'Sistema no inicializado para registrar feedback'
            }), 500
        
        # Registrar feedback
        result = ejecutor.register_user_feedback(query, response, satisfied, comment)
        
        if result['success']:
            # Si el usuario no está satisfecho, forzar guardado de la sesión
            if not satisfied and hasattr(ejecutor, 'logger'):
                print(f"💾 Guardando sesión como problemática debido a feedback negativo")
                ejecutor.logger.dev_log(
                    "💾 Sesión marcada como problemática por feedback negativo del usuario", 
                    "feedback", 
                    "warning"
                )
            
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        print(f"❌ Error en /feedback: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Error registrando feedback: {str(e)}'
        }), 500


@app.route('/feedback-summary')
def feedback_summary():
    """
    📊 RESUMEN DE RETROALIMENTACIÓN DE LA SESIÓN
    """
    try:
        ejecutor, _, error = get_sistema_actual()
        
        if error or not ejecutor:
            return jsonify({
                'error': 'Sistema no inicializado'
            })
        
        summary = ejecutor.get_feedback_summary()
        return jsonify(summary)
        
    except Exception as e:
        return jsonify({
            'error': f'Error obteniendo resumen: {str(e)}'
        }), 500


@app.route('/feedback-report')
def feedback_report():
    """
    📊 REPORTE DE SESIONES CON FEEDBACK NEGATIVO
    """
    try:
        # Importar la función del ejecutor
        from ejecutor import get_negative_feedback_report
        
        report = get_negative_feedback_report()
        
        # Agregar información adicional para la UI
        if report['status'] == 'success':
            report['ui_summary'] = {
                'has_negative_feedback': report['total_negative_feedbacks'] > 0,
                'severity': 'high' if report['total_negative_feedbacks'] > 10 else 
                            'medium' if report['total_negative_feedbacks'] > 5 else 'low',
                'top_complaints': list(report.get('common_complaints', {}).keys())[:5]
            }
        
        return jsonify(report)
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error generando reporte: {str(e)}'
        }), 500


@app.route('/feedback-analysis')
def feedback_analysis():
    """
    🔍 ANÁLISIS DE PATRONES DE FEEDBACK
    """
    try:
        from ejecutor import analyze_feedback_patterns
        
        analysis = analyze_feedback_patterns()
        return jsonify(analysis)
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error analizando patrones: {str(e)}'
        }), 500


@app.route('/feedback-dashboard')
def feedback_dashboard():
    """
    📊 DASHBOARD DE FEEDBACK (PÁGINA HTML)
    """
    dashboard_html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard de Retroalimentación - CABI</title>
        <style>
            body { font-family: Arial; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .card { background: white; padding: 20px; margin: 10px 0; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
            .metric { display: inline-block; margin: 10px 20px; }
            .metric-value { font-size: 36px; font-weight: bold; color: #2c3e50; }
            .metric-label { font-size: 14px; color: #7f8c8d; }
            .severity-high { color: #e74c3c; }
            .severity-medium { color: #f39c12; }
            .severity-low { color: #27ae60; }
            .feedback-item { background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #e74c3c; }
            .recommendation { background: #e8f5e9; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #4caf50; }
            h1, h2 { color: #2c3e50; }
            .loading { text-align: center; padding: 40px; color: #7f8c8d; }
            .error { background: #f8d7da; color: #721c24; padding: 20px; border-radius: 5px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Dashboard de Retroalimentación</h1>
            
            <div id="summary" class="card">
                <h2>Resumen General</h2>
                <div id="summaryContent" class="loading">Cargando datos...</div>
            </div>
            
            <div id="patterns" class="card">
                <h2>Análisis de Patrones</h2>
                <div id="patternsContent" class="loading">Analizando patrones...</div>
            </div>
            
            <div id="recent" class="card">
                <h2>Feedback Reciente</h2>
                <div id="recentContent" class="loading">Cargando feedback reciente...</div>
            </div>
            
            <div id="recommendations" class="card">
                <h2>Recomendaciones</h2>
                <div id="recommendationsContent" class="loading">Generando recomendaciones...</div>
            </div>
        </div>
        
        <script>
            // Cargar resumen
            fetch('/feedback-report')
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    const severity = data.ui_summary?.severity || 'low';
                    const severityClass = `severity-${severity}`;
                    
                    document.getElementById('summaryContent').innerHTML = `
                        <div class="metric">
                            <div class="metric-value ${severityClass}">${data.total_negative_feedbacks}</div>
                            <div class="metric-label">Feedbacks Negativos</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value">${data.total_sessions_with_negative_feedback}</div>
                            <div class="metric-label">Sesiones Afectadas</div>
                        </div>
                        <div class="metric">
                            <div class="metric-value ${severityClass}">${severity.toUpperCase()}</div>
                            <div class="metric-label">Nivel de Severidad</div>
                        </div>
                    `;
                    
                    // Mostrar feedback reciente
                    if (data.sessions && data.sessions.length > 0) {
                        let recentHtml = '';
                        data.sessions.slice(0, 5).forEach(session => {
                            session.feedbacks.forEach(feedback => {
                                recentHtml += `
                                    <div class="feedback-item">
                                        <strong>Consulta:</strong> ${feedback.query}<br>
                                        <strong>Comentario:</strong> ${feedback.comment}<br>
                                        <small>Sesión: ${session.session_id}</small>
                                    </div>
                                `;
                            });
                        });
                        document.getElementById('recentContent').innerHTML = recentHtml;
                    } else {
                        document.getElementById('recentContent').innerHTML = '<p>No hay feedback negativo reciente</p>';
                    }
                } else {
                    document.getElementById('summaryContent').innerHTML = '<div class="error">Error cargando datos</div>';
                }
            })
            .catch(error => {
                document.getElementById('summaryContent').innerHTML = '<div class="error">Error de conexión</div>';
            });
            
            // Cargar análisis
            fetch('/feedback-analysis')
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    let patternsHtml = '<h3>Tipos de Problemas</h3><ul>';
                    const issues = data.patterns.response_issues;
                    for (let issue in issues) {
                        if (issues[issue] > 0) {
                            patternsHtml += `<li>${issue.replace(/_/g, ' ')}: ${issues[issue]} casos</li>`;
                        }
                    }
                    patternsHtml += '</ul>';
                    
                    document.getElementById('patternsContent').innerHTML = patternsHtml;
                    
                    // Mostrar recomendaciones
                    if (data.recommendations && data.recommendations.length > 0) {
                        let recsHtml = '';
                        data.recommendations.forEach(rec => {
                            recsHtml += `<div class="recommendation">${rec}</div>`;
                        });
                        document.getElementById('recommendationsContent').innerHTML = recsHtml;
                    } else {
                        document.getElementById('recommendationsContent').innerHTML = '<p>No hay recomendaciones disponibles</p>';
                    }
                }
            })
            .catch(error => {
                document.getElementById('patternsContent').innerHTML = '<div class="error">Error cargando análisis</div>';
            });
        </script>

    </body>
    </html>
    '''
    return dashboard_html


@app.route('/table-info')
def table_info():
    """
    📋 INFORMACIÓN DEL SISTEMA LAZY: Devuelve el estado de la conexión ClickHouse 
    y la metadata de la tabla activa.
    """
    try:
        ejecutor, table_metadata, error = get_sistema_actual()
        
        # 1. Manejo de estado de inicialización
        if _sistema_estado['inicializando']:
            return jsonify({'loaded': False, 'mode': 'clickhouse', 'initializing': True, 'message': 'Sistema inicializándose...'})
        
        if error or not ejecutor:
            return jsonify({'loaded': False, 'mode': 'clickhouse', 'error': str(error) or 'Sistema no inicializado', 'initializing': False})

        # 2. Obtener información del ejecutor
        # Asumiendo que get_master_table_info() devuelve la información de ClickHouse
        info = ejecutor.get_master_table_info()
        
        # 3. Función auxiliar para manejar bytes
        def convert_bytes_to_string(obj):
            if isinstance(obj, dict):
                return {k: convert_bytes_to_string(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_bytes_to_string(v) for v in obj]
            elif isinstance(obj, bytes):
                return obj.decode('utf-8', errors='ignore')
            else:
                return obj
        
        # Limpiar y asegurar strings
        info = convert_bytes_to_string(info)
        
        # 4. Asegurar que el estado y la metadata sean correctos
        # Cambiamos las claves para reflejar ClickHouse
        if 'mysql_status' in info:
            info['clickhouse_status'] = info.pop('mysql_status')
        info['mode'] = 'clickhouse'
        info['initializing'] = False

        if table_metadata:
            info['table_metadata'] = {
                'original_filename': str(table_metadata.get('original_filename', 'N/A')),
                'analyzed_at': str(table_metadata.get('analyzed_at', 'N/A')),
                'dimensions_count': int(table_metadata.get('dimensions_count', 0)),
                'metrics_count': int(table_metadata.get('metrics_count', 0))
            }
        
        return jsonify(info)
        
    except Exception as e:
        print(f"❌ Error obteniendo información de tabla: {str(e)}")
        return jsonify({'loaded': False, 'mode': 'clickhouse', 'error': f'Error obteniendo información: {str(e)}', 'initializing': False}), 500
    


@app.route('/force-reinit', methods=['POST'])
def force_reinit():
    """
    🔄 ENDPOINT PARA FORZAR REINICIALIZACIÓN
    """
    try:
        print("🔄 Endpoint: Forzando reinicialización...")
        ejecutor, table_metadata, error = forzar_reinicializacion()
        
        if error:
            return jsonify({
                'success': False,
                'error': error
            }), 500
        
        return jsonify({
            'success': True,
            'message': 'Sistema reinicializado exitosamente',
            'table_name': table_metadata['table_name'] if table_metadata else None
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error en reinicialización: {str(e)}'
        }), 500


@app.route('/stats')
def stats():
    """📊 Estadísticas del sistema LAZY"""
    try:
        ejecutor, table_metadata, error = get_sistema_actual()
        
        if error or not ejecutor:
            return jsonify({'error': 'Sistema no inicializado'})
        
        stats = ejecutor.get_master_session_stats()
        
        # Agregar información específica LAZY
        stats['initialization_mode'] = 'lazy'
        stats['mysql_mode'] = True
        stats['auto_connected'] = True
        if table_metadata:
            stats['table_source'] = table_metadata['original_filename']
            stats['table_created'] = str(table_metadata['analyzed_at'])
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({
            'error': f'Error obteniendo estadísticas: {str(e)}'
        }), 500


@app.route('/health')
def health():
    """🏥 Estado de salud del sistema LAZY"""
    health_status = {
        'status': 'healthy',
        'mode': 'mysql_lazy',
        'initialized': _sistema_estado['inicializado'],
        'initializing': _sistema_estado['inicializando'],
        'mysql_connected': False,
        'table_available': False
    }
    
    if _sistema_estado['inicializado'] and _sistema_estado['ejecutor']:
        health_status['mysql_connected'] = (
            _sistema_estado['ejecutor'].mysql_manager and 
            _sistema_estado['ejecutor'].mysql_manager.is_connected
        )
        health_status['table_available'] = _sistema_estado['table_metadata'] is not None
        
        if _sistema_estado['table_metadata']:
            health_status['table_name'] = _sistema_estado['table_metadata']['table_name']
            health_status['table_rows'] = _sistema_estado['table_metadata']['total_rows']
    
    if _sistema_estado['error_inicializacion']:
        health_status['status'] = 'error'
        health_status['error'] = _sistema_estado['error_inicializacion']
    
    return jsonify(health_status)


@app.route('/session-info')
def session_info():
    """📋 Información de la sesión Flask actual"""
    try:
        ejecutor, _, _ = get_sistema_actual()
        if ejecutor and hasattr(ejecutor, 'logger'):
            session_info = ejecutor.logger.get_session_info()
            return jsonify(session_info)
        else:
            return jsonify({
                'error': 'No hay sesión activa'
            })
    except Exception as e:
        return jsonify({
            'error': f'Error obteniendo info de sesión: {str(e)}'
        }), 500


@app.route('/user-sessions-report')
def user_sessions_report():
    """📊 Reporte de sesiones de usuarios"""
    try:
        report = get_all_sessions_report() 
        return jsonify(report)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error obteniendo reporte: {str(e)}'
        }), 500

# ====================================================================
# PUNTO DE ENTRADA
# ====================================================================

import socket

def get_local_ip():
    """Obtener IP local de la red"""
    try:
        # Conectar a una dirección externa para obtener la IP local
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "No se pudo obtener la IP"

if __name__ == '__main__':
    local_ip = get_local_ip()
    
    print("\n" + "="*60)
    print("🎯 CABI 0.0.5 - URLs de Acceso")
    print("="*60)
    print(f"📱 Local (esta computadora):  http://localhost:5001")
    print(f"🌐 Red WiFi (otros dispositivos): http://{local_ip}:5001")
    print("="*60)
    print("🔗 Comparte esta URL con los otros dispositivos:")
    print(f"   http://{local_ip}:5001")
    print("="*60)
    
    app.run(debug=True, host='0.0.0.0', port=5001)