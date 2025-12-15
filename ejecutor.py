import sqlite3
import pandas as pd
import json
import sys
import io
import os
import re
import shutil
import threading
import atexit
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from contextlib import contextmanager
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import time
import hashlib


# Imports opcionales
try:
    import clickhouse_connect
    HAS_CLICKHOUSE = True
except ImportError:
    HAS_CLICKHOUSE = False
    clickhouse_connect = None
    
    
"""
=============================================================================
                    CABI EJECUTOR - SISTEMA MAESTRO REFACTORIZADO
=============================================================================

Coordinador maestro para consultas en lenguaje natural con dos modos:
- MODO TRADICIONAL: Carga de archivos CSV/Excel en memoria volátil
- MODO SQL: Consultas directas sobre base de datos

Arquitectura:
    TableAnalyzer → Problemizador → SQLSchemaMapper → Ejecutor → Interfaz

Versión: 2.0 (Refactorizada)
"""

# =============================================================================
# POOL DE CONEXIONES MYSQL
# =============================================================================

# =============================================================================
# GESTOR DE CONSULTAS CONCURRENTES
# =============================================================================

# =============================================================================
# 2. CONFIGURACIONES Y CONSTANTES
# =============================================================================

# =============================================================================
# CLICKHOUSE MANAGER INTEGRADO
# =============================================================================

class UnifiedClickHouseManager:
    """Manager de ClickHouse integrado en el ejecutor"""
    
    def __init__(self, clickhouse_config: Dict[str, Any], selected_table: str, logger=None):
        """Inicializar manager"""
        self.config = clickhouse_config
        self.selected_table = selected_table
        self.logger = logger
        self.is_connected = False
        self.client = None
        
        # Metadatos
        self.table_info = {}
        self.table_schema = {}
        self.tableanalyzer_metadata = {}
        
        # Conectar
        self._connect()
        
        if self.is_connected:
            self._load_table_metadata()
        
    def _connect(self):
        """Establecer conexión con ClickHouse"""
        try:
            print("🔌 Intentando conectar a ClickHouse Cloud...") 
            
            self.client = clickhouse_connect.get_client(
                host=self.config.get('host', 'amj0c9lgbe.us-west-2.aws.clickhouse.cloud'),
                port=self.config.get('port', 8443),
                database=self.config.get('database', 'datos_imperiales'),
                username=self.config.get('user', 'default'),
                password=self.config.get('password', 'c1.i4f8KmZ5HP'),
                
                # --- CORRECCIONES CLAVE ---
                secure=True,    # Puerto 8443 requiere SSL
                verify=False    # <--- IMPORTANTE: Esto debe ser False directo
                # --------------------------
            )
            
            # Verificar conexión con una consulta simple
            self.client.query("SELECT 1")
            self.is_connected = True
            print("✅ ¡CONEXIÓN EXITOSA! ClickHouse está listo.")
            
            if self.logger:
                self.logger.dev_log(f"✅ ClickHouse conectado", "clickhouse")
        
        except Exception as e:
            self.is_connected = False
            print(f"\n❌ ERROR CRÍTICO DE CONEXIÓN: {e}")
            print("💡 PISTA: Si el error menciona SSL, asegúrate de que verify=False esté puesto.\n")
            if self.logger:
                self.logger.log_exception(e, "clickhouse", "Error conectando")
            raise
            
            
    def _load_table_metadata(self):
            """Cargar metadatos de la tabla"""
            if not self.is_connected:
                return
            
            try:
                # Buscar en tablas_metadata
                metadata_query = f"""
                    SELECT 
                        original_filename,
                        file_path,
                        total_rows,
                        total_columns,
                        dimensions_count,
                        metrics_count,
                        analyzed_at
                    FROM tablas_metadata
                    WHERE table_name = '{self.selected_table}'
                    ORDER BY analyzed_at DESC
                    LIMIT 1
                """
                
                result = self.client.query(metadata_query)
                
                if result.result_rows:
                    row = result.result_rows[0]
                    self.tableanalyzer_metadata = {
                        'original_filename': row[0],
                        'file_path': row[1],
                        'total_rows': row[2],
                        'total_columns': row[3],
                        'dimensions_count': row[4],
                        'metrics_count': row[5],
                        'analyzed_at': row[6],
                        'source': 'TableAnalyzer'
                    }
                    
                    if self.logger:
                        self.logger.dev_log(f"✅ Metadata encontrada para {self.selected_table}", "clickhouse")
                else:
                    self.tableanalyzer_metadata = {'source': 'Unknown'}
                
                # Obtener esquema de columnas
                columns_query = f"""
                    SELECT name, type
                    FROM system.columns
                    WHERE database = '{self.config.get('database', 'datos_imperiales')}'
                    AND table = '{self.selected_table}'
                    AND name NOT IN ('id', 'created_at')
                """
                
                columns_result = self.client.query(columns_query)
                
                self.table_schema = {
                    'columns': [],
                    'column_types': {}
                }
                
                for row in columns_result.result_rows:
                    col_name = row[0]
                    col_type = row[1]
                    self.table_schema['columns'].append(col_name)
                    self.table_schema['column_types'][col_name] = col_type
                
                # Obtener información de la tabla
                count_query = f"SELECT count() FROM {self.selected_table}"
                count_result = self.client.query(count_query)
                rows_count = count_result.result_rows[0][0] if count_result.result_rows else 0
                
                self.table_info = {
                    'name': self.selected_table,
                    'database': self.config.get('database', 'datos_imperiales'),
                    'columns_count': len(self.table_schema['columns']),
                    'rows_count': rows_count,
                    'data_size_mb': 0
                }
                
                if self.logger:
                    self.logger.dev_log(
                        f"✅ Tabla cargada: {rows_count:,} filas, {len(self.table_schema['columns'])} columnas", 
                        "clickhouse"
                    )
            
            except Exception as e:
                if self.logger:
                    self.logger.dev_log(f"❌ Error cargando metadatos: {e}", "clickhouse", "error")
                self.table_schema = {'columns': [], 'column_types': {}}
                self.table_info = {'name': self.selected_table, 'rows_count': 0}
    
    def execute_query(self, sql_query: str, user_id: str = None) -> Dict[str, Any]:
        """Ejecutar consulta SQL"""
        if not self.is_connected:
            return {'success': False, 'error': 'ClickHouse no conectado'}
        
        query_start = datetime.now()
        user_id = user_id or 'default'
        
        try:
            # Adaptar SQL para ClickHouse
            final_query = self._adapt_sql_for_clickhouse(sql_query)
            
            if self.logger:
                self.logger.dev_log(f"🚀 Ejecutando en ClickHouse", "clickhouse")
                self.logger.dev_log(f"   SQL: {final_query}", "clickhouse")
            
            # Ejecutar consulta
            result = self.client.query(final_query)
            
            # Convertir resultados
            results = []
            columns = list(result.column_names) if hasattr(result, 'column_names') else []
            
            for row in result.result_rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                results.append(row_dict)
            
            execution_time_ms = int((datetime.now() - query_start).total_seconds() * 1000)
            
            # Registrar en log
            self._log_query(
                self.selected_table, final_query,
                execution_time_ms, len(results), True, '', user_id
            )
            
            if self.logger:
                self.logger.dev_log(
                    f"✅ Query ejecutada: {len(results)} filas en {execution_time_ms}ms", 
                    "clickhouse"
                )
            
            return {
                'success': True,
                'results': results,
                'columns': columns,
                'row_count': len(results),
                'execution_time_ms': execution_time_ms,
                'sql_executed': final_query,
                'sql_original': sql_query,
                'execution_mode': 'clickhouse'
            }
        
        except Exception as e:
            execution_time_ms = int((datetime.now() - query_start).total_seconds() * 1000)
            
            # Registrar error
            self._log_query(
                self.selected_table, sql_query,
                execution_time_ms, 0, False, str(e)[:500], user_id
            )
            
            if self.logger:
                self.logger.log_exception(e, "clickhouse", "Error ejecutando query")
            
            return {
                'success': False,
                'error': str(e),
                'sql_attempted': final_query if 'final_query' in locals() else sql_query
            }
    
    def _adapt_sql_for_clickhouse(self, sql_query: str) -> str:
        """Adaptar SQL para ClickHouse"""
        # Reemplazar backticks con comillas dobles
        query = sql_query.replace('`', '"')
        
        # Reemplazar tabla 'datos' con tabla real
        query = re.sub(r'\bdatos\b', self.selected_table, query, flags=re.IGNORECASE)
        
        # TOTAL() -> sum()
        query = re.sub(r'\bTOTAL\s*\(', 'sum(', query, flags=re.IGNORECASE)
        
        return query
    
    def _log_query(self, table_used: str, query_text: str,
                execution_time_ms: int, results_count: int, success: bool,
                error_message: str, user_id: str):
        """Registrar consulta en consultas_log"""
        try:
            query_id = int(time.time() * 1000000)
            
            # Detectar tipo de query
            query_upper = query_text.upper().strip()
            if query_upper.startswith('SELECT'):
                if 'GROUP BY' in query_upper:
                    query_type = 'AGGREGATION'
                else:
                    query_type = 'SELECT'
            else:
                query_type = 'OTHER'
            
            # Generar hash
            query_hash = hashlib.md5(query_text.encode()).hexdigest()
            
            # Insertar en log
            log_data = [[
                query_id,
                table_used,
                query_text[:1000],
                query_type,
                execution_time_ms,
                results_count,
                1 if success else 0,
                error_message,
                datetime.now(),
                user_id,
                f"session_{datetime.now().strftime('%Y%m%d')}",
                query_hash
            ]]
            
            self.client.insert(
                'consultas_log',
                log_data,
                column_names=['id', 'table_used', 'query_text', 'query_type',
                            'execution_time_ms', 'results_count', 'success',
                            'error_message', 'created_at', 'user_id', 
                            'session_id', 'query_hash']
            )
        
        except Exception as e:
            if self.logger:
                self.logger.dev_log(f"⚠️ No se pudo registrar en log: {e}", "clickhouse", "warning")
    
    def get_table_info(self) -> Dict[str, Any]:
        """Obtener información de la tabla"""
        return {
            'connected': self.is_connected,
            'table_info': self.table_info,
            'schema': self.table_schema,
            'tableanalyzer_metadata': self.tableanalyzer_metadata,
            'config': {
                'host': self.config.get('host'),
                'database': self.config.get('database', 'datos_imperiales'),
                'table_name': self.selected_table
            }
        }
    
    def show_table_preview(self, limit: int = 10) -> str:
        """Mostrar preview de la tabla"""
        if not self.is_connected:
            return "❌ No hay conexión ClickHouse"
        
        try:
            result = self.execute_query(f"SELECT * FROM {self.selected_table} LIMIT {limit}")
            
            if not result.get('success'):
                return f"❌ Error: {result.get('error')}"
            
            df = pd.DataFrame(result['results'])
            
            output = []
            output.append(f"\n📊 TABLA: {self.selected_table}")
            output.append("=" * 70)
            
            if self.tableanalyzer_metadata.get('source') == 'TableAnalyzer':
                meta = self.tableanalyzer_metadata
                output.append(f"📄 Archivo: {meta.get('original_filename')}")
                output.append(f"📊 Filas: {meta.get('total_rows'):,}")
            
            output.append(f"\n📋 MUESTRA ({len(df)} filas):")
            output.append(df.to_string())
            
            return "\n".join(output)
        
        except Exception as e:
            return f"❌ Error: {e}"
    
    def close(self):
        """Cerrar conexión"""
        try:
            if self.client:
                self.client.close()
                self.is_connected = False
            
            if self.logger:
                self.logger.dev_log("✅ ClickHouse cerrado", "clickhouse")
        
        except Exception as e:
            if self.logger:
                self.logger.log_exception(e, "clickhouse", "Error cerrando ClickHouse")
    
    # Métodos de compatibilidad
    def optimize_mysql_performance(self):
        return True
    
    def get_optimization_status(self):
        return {'optimized': True, 'connection_active': self.is_connected}



class ExecutionMode(Enum):
    """Modos de ejecución del sistema"""
    TRADITIONAL = "traditional"
    CLICKHOUSE = "clickhouse"
    DEVELOPER = "developer"


@dataclass
class SystemConfiguration:
    """Configuración centralizada del sistema"""
    
    # Configuración de logging
    DEVELOPER_MODE: bool = field(default_factory=lambda: _detect_developer_mode())
    CONSOLE_CLEAN: bool = True
    STORE_ONLY_PROBLEMS: bool = True
    AUTO_FLUSH_ON_ERROR: bool = True
    ERROR_THRESHOLD: int = 1
    
    # Configuración de MySQL por defecto
    DEFAULT_CLICKHOUSE_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "host": os.getenv("CLICKHOUSE_HOST", "amj0c9lgbe.us-west-2.aws.clickhouse.cloud"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8443")),
        "database": os.getenv("CLICKHOUSE_DATABASE", "datos_imperiales"),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "c1.i4f8KmZ5HP"),
        "secure": False,
        "verify": False,
        "compress": True
    })
    
    # Límites del sistema
    MAX_SESSIONS_PER_USER: int = 1
    DEFAULT_USER_ID: str = "local_user"
    CLEANUP_DAYS_DEFAULT: int = 30
    
    # Paths del sistema
    CONTROL_DIR: Path = field(default_factory=lambda: Path("./control"))
    ERRORS_DIR: Path = field(default_factory=lambda: Path("./control/errores_encontrados"))
    
    def __post_init__(self):
        """Validaciones post-inicialización"""
        self.CONTROL_DIR.mkdir(parents=True, exist_ok=True)
        self.ERRORS_DIR.mkdir(parents=True, exist_ok=True)


def _detect_developer_mode() -> bool:
    """Auto-detectar si se ejecuta desde ejecutor.py directamente"""
    main_frame = sys._getframe()
    while main_frame.f_back is not None:
        main_frame = main_frame.f_back
    
    main_script = main_frame.f_globals.get('__file__', '')
    main_script_name = Path(main_script).name if main_script else ''
    
    return main_script_name == 'ejecutor.py'

# Configuración global del sistema
CONFIG = SystemConfiguration()

# =============================================================================
# 3. GESTIÓN DE CONTEXTO Y ESTADO GLOBAL
# =============================================================================

class ApplicationContext:
    """Contexto centralizado de la aplicación para evitar variables globales"""
    
    def __init__(self):
        self._current_session_id: Optional[str] = None
        self._current_session_logger: Optional['BaseLogger'] = None
        self._components: Optional[Dict[str, Any]] = None
        self._session_manager: Optional['UserSessionManager'] = None
        self._lock = threading.Lock()
    
    def set_current_session_logger(self, logger: 'BaseLogger'):
        """Establecer logger de sesión activa"""
        with self._lock:
            self._current_session_id = logger.session_id
            self._current_session_logger = logger
    
    def get_current_session_logger(self) -> Optional['BaseLogger']:
        """Obtener logger de sesión activa"""
        with self._lock:
            return self._current_session_logger
    
    def clear_current_session(self):
        """Limpiar sesión activa"""
        with self._lock:
            self._current_session_id = None
            self._current_session_logger = None
    
    def get_components(self) -> Dict[str, Any]:
        """Obtener componentes con lazy loading"""
        if self._components is None:
            self._components = self._verify_and_import_components()
        return self._components or {}
    
    def _verify_and_import_components(self) -> Optional[Dict[str, Any]]:
        """Verificar e importar componentes del sistema"""
        components = {}
        logger = self.get_current_session_logger()
        
        if logger:
            logger.user_message("Verificando componentes del sistema", "processing")
        
        try:
            with suppress_component_output("component_imports"):
        # TableAnalyzer - OBLIGATORIO
                try:
                    from analizador_esquemas.analizador_esquemas_4 import TableAnalyzer
                    components['TableAnalyzer'] = TableAnalyzer
                    if logger:
                        logger.dev_log("✅ TableAnalyzer: Importado correctamente", "main")
                except ImportError as e:
                    if logger:
                        logger.user_message("Error: TableAnalyzer no disponible", "error")
                        logger.dev_log(f"❌ CRÍTICO: TableAnalyzer no disponible - {e}", "main", "error")
                    return None
                
        # Problemizador - OBLIGATORIO
                try:
                    from problemizador_18 import UnifiedNLPParser, SQLSchemaMapper
                    components['UnifiedNLPParser'] = UnifiedNLPParser
                    components['SQLSchemaMapper'] = SQLSchemaMapper
                    if logger:
                        logger.dev_log("✅ Problemizador: Importado correctamente", "main")
                except ImportError as e:
                    if logger:
                        logger.user_message("Error: Problemizador no disponible", "error")
                        logger.dev_log(f"❌ CRÍTICO: Problemizador no disponible - {e}", "main", "error")
                    return None
            
            if logger:
                logger.user_message("Componentes verificados exitosamente", "success")
            
            return components
            
        except Exception as e:
            if logger:
                logger.dev_log(f"❌ Error en verificación de componentes: {e}", "main", "error")
            return None

# Contexto global de la aplicación
APP_CONTEXT = ApplicationContext()


# =============================================================================
# 4. UTILIDADES Y DECORADORES
# =============================================================================

@contextmanager
def suppress_component_output(operation_name: str = "unknown"):
    """Suprimir prints y guardar en sesión específica activa"""
    
    if CONFIG.DEVELOPER_MODE:
        print(f" \n 🔧 [DEV-OUTPUT] ═══ INICIANDO: {operation_name} ═══")
        yield
        print(f"🔧 [DEV-OUTPUT] ═══ COMPLETADO: {operation_name} ═══ \n ")
        return
    
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    session_logger = APP_CONTEXT.get_current_session_logger()
    
    try:
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        
        sys.stdout = stdout_buffer
        sys.stderr = stderr_buffer
        
        yield
        
        stdout_content = stdout_buffer.getvalue()
        stderr_content = stderr_buffer.getvalue()
        
        if session_logger and (stdout_content or stderr_content):
            suppressed_entry = {
                'session_id': session_logger.session_id,
                'timestamp': datetime.now().isoformat(),
                'operation_name': operation_name,
                'stdout_content': stdout_content.strip(),
                'stderr_content': stderr_content.strip(),
                'has_content': bool(stdout_content.strip() or stderr_content.strip())
            }
            
            session_logger.memory_buffer['context'].append({
                'timestamp': datetime.now().isoformat(),
                'component': 'suppressed_output',
                'level': 'suppressed_component',
                'session_id': session_logger.session_id,
                'operation': operation_name,
                'suppressed_data': suppressed_entry
            })
    
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr


def fix_sql_column_quotes(sql_query: str, dataframe_columns: List[str]) -> str:
    """Arreglar comillas dobles en columnas SQL"""
    def column_replacer(match):
        quoted_col = match.group(1)
        
        if quoted_col in dataframe_columns:
            if ' ' in quoted_col or '-' in quoted_col or any(c in quoted_col for c in ['(', ')', '[', ']']):
                return f'`{quoted_col}`'
            else:
                return quoted_col
        
        for col in dataframe_columns:
            if col.lower() == quoted_col.lower():
                if ' ' in col or '-' in col or any(c in col for c in ['(', ')', '[', ']']):
                    return f'`{col}`'
                else:
                    return col
        
        return f'`{quoted_col}`'
    
    pattern = r'"([^"]+)"'
    return re.sub(pattern, column_replacer, sql_query)


# =============================================================================
# 5. SISTEMA DE LOGGING UNIFICADO
# =============================================================================

class BaseLogger(ABC):
    """Clase base abstracta para todos los loggers"""
    
    def __init__(self, session_name: str = "base_session"):
        self.session_name = session_name
        self.session_start = datetime.now()
        self.session_id = self._generate_session_id()
        
        # Estado común
        self.has_problems = False
        self.error_count = 0
        self.warning_count = 0
        self.current_operation = None
        
        # Buffer de memoria común
        self.memory_buffer = {
            'errors': [],
            'warnings': [],
            'context': [],
            'operations': [],
            'stats': {
                'total_operations': 0,
                'failed_operations': 0,
                'components_used': set()
            }
        }
        
        # Auto-registrarse como sesión activa
        APP_CONTEXT.set_current_session_logger(self)
    
    def _generate_session_id(self) -> str:
        """Generar ID único de sesión"""
        prefix = "dev" if CONFIG.DEVELOPER_MODE else "user"
        timestamp = self.session_start.strftime('%Y%m%d_%H%M%S')
        return f"{prefix}_{timestamp}"
    
    @abstractmethod
    def user_message(self, message: str, level: str = "info"):
        """Mensaje para el usuario"""
        pass
    
    @abstractmethod
    def dev_log(self, message: str, component: str = "main", level: str = "info"):
        """Log para desarrollador"""
        pass
    
    def start_operation(self, operation_name: str, component: str = "main") -> Dict[str, Any]:
        """Iniciar operación con tracking"""
        self.current_operation = {
            'name': operation_name,
            'component': component,
            'start_time': datetime.now(),
            'status': 'running'
        }
        
        self.memory_buffer['stats']['total_operations'] += 1
        self.user_message(f"{operation_name}...", "processing")
        self.dev_log(f"🔄 INICIANDO: {operation_name}", component, "info")
        
        return self.current_operation
    
    def update_operation(self, message: str, progress: int = None):
        """Actualizar progreso de operación"""
        if self.current_operation:
            progress_msg = f" ({progress}%)" if progress else ""
            self.dev_log(f"├── {message}{progress_msg}", self.current_operation['component'], "info")
    
    def complete_operation(self, success: bool = True, message: str = "") -> int:
        """Completar operación"""
        if not self.current_operation:
            return 0
        
        duration = datetime.now() - self.current_operation['start_time']
        duration_ms = int(duration.total_seconds() * 1000)
        
        if success:
            self.user_message(f"✅ {self.current_operation['name']} completado", "success")
            self.dev_log(f"✅ SUCCESS: {self.current_operation['name']} ({duration_ms}ms)", 
                        self.current_operation['component'], "info")
        else:
            self.memory_buffer['stats']['failed_operations'] += 1
            self.user_message(f"❌ Error en {self.current_operation['name']}: {message}", "error")
            self.dev_log(f"❌ FAILED: {self.current_operation['name']} ({duration_ms}ms) - {message}", 
                        self.current_operation['component'], "error")
        
        # Registrar operación
        self.memory_buffer['operations'].append({
            'name': self.current_operation['name'],
            'component': self.current_operation['component'],
            'duration_ms': duration_ms,
            'success': success,
            'error_message': message if not success else None,
            'timestamp': self.current_operation['start_time'].isoformat()
        })
        
        self.current_operation = None
        return duration_ms
    
    
    def log_exception(self, exception: Exception, component: str = "main", context: str = ""):
        """Manejar excepciones"""
        error_details = {
            'exception_type': type(exception).__name__,
            'exception_message': str(exception),
            'context': context,
            'traceback': traceback.format_exc()
        }
        
        self.dev_log(f"🚨 EXCEPTION: {error_details['exception_type']} - {error_details['exception_message']}", 
                    component, "error")
        
        self.memory_buffer['context'].append({
            'timestamp': datetime.now().isoformat(),
            'component': component,
            'level': 'exception_details',
            'error_details': error_details
        })


    def log_user_feedback(self, query: str, response: str, satisfied: bool, comment: str = ""):
        """Registrar retroalimentación del usuario sobre una respuesta"""
        feedback_entry = {
            'timestamp': datetime.now().isoformat(),
            'component': 'user_feedback',
            'level': 'feedback',
            'query': query,
            'response_preview': response[:500] if len(response) > 500 else response,
            'satisfied': satisfied,
            'comment': comment,
            'session_id': self.session_id
        }
        
        if not satisfied:
            # Marcar como problema si el usuario no está satisfecho
            self.has_problems = True
            self.memory_buffer['context'].append({
                'timestamp': datetime.now().isoformat(),
                'component': 'user_feedback',
                'level': 'negative_feedback',
                'feedback_data': feedback_entry
            })
            
            # Agregar a warnings para que se guarde
            self.memory_buffer['warnings'].append({
                'timestamp': datetime.now().isoformat(),
                'component': 'user_satisfaction',
                'level': 'warning',
                'message': f'Usuario no satisfecho con respuesta: {comment}',
                'query': query,
                'response_length': len(response)
            })
            
            self.warning_count += 1
            
            # Log para desarrollador
            self.dev_log(f"👎 Retroalimentación negativa recibida", "feedback", "warning")
            self.dev_log(f"   Consulta: {query[:100]}...", "feedback", "warning")
            self.dev_log(f"   Comentario: {comment}", "feedback", "warning")
            
            # Auto-flush si está configurado
            if hasattr(self, '_flush_errors_to_disk') and self.has_problems:
                self._flush_errors_to_disk()
        else:
            # Feedback positivo solo se registra en memoria
            self.memory_buffer['context'].append({
                'timestamp': datetime.now().isoformat(),
                'component': 'user_feedback',
                'level': 'positive_feedback',
                'feedback_data': feedback_entry
            })
            
            self.dev_log(f"👍 Retroalimentación positiva recibida", "feedback", "info")
        
    
    def show_user_results(self, results_text: str):
        """Mostrar resultados al usuario"""
        print(results_text)
    
    
    def get_suppressed_content(self) -> List[Dict]:
        """Obtener contenido suprimido"""
        suppressed_content = []
        for entry in self.memory_buffer['context']:
            if (entry.get('component') == 'suppressed_output' and 
                entry.get('level') == 'suppressed_component' and
                entry.get('session_id') == self.session_id):
                
                suppressed_data = entry.get('suppressed_data', {})
                if suppressed_data.get('has_content'):
                    suppressed_content.append(suppressed_data)
        
        return suppressed_content
    
    
    def get_session_info(self) -> Dict[str, Any]:
        """Obtener información de la sesión"""
        return {
            'session_id': self.session_id,
            'session_name': self.session_name,
            'start_time': self.session_start.isoformat(),
            'duration': str(datetime.now() - self.session_start),
            'has_problems': self.has_problems,
            'error_count': self.error_count,
            'warning_count': self.warning_count,
            'operations_count': len(self.memory_buffer['operations']),
            'mode': 'developer' if CONFIG.DEVELOPER_MODE else 'optimized'
        }
    
    @abstractmethod
    def finalize_session(self):
        """Finalizar sesión"""
        pass


class DeveloperLogger(BaseLogger):
    """Logger para modo developer - muestra todo en consola"""
    
    def user_message(self, message: str, level: str = "info"):
        """Mensaje para el usuario en modo developer"""
        icons = {"info": "🎯", "success": "✅", "error": "❌", "warning": "⚠️", 
                "processing": "📄", "question": "❓"}
        icon = icons.get(level, "🎯")
        print(f"🔧 [DEV-USER] {icon} {message}")
    
    def dev_log(self, message: str, component: str = "main", level: str = "info"):
        """Log de desarrollador visible"""
        level_icons = {"info": "ℹ️", "error": "❌", "warning": "⚠️", "debug": "🔍"}
        icon = level_icons.get(level, "ℹ️")
        
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"🔧 [DEV-LOG] {timestamp} {icon} [{component.upper()}] {message}")
        
        if level == "error":
            self.error_count += 1
            self.has_problems = True
        elif level == "warning":
            self.warning_count += 1
            self.has_problems = True
    
    def show_user_results(self, results_text: str):
        """Mostrar resultados en modo developer"""
        print(f" \n 🔧 [DEV-RESULTS] ═══ RESULTADOS DE CONSULTA ═══")
        print(f"📊 [DEV] Longitud del output: {len(results_text)} caracteres")
        print(f"🔧 [DEV-RESULTS] ═══ INICIO DEL OUTPUT ═══")
        
        for line in results_text.split(' \n '):
            if line.strip():
                print(f"📋 [RESULT] {line}")
            else:
                print()
        
        print(f"🔧 [DEV-RESULTS] ═══ FIN DEL OUTPUT ═══ \n ")
    
    def finalize_session(self):
        """Finalizar sesión developer"""
        session_duration = datetime.now() - self.session_start
        
        print(f" \n 🔧 [DEV] ═══ FINALIZANDO SESIÓN DEVELOPER ═══")
        print(f"🔧 [DEV] ⏱️  Duración: {session_duration}")
        print(f"🔧 [DEV] 📊 Operaciones: {len(self.memory_buffer['operations'])} total")
        print(f"🔧 [DEV] ❌ Errores: {self.error_count}")
        print(f"🔧 [DEV] ⚠️  Warnings: {self.warning_count}")
        print(f"🔧 [DEV] 📵 NO se guardaron archivos (modo developer)")
        print(f"🔧 [DEV] ═══ SESIÓN DEVELOPER FINALIZADA ═══ \n ")
        
        APP_CONTEXT.clear_current_session()


class OptimizedLogger(BaseLogger):
    """Logger optimizado para producción"""
    
    def __init__(self, session_name: str = "optimized_session"):
        super().__init__(session_name)
        
        # Directorios específicos
        self.session_dir_errores = CONFIG.ERRORS_DIR / self.session_id
        self.all_sessions_index = CONFIG.CONTROL_DIR / "all_sessions_index.json"
        self.error_sessions_index = CONFIG.CONTROL_DIR / "error_sessions_index.json"
        
        self._create_initial_structure()
    
    def _create_initial_structure(self):
        """Crear estructura básica"""
        try:
            CONFIG.CONTROL_DIR.mkdir(parents=True, exist_ok=True)
            CONFIG.ERRORS_DIR.mkdir(parents=True, exist_ok=True)
            self._update_all_sessions_index()
        except Exception as e:
            print(f"❌ Error creando estructura inicial: {e}")
    
    def user_message(self, message: str, level: str = "info"):
        """Mensaje limpio para el usuario"""
        if CONFIG.CONSOLE_CLEAN:
            icons = {"info": "🎯", "success": "✅", "error": "❌", "warning": "⚠️",
                    "processing": "🔄", "question": "❓"}
            icon = icons.get(level, "🎯")
            print(f"{icon} {message}")
    
    def dev_log(self, message: str, component: str = "main", level: str = "info"):
        """Log inteligente con auto-flush"""
        timestamp = datetime.now()
        
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'component': component,
            'level': level,
            'message': message,
            'session_time': (timestamp - self.session_start).total_seconds()
        }
        
        if level == "error":
            self.memory_buffer['errors'].append(log_entry)
            self.has_problems = True
            self.error_count += 1
            
            if CONFIG.AUTO_FLUSH_ON_ERROR and self.error_count >= CONFIG.ERROR_THRESHOLD:
                self._flush_errors_to_disk()
                
        elif level == "warning":
            self.memory_buffer['warnings'].append(log_entry)
            self.has_problems = True
            self.warning_count += 1
                
        elif level in ["info", "debug"] and self.has_problems:
            self.memory_buffer['context'].append(log_entry)
        
        self.memory_buffer['stats']['components_used'].add(component)
    
    def _flush_errors_to_disk(self):
        """Flush inmediato a disco"""
        if not self.has_problems:
            return
        
        try:
            self.session_dir_errores.mkdir(parents=True, exist_ok=True)
            self._write_session_content(self.session_dir_errores)
            self._update_error_sessions_index()
            self._update_all_sessions_index()
        except Exception as e:
            print(f"❌ Error en auto-flush: {e}")
    
    def _write_session_content(self, target_dir: Path):
        """Escribir contenido de sesión"""
        if self.memory_buffer['errors']:
            self._write_json(target_dir / "errors.json", self.memory_buffer['errors'])
        
        if self.memory_buffer['warnings']:
            self._write_json(target_dir / "warnings.json", self.memory_buffer['warnings'])
        
        if self.memory_buffer['context']:
            self._write_json(target_dir / "context.json", self.memory_buffer['context'])
        
        if self.memory_buffer['operations']:
            self._write_json(target_dir / "operations.json", self.memory_buffer['operations'])
        
        # Resumen de sesión
        session_summary = {
            'session_id': self.session_id,
            'session_name': self.session_name,
            'start_time': self.session_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - self.session_start).total_seconds(),
            'problem_summary': {
                'has_problems': self.has_problems,
                'error_count': self.error_count,
                'warning_count': self.warning_count,
                'failed_operations': self.memory_buffer['stats']['failed_operations'],
                'total_operations': self.memory_buffer['stats']['total_operations']
            }
        }
        
        self._write_json(target_dir / "session_summary.json", session_summary)
    
    def _write_json(self, file_path: Path, data: Any):
        """Escribir JSON con formato"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    def _update_all_sessions_index(self):
        """Actualizar índice de todas las sesiones"""
        try:
            if self.all_sessions_index.exists():
                with open(self.all_sessions_index, 'r', encoding='utf-8') as f:
                    all_index = json.load(f)
            else:
                all_index = {
                    'created_at': datetime.now().isoformat(),
                    'total_sessions': 0,
                    'sessions': []
                }
            
            session_entry = {
                'session_id': self.session_id,
                'session_name': self.session_name,
                'start_time': self.session_start.isoformat(),
                'status': 'active',
                'has_problems': self.has_problems
            }
            
            existing_session = next((s for s in all_index['sessions'] 
                                   if s['session_id'] == self.session_id), None)
            
            if existing_session:
                existing_session.update(session_entry)
            else:
                all_index['sessions'].append(session_entry)
            
            all_index['total_sessions'] = len(all_index['sessions'])
            all_index['last_updated'] = datetime.now().isoformat()
            
            with open(self.all_sessions_index, 'w', encoding='utf-8') as f:
                json.dump(all_index, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            print(f"❌ Error actualizando índice general: {e}")
    
    def _update_error_sessions_index(self):
        """Actualizar índice de sesiones con errores"""
        try:
            if self.error_sessions_index.exists():
                with open(self.error_sessions_index, 'r', encoding='utf-8') as f:
                    error_index = json.load(f)
            else:
                error_index = {
                    'created_at': datetime.now().isoformat(),
                    'total_error_sessions': 0,
                    'error_sessions': []
                }
            
            error_session_entry = {
                'session_id': self.session_id,
                'session_name': self.session_name,
                'start_time': self.session_start.isoformat(),
                'error_count': self.error_count,
                'warning_count': self.warning_count,
                'failed_operations': self.memory_buffer['stats']['failed_operations']
            }
            
            existing_error = next((s for s in error_index['error_sessions'] 
                                 if s['session_id'] == self.session_id), None)
            
            if existing_error:
                existing_error.update(error_session_entry)
            else:
                error_index['error_sessions'].append(error_session_entry)
            
            error_index['total_error_sessions'] = len(error_index['error_sessions'])
            error_index['last_updated'] = datetime.now().isoformat()
            
            with open(self.error_sessions_index, 'w', encoding='utf-8') as f:
                json.dump(error_index, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            print(f"❌ Error actualizando índice de errores: {e}")
    
    def finalize_session(self):
        """Finalizar sesión optimizada"""
        session_duration = datetime.now() - self.session_start
        
        try:
            if self.has_problems:
                if not any(self.session_dir_errores.glob("*.json")):
                    self._flush_errors_to_disk()
                
                suppressed_count = len(self.get_suppressed_content())
                self.user_message(f"⚠️ Sesión completada con {self.error_count} errores y {self.warning_count} warnings", "warning")
                self.user_message(f"📂 Errores guardados en: {self.session_dir_errores}", "info")
                if suppressed_count > 0:
                    self.user_message(f"📄 Contenido suprimido: {suppressed_count} operaciones", "info")
            else:
                self.user_message("✅ Sesión completada exitosamente", "success")
                self._mark_session_completed()
            
            total_ops = self.memory_buffer['stats']['total_operations']
            failed_ops = self.memory_buffer['stats']['failed_operations']
            success_rate = ((total_ops - failed_ops) / max(total_ops, 1)) * 100
            
            print(f"📊 Operaciones: {total_ops} total, {failed_ops} fallidas ({success_rate:.1f}% éxito)")
            print(f"⏱️ Duración: {session_duration}")
            
            APP_CONTEXT.clear_current_session()
            
        except Exception as e:
            print(f"⚠️ Error finalizando sesión: {e}")
        finally:
            self._cleanup_memory_buffer()
    
    def _mark_session_completed(self):
        """Marcar sesión como completada"""
        try:
            if self.all_sessions_index.exists():
                with open(self.all_sessions_index, 'r', encoding='utf-8') as f:
                    all_index = json.load(f)
                
                for session in all_index['sessions']:
                    if session['session_id'] == self.session_id:
                        session['status'] = 'completed'
                        session['has_problems'] = self.has_problems
                        session['end_time'] = datetime.now().isoformat()
                        break
                
                with open(self.all_sessions_index, 'w', encoding='utf-8') as f:
                    json.dump(all_index, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            print(f"❌ Error marcando sesión como completada: {e}")
    
    def _cleanup_memory_buffer(self):
        """Limpiar buffer de memoria"""
        self.memory_buffer = {
            'errors': [], 'warnings': [], 'context': [], 'operations': [],
            'stats': {'total_operations': 0, 'failed_operations': 0, 'components_used': set()}
        }
        self.has_problems = False
        self.error_count = 0
        self.warning_count = 0

# =============================================================================
# 6. FACTORY DE LOGGERS
# =============================================================================

def create_logger(session_name: str = "master_session") -> BaseLogger:
    """Factory para crear el logger apropiado según el modo"""
    if CONFIG.DEVELOPER_MODE:
        return DeveloperLogger(session_name)
    else:
        return OptimizedLogger(session_name)


def get_master_logger() -> BaseLogger:
    """Obtener logger maestro global"""
    logger = APP_CONTEXT.get_current_session_logger()
    if logger is None:
        logger = create_logger()
    return logger





# =============================================================================
# 7. ADAPTADORES SQL
# =============================================================================

class SQLiteToMySQLAdapter:
    """Adaptador para convertir consultas SQLite a MySQL"""
    
    def __init__(self, mysql_table_name: str, logger: BaseLogger):
        self.mysql_table_name = mysql_table_name
        self.logger = logger
        self.conversion_stats = {
            'total_conversions': 0,
            'quotes_replaced': 0,
            'table_replacements': 0,
            'rowid_removals': 0,
            'queries_processed': []
        }
    
    def convert_sqlite_to_mysql(self, sqlite_query: str) -> Dict[str, Any]:
        """Convertir consulta SQLite a MySQL"""
        try:
            self.logger.dev_log(f"🔄 INICIANDO CONVERSIÓN SQL:", "sql")
            self.logger.dev_log(f"   📥 SQL SQLite: {sqlite_query}", "sql")
            
            conversion_info = {
                'original_query': sqlite_query,
                'steps_applied': [],
                'success': True,
                'error': None
            }
            
            # Paso 1: Convertir comillas dobles a backticks
            mysql_query, quotes_replaced = self._convert_double_quotes_to_backticks(sqlite_query)
            conversion_info['quotes_found'] = quotes_replaced
            conversion_info['steps_applied'].append(f"quotes_conversion_{quotes_replaced}_replaced")
            
            # Paso 2: Reemplazar tabla "datos" por tabla real
            mysql_query, table_replaced = self._replace_datos_table(mysql_query)
            conversion_info['table_found'] = table_replaced
            conversion_info['steps_applied'].append(f"table_replacement_{table_replaced}")
            
            # Paso 3: Eliminar ORDER BY ROWID
            mysql_query, rowid_removed = self._remove_rowid_order(mysql_query)
            conversion_info['rowid_removed'] = rowid_removed
            conversion_info['steps_applied'].append(f"rowid_removal_{rowid_removed}")
            
            # Actualizar estadísticas
            self.conversion_stats['total_conversions'] += 1
            self.conversion_stats['quotes_replaced'] += quotes_replaced
            if table_replaced:
                self.conversion_stats['table_replacements'] += 1
            if rowid_removed:
                self.conversion_stats['rowid_removals'] += 1
            
            conversion_info['converted_query'] = mysql_query
            self.conversion_stats['queries_processed'].append(conversion_info)
            
            self.logger.dev_log(f"✅ CONVERSIÓN COMPLETADA:", "sql")
            self.logger.dev_log(f"   📤 SQL MySQL: {mysql_query}", "sql")
            
            return {
                'success': True,
                'original_query': sqlite_query,
                'converted_query': mysql_query,
                'changes_made': {
                    'quotes_replaced': quotes_replaced,
                    'table_replaced': table_replaced,
                    'rowid_removed': rowid_removed,
                    'total_changes': quotes_replaced + (1 if table_replaced else 0) + (1 if rowid_removed else 0)
                },
                'conversion_info': conversion_info
            }
            
        except Exception as e:
            error_msg = f"Error en conversión SQL: {str(e)}"
            self.logger.dev_log(f"❌ {error_msg}", "sql", "error")
            
            return {
                'success': False,
                'original_query': sqlite_query,
                'converted_query': sqlite_query,
                'error': error_msg
            }
    
    def _convert_double_quotes_to_backticks(self, query: str) -> Tuple[str, int]:
        """Convertir comillas dobles a backticks"""
        pattern = r'"([^"]+)"'
        replaced_count = 0
        
        def replace_quotes(match):
            nonlocal replaced_count
            column_name = match.group(1)
            replaced_count += 1
            return f'`{column_name}`'
        
        converted_query = re.sub(pattern, replace_quotes, query)
        return converted_query, replaced_count
    
    def _replace_datos_table(self, query: str) -> Tuple[str, bool]:
        """Reemplazar tabla 'datos' por tabla MySQL real"""
        patterns = [
            r'\bFROM\s+datos\b',
            r'\bJOIN\s+datos\b',
            r'\bINTO\s+datos\b',
            r'\bUPDATE\s+datos\b',
            r'\bdatos\.'
        ]
        
        table_replaced = False
        converted_query = query
        
        for pattern in patterns:
            if re.search(pattern, converted_query, re.IGNORECASE):
                converted_query = re.sub(
                    pattern,
                    lambda m: m.group(0).replace('datos', f'`{self.mysql_table_name}`'),
                    converted_query,
                    flags=re.IGNORECASE
                )
                table_replaced = True
        
        return converted_query, table_replaced
    
    def _remove_rowid_order(self, query: str) -> Tuple[str, bool]:
        """Eliminar ORDER BY ROWID (específico de SQLite)"""
        pattern = r'\s+ORDER\s+BY\s+ROWID\s+(ASC|DESC)?\s*'
        
        if re.search(pattern, query, re.IGNORECASE):
            clean_query = re.sub(pattern, ' ', query, flags=re.IGNORECASE)
            return clean_query.strip(), True
        
        return query, False
    
    def get_conversion_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de conversión"""
        return {
            'total_conversions': self.conversion_stats['total_conversions'],
            'total_quotes_replaced': self.conversion_stats['quotes_replaced'],
            'total_table_replacements': self.conversion_stats['table_replacements'],
            'total_rowid_removals': self.conversion_stats['rowid_removals'],
            'mysql_table_name': self.mysql_table_name
        }

        
# =============================================================================
# 9. GESTOR DE SESIONES DE USUARIOS
# =============================================================================

class UserSessionManager:
    """Gestor de sesiones por usuario"""
    
    def __init__(self):
        self._user_sessions: Dict[str, BaseLogger] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._master_lock = threading.Lock()
        
        self.max_sessions_per_user = CONFIG.MAX_SESSIONS_PER_USER
        self.default_user_id = CONFIG.DEFAULT_USER_ID
    
    def get_or_create_session(self, user_id: Optional[str] = None) -> BaseLogger:
        """Obtener o crear sesión para usuario"""
        if user_id is None:
            user_id = self.default_user_id
        
        with self._master_lock:
            if user_id not in self._locks:
                self._locks[user_id] = threading.Lock()
        
        with self._locks[user_id]:
            if user_id in self._user_sessions:
                existing_session = self._user_sessions[user_id]
                if hasattr(existing_session, 'session_id'):
                    return existing_session
            
            session_name = f"usuario_{user_id}_{datetime.now().strftime('%H%M%S')}"
            new_logger = create_logger(session_name)
            
            new_logger.dev_log(f"🎯 Sesión iniciada para usuario: {user_id}", "session_manager", "info")
            
            self._user_sessions[user_id] = new_logger
            return new_logger
    
    def close_user_session(self, user_id: Optional[str] = None):
        """Cerrar sesión de usuario"""
        if user_id is None:
            user_id = self.default_user_id
        
        if user_id in self._locks:
            with self._locks[user_id]:
                if user_id in self._user_sessions:
                    session = self._user_sessions[user_id]
                    session.finalize_session()
                    del self._user_sessions[user_id]
    
    def get_active_sessions(self) -> Dict[str, str]:
        """Obtener sesiones activas"""
        active_sessions = {}
        with self._master_lock:
            for user_id, session in self._user_sessions.items():
                if hasattr(session, 'session_id'):
                    active_sessions[user_id] = session.session_id
        return active_sessions
    
    def close_all_sessions(self):
        """Cerrar todas las sesiones"""
        users_to_close = list(self._user_sessions.keys())
        for user_id in users_to_close:
            self.close_user_session(user_id)


# =============================================================================
# 10. EJECUTOR MAESTRO PRINCIPAL
# =============================================================================


class MasterQueryExecutor:
    """Ejecutor maestro integrado - Modo dual (Traditional/MySQL) con Pool de Conexiones"""
    
    def __init__(self, user_id: Optional[str] = None):
        # Obtener logger del usuario
        session_manager = UserSessionManager()
        self.logger = session_manager.get_or_create_session(user_id)
        
        # ===== CAMBIO 1: Guardar user_id =====
        self.user_id = user_id or 'default'
        
        self.logger.dev_log("🚀 INICIALIZANDO EJECUTOR MAESTRO DUAL MODE", "main")
        
        # Componentes principales
        self.table_analyzer = None
        self.problemizador = None
        self.sql_mapper = None
        
        # Estado del sistema tradicional
        self.table_loaded = False
        self.current_dataframe = None
        self.analysis_result = {}
        self.table_info = {}
        
        # Configuración dual mode
        self.mode = ExecutionMode.TRADITIONAL
        self.clickhouse_manager = None
        self.clickhouse_config = None
        self.selected_table = None
        self.sql_adapter = None
                
        # Historial de consultas
        self.query_history = []
        self.session_stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'session_start': datetime.now()
        }
        
        # Inicializar componentes
        self._initialize_components()
        
        print("🎯 Ejecutor maestro dual listo")
        self._show_system_status()
    
    
    def _initialize_components(self):
        """Inicializar componentes con logging limpio"""
        op = self.logger.start_operation("Configurando componentes", "main")
        
        try:
            components = APP_CONTEXT.get_components()
            
            if not components:
                raise Exception("No se pudieron cargar componentes críticos")
            
            # TableAnalyzer
            self.logger.update_operation("Inicializando TableAnalyzer...")
            with suppress_component_output("tableanalyzer_init"):
                self.table_analyzer = components['TableAnalyzer']()
            self.logger.dev_log("✅ TableAnalyzer: Listo", "main")
            
            # Problemizador
            self.logger.update_operation("Inicializando Problemizador...")
            with suppress_component_output("problemizador_init"):
                self.problemizador = components['UnifiedNLPParser']()
            self.logger.dev_log("✅ Problemizador: Listo", "main")
            
            # SQLSchemaMapper
            self.logger.update_operation("Inicializando SQLSchemaMapper...")
            with suppress_component_output("sql_mapper_init"):
                self.sql_mapper = components['SQLSchemaMapper']()
            self.logger.dev_log("✅ SQLSchemaMapper: Listo", "main")
            
            self.logger.complete_operation(True)
            
        except Exception as e:
            self.logger.log_exception(e, "main", "Durante inicialización de componentes")
            self.logger.complete_operation(False, str(e))
            raise
    
    
    def _show_system_status(self):
        """Mostrar estado del sistema maestro"""
        print(f"\n🎉 SISTEMA MAESTRO DUAL OPERATIVO")
        print("=" * 50)
        print("📊 TableAnalyzer: Carga y análisis completo")
        print("🧠 Problemizador: Procesamiento de consultas naturales")
        print("🔧 SQLSchemaMapper: Normalización de SQL con anchors")
        print("⚡ Ejecución: Consultas SQL sobre datos (SQLite/clickhouse)")
        print("🎯 Modo: DUAL - Tradicional o clickhouse")
    
    
    def set_clickhouse_mode(self, clickhouse_config: Dict[str, Any], selected_table: str):
        """Configurar modo ClickHouse"""
        self.mode = ExecutionMode.CLICKHOUSE
        self.clickhouse_config = clickhouse_config
        self.selected_table = selected_table
        
        
        self.clickhouse_manager = UnifiedClickHouseManager(
            clickhouse_config, selected_table, self.logger
        )
        
        if self.clickhouse_manager.is_connected:
            self.logger.dev_log(f"✅ MODO CLICKHOUSE CONFIGURADO:", "main")
            self.logger.dev_log(f"   🗄️ Base: {clickhouse_config.get('database', 'datos_imperiales')}", "main")
            self.logger.dev_log(f"   📋 Tabla: {selected_table}", "main")
            self.logger.dev_log(f"   ⚡ Motor columnar ultrarrápido activo", "main")
            self._show_clickhouse_status()
        else:
            self.logger.dev_log(f"❌ ERROR: No se pudo conectar a ClickHouse", "main", "error")
            self.mode = ExecutionMode.TRADITIONAL
        
        
    def _show_clickhouse_status(self):
        """Mostrar estado ClickHouse"""
        if self.clickhouse_manager:
            table_info = self.clickhouse_manager.get_table_info()
            info = table_info['table_info']
            
            print(f"\n🚀 CLICKHOUSE CONECTADO (MOTOR COLUMNAR)")
            print("=" * 50)
            print(f"📋 Tabla: {info['name']}")
            print(f"📊 Datos: {info['rows_count']:,} filas × {info['columns_count']} columnas")
            print(f"💾 Tamaño: {info.get('data_size_mb', 0):.2f}MB")
            print(f"📦 Compresión: {info.get('compression_ratio', 1):.1f}x")
            print("\n⚡ Características ClickHouse:")
            print("   • Consultas 100-1000x más rápidas")
            print("   • Compresión columnar eficiente")
            print("   • Procesamiento paralelo masivo")
            print("🎯 ¡Listo para consultas ultrarrápidas!")
        
        
    def load_and_analyze_table(self, file_path: str) -> Dict[str, Any]:
        """Carga y análisis de tabla (solo modo tradicional)"""
        if self.mode != ExecutionMode.TRADITIONAL:
            error_msg = f'Carga de archivos no permitida en modo {self.mode.value}'
            self.logger.dev_log(f"🛑 {error_msg}", "table", "warning")
            return {
                'success': False,
                'error': error_msg,
                'suggestion': f'Modo {self.mode.value} trabaja directamente con datos existentes'
            }
        
        file_name = Path(file_path).name
        op = self.logger.start_operation(f"Cargando {file_name}", "table")
        
        try:
            self.logger.dev_log(f"📊 PASO 1 MAESTRO: CARGA Y ANÁLISIS CON TABLEANALYZER", "table")
            self.logger.update_operation("Ejecutando análisis TableAnalyzer...")
            
            analysis_result = self.table_analyzer.analyze_table(file_path)
            
            if not analysis_result.get('success', False):
                error_msg = f"TableAnalyzer falló: {analysis_result.get('error', 'Error desconocido')}"
                self.logger.dev_log(f"❌ {error_msg}", "table", "error")
                self.logger.complete_operation(False, "Error analizando tabla")
                return {
                    'success': False,
                    'error': error_msg,
                    'analysis_result': analysis_result
                }
            
            # Obtener DataFrame desde TableAnalyzer
            self.logger.update_operation("Procesando DataFrame...")
            dataframe = self.table_analyzer.current_table
            
            if dataframe is None or dataframe.empty:
                error_msg = 'TableAnalyzer no cargó datos válidos'
                self.logger.dev_log(f"❌ {error_msg}", "table", "error")
                self.logger.complete_operation(False, "Datos no válidos")
                return {'success': False, 'error': error_msg}
            
            # Guardar estado
            self.table_loaded = True
            self.current_dataframe = dataframe
            self.analysis_result = analysis_result
            
            self.table_info = {
                'file_path': file_path,
                'file_name': Path(file_path).stem,
                'rows': len(dataframe),
                'columns': len(dataframe.columns),
                'column_names': list(dataframe.columns),
                'loaded_at': datetime.now().isoformat()
            }
            
            self.logger.dev_log(f"✅ TABLA CARGADA Y ANALIZADA:", "table")
            self.logger.dev_log(f"   📊 Datos: {self.table_info['rows']:,} filas × {self.table_info['columns']} columnas", "table")
            
            self.logger.complete_operation(True)
            
            self.logger.user_message(f"Tabla cargada: {self.table_info['rows']:,} filas × {self.table_info['columns']} columnas", "info")
            self.logger.user_message("🧠 ¡Listo para consultas! Escribe tu pregunta.", "info")
            
            return {
                'success': True,
                'table_info': self.table_info,
                'analysis_result': analysis_result,
                'dataframe': dataframe,
                'message': f"Tabla '{self.table_info['file_name']}' cargada exitosamente"
            }
            
        except Exception as e:
            error_msg = f"Error en carga maestro: {str(e)}"
            self.logger.log_exception(e, component="table", context="Durante carga y análisis de tabla")
            self.logger.complete_operation(False, "Error cargando tabla")
            
            return {
                'success': False,
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
    
    
    def process_natural_query(self, user_input: str) -> Dict[str, Any]:
        """Procesamiento de consulta natural (directo por modo)"""
        self.logger.user_message(f'Consulta: "{user_input}"', "question")
        
        # Validación por modo
        if self.mode == ExecutionMode.TRADITIONAL:
            if not self.table_loaded:
                self.logger.user_message("No hay tabla cargada", "warning")
                return {
                    'success': False,
                    'error': 'No hay tabla cargada',
                    'suggestion': 'Primero carga una tabla'
                }
        elif self.mode == ExecutionMode.CLICKHOUSE:
            if not self.clickhouse_manager or not self.clickhouse_manager.is_connected:
                self.logger.user_message("No hay conexión ClickHouse activa", "warning")
                return {
                    'success': False,
                    'error': 'No hay conexión ClickHouse activa',
                    'suggestion': 'Verifica la conexión a la base de datos'
                }
        
        op = self.logger.start_operation("Procesando consulta", "nlp")
        
        try:
            self.logger.dev_log(f"🧠 PASO 2 MAESTRO: PROCESAMIENTO CON PROBLEMIZADOR", "nlp")
            self.logger.dev_log(f"❓ Consulta: '{user_input}'", "nlp")
            self.logger.dev_log(f"🎯 Modo actual: {self.mode.value}", "nlp")
            
            self.logger.update_operation("Analizando lenguaje natural...")
            
            with suppress_component_output("problemizador_process"):
                result = self.problemizador.process_user_input(user_input)
            
            if not result.get('success', False):
                error_msg = "No se pudo procesar la consulta"
                self.logger.dev_log(f"❌ Problemizador falló: {result.get('error', 'Error desconocido')}", "nlp", "error")
                self.logger.complete_operation(False, error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'problemizador_result': result,
                    'suggestions': result.get('suggestions', []),
                    'user_input': user_input
                }
            
            sql_query = result.get('sql_query')
            
            if not sql_query:
                error_msg = 'No se pudo generar consulta SQL válida'
                self.logger.dev_log(f"❌ {error_msg}", "nlp", "error")
                self.logger.complete_operation(False, error_msg)
                return {'success': False, 'error': error_msg}
            
            self.logger.dev_log(f"✅ Problemizador completado:", "nlp")
            self.logger.dev_log(f"   🗄️ SQL conceptual generado: {sql_query}", "nlp")
            
            self.logger.complete_operation(True)
            
            return {
                'success': True,
                'sql_query': sql_query,
                'problemizador_result': result,
                'interpretation': result.get('interpretation', ''),
                'confidence': result.get('confidence', 0.0),
                'complexity': result.get('complexity_level', 'unknown'),
                'original_query': user_input
            }
            
        except Exception as e:
            error_msg = f"Error en Problemizador: {str(e)}"
            self.logger.dev_log(f"❌ {error_msg}", "nlp", "error")
            self.logger.complete_operation(False, "Error procesando consulta")
            return {
                'success': False,
                'error': error_msg,
                'traceback': traceback.format_exc()
            }
    
    
    def execute_sql_on_data(self, sql_query: str) -> Dict[str, Any]:
        """Ejecutar SQL en datos (modo dual)"""
        self.logger.dev_log(f"🗄️ EJECUTANDO SQL - MODO: {self.mode.value}", "exec")
        
        if self.mode == ExecutionMode.TRADITIONAL:
            self.logger.dev_log(f"   📊 Destino: RAM (SQLite temporal)", "exec")
            return self._execute_sql_on_dataframe(sql_query)
        elif self.mode == ExecutionMode.CLICKHOUSE:
            self.logger.dev_log(f"   🚀 Destino: ClickHouse (Motor Columnar)", "exec")
            return self._execute_sql_on_clickhouse(sql_query)
        else:
            return {'success': False, 'error': f'Modo desconocido: {self.mode.value}'}
    
    
    def _execute_sql_on_dataframe(self, sql_query: str) -> Dict[str, Any]:
        """Ejecución SQL tradicional (SQLite temporal)"""
        if not self.table_loaded or self.current_dataframe is None:
            self.logger.user_message("No hay tabla cargada para ejecutar consulta", "error")
            return {'success': False, 'error': 'No hay tabla cargada para ejecutar consulta'}
        
        try:
            self.logger.dev_log(f"🗄️ PASO 3 MAESTRO: EJECUTANDO SQL SOBRE DATAFRAME (TRADICIONAL)", "exec")
            
            # Aplicar fix de comillas
            column_names = list(self.current_dataframe.columns)
            fixed_sql = fix_sql_column_quotes(sql_query, column_names)
            
            if fixed_sql != sql_query:
                self.logger.dev_log(f"🔧 SQL corregido: {fixed_sql}", "exec")
            
            # Crear SQLite temporal en memoria
            conn = sqlite3.connect(":memory:")
            
            # Cargar DataFrame en SQLite
            table_name = "datos"
            self.current_dataframe.to_sql(table_name, conn, index=False, if_exists='replace')
            
            self.logger.dev_log(f"💾 DataFrame cargado en SQLite temporal como '{table_name}'", "exec")
            
            # Ejecutar consulta
            cursor = conn.cursor()
            cursor.execute(fixed_sql)
            
            # Obtener resultados
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            # Convertir a lista de diccionarios
            results = []
            for row in rows:
                result_dict = {}
                for i, column in enumerate(columns):
                    result_dict[column] = row[i]
                results.append(result_dict)
            
            conn.close()
            
            self.logger.dev_log(f"✅ Consulta ejecutada exitosamente:", "exec")
            self.logger.dev_log(f"   📊 Filas resultado: {len(results)}", "exec")
            
            return {
                'success': True,
                'results': results,
                'columns': columns,
                'row_count': len(results),
                'sql_executed': fixed_sql,
                'sql_original': sql_query,
                'sql_was_fixed': fixed_sql != sql_query,
                'execution_mode': 'traditional'
            }
            
        except Exception as e:
            error_msg = f"Error ejecutando SQL: {str(e)}"
            self.logger.dev_log(f"❌ {error_msg}", "exec", "error")
            return {
                'success': False,
                'error': error_msg,
                'sql_attempted': fixed_sql if 'fixed_sql' in locals() else sql_query,
                'sql_original': sql_query,
                'execution_mode': 'traditional'
            }
    
    def _execute_sql_on_clickhouse(self, sql_query: str) -> Dict[str, Any]:
        """
        Ejecución SQL directa en ClickHouse, convirtiendo el resultado 
        del cliente de ClickHouse (tuplas) a un formato de lista de diccionarios.
        """
        # 1. Validación de conexión
        if not self.clickhouse_manager or not self.clickhouse_manager.is_connected:
            return {'success': False, 'error': 'No hay conexión ClickHouse activa'}
        
        query_start = datetime.now() # Iniciar el contador de tiempo
        user_id = self.user_id if hasattr(self, 'user_id') else 'default'
        
        self.logger.dev_log(f"🚀 PASO 3: EJECUCIÓN EN CLICKHOUSE", "exec")
        
        # Adaptar la consulta (usando tu método existente)
        # Nota: Este método debería residir en UnifiedClickHouseManager
        final_query = self.clickhouse_manager._adapt_sql_for_clickhouse(sql_query)
        
        try:
            # 2. Ejecutar consulta
            result = self.clickhouse_manager.client.query(final_query)
            
            # 3. Procesar resultados
            columns = list(result.column_names) if hasattr(result, 'column_names') else []
            results = []

            # CONVERSIÓN CRÍTICA: Tuplas de filas a lista de diccionarios
            for row_tuple in result.result_rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    # Usamos el valor directamente, ClickHouse Connect maneja la mayoría de los tipos
                    row_dict[col] = row_tuple[i] 
                results.append(row_dict)
            
            # 4. Calcular tiempo
            query_end = datetime.now()
            execution_time_ms = int((query_end - query_start).total_seconds() * 1000)
            
            # 5. Registrar éxito en el log (asumiendo que _log_query existe)
            if hasattr(self, '_log_query'):
                self._log_query(
                    self.selected_table, final_query,
                    execution_time_ms, len(results), True, '', user_id
                )
            
            self.logger.dev_log(f"✅ Query ejecutada: {len(results)} filas en {execution_time_ms}ms", "exec")
            
            return {
                'success': True,
                'results': results, # <--- Lista de diccionarios, crítica para el frontend
                'columns': columns,
                'row_count': len(results),
                'execution_time_ms': execution_time_ms,
                'sql_executed': final_query,
                'sql_original': sql_query,
                'execution_mode': 'clickhouse'
            }
            
        except Exception as e:
            # 6. Manejar y registrar error
            error_msg = str(e)
            
            query_end = datetime.now()
            execution_time_ms = int((query_end - query_start).total_seconds() * 1000)
            
            if hasattr(self, '_log_query'):
                self._log_query(
                    self.selected_table, final_query,
                    execution_time_ms, 0, False, error_msg[:500], user_id
                )
            
            self.logger.log_exception(e, "clickhouse", "Error ejecutando query ClickHouse")
            
            return {
                'success': False,
                'error': error_msg,
                'sql_attempted': final_query,
                'execution_mode': 'clickhouse'
            }
    
    
    def execute_complete_master_flow(self, user_input: str) -> Dict[str, Any]:
        """Flujo maestro completo con experiencia limpia (modo dual)"""
        self.logger.dev_log(f"🚀 INICIANDO FLUJO MAESTRO COMPLETO - MODO: {self.mode.value}", "main")
        
        self.session_stats['total_queries'] += 1
        query_start_time = datetime.now()
        
        # Paso 2: Procesar consulta natural
        query_result = self.process_natural_query(user_input)
        
        if not query_result.get('success', False):
            self.session_stats['failed_queries'] += 1
            
            error_msg = query_result.get('error', 'Error procesando consulta')
            self.logger.user_message(f"❌ {error_msg}", "error")
            
            suggestions = query_result.get('suggestions', [])
            if suggestions:
                self.logger.user_message("💡 Sugerencias:", "info")
                for suggestion in suggestions:
                    print(f"   • {suggestion}")
            
            return {
                'success': False,
                'step_failed': 'natural_query_processing',
                'error': error_msg,
                'suggestions': suggestions,
                'query_result': query_result
            }
        
        # Paso 3: Ejecutar SQL sobre datos
        sql_query = query_result['sql_query']
        execution_result = self.execute_sql_on_data(sql_query)
        
        if not execution_result.get('success', False):
            self.session_stats['failed_queries'] += 1
            self.logger.user_message("❌ Error ejecutando consulta", "error")
            self.logger.dev_log(f"❌ Error en ejecución SQL: {execution_result.get('error')}", "exec", "error")
            return {
                'success': False,
                'step_failed': 'sql_execution',
                'error': execution_result.get('error'),
                'sql_attempted': sql_query,
                'query_result': query_result,
                'execution_result': execution_result
            }
        
        # Resultados exitosos
        self.session_stats['successful_queries'] += 1
        query_duration = datetime.now() - query_start_time
        duration_ms = int(query_duration.total_seconds() * 1000)
        
        # Formatear y mostrar resultados al usuario
        formatted_output = self._format_clean_user_results(execution_result, query_result)
        self.logger.show_user_results(formatted_output)
        
        # Métricas básicas para el usuario
        confidence = query_result.get('confidence', 0.0)
        row_count = execution_result.get('row_count', 0)
        execution_mode = execution_result.get('execution_mode', 'unknown')
        
        # ===== CAMBIO 6: Actualizar iconos para reflejar pool =====
        mode_icon = "⚡" if execution_mode == 'mysql_pool' else "🗄️" if execution_mode == 'mysql' else "💾"
        mode_text = "MySQL Pool" if execution_mode == 'mysql_pool' else execution_mode.title()
        
        # Agregar info de concurrencia si está disponible
        concurrency_info = ""
        if 'concurrency_stats' in execution_result:
            stats = execution_result['concurrency_stats']
            concurrency_info = f" | ⏱️ Espera: {stats['wait_time_ms']}ms"
        
        self.logger.user_message(
            f"⏱️ Procesado en {duration_ms}ms | 🎯 Confianza: {confidence:.2f} | {mode_icon} {mode_text}{concurrency_info}", 
            "info"
        )
        
        # Guardar en historial
        history_entry = {
            'timestamp': query_start_time.strftime('%H:%M:%S'),
            'user_input': user_input,
            'interpretation': query_result.get('interpretation', ''),
            'sql_query': sql_query,
            'row_count': row_count,
            'confidence': confidence,
            'duration_ms': duration_ms,
            'execution_mode': execution_mode,
            'success': True
        }
        
        # Agregar stats de concurrencia si existen
        if 'concurrency_stats' in execution_result:
            history_entry['concurrency_stats'] = execution_result['concurrency_stats']
        
        self.query_history.append(history_entry)
        
        return {
            'success': True,
            'formatted_output': formatted_output,
            'query_result': query_result,
            'execution_result': execution_result,
            'duration_ms': duration_ms,
            'history_entry': history_entry,
            'flow_type': f'master_complete_{execution_mode}'
        }
    
        
    def register_user_feedback(self, query: str, response: str, satisfied: bool, comment: str = "") -> Dict[str, Any]:
        """Registrar retroalimentación del usuario sobre la última respuesta"""
        try:
            self.logger.user_message(
                f"{'👍 Gracias por tu feedback positivo' if satisfied else '👎 Lamentamos que no estés satisfecho'}", 
                "info" if satisfied else "warning"
            )
            
            # Registrar en el logger
            self.logger.log_user_feedback(query, response, satisfied, comment)
            
            # Actualizar estadísticas de sesión
            if not hasattr(self.session_stats, 'feedback_count'):
                self.session_stats['feedback_count'] = 0
                self.session_stats['negative_feedback_count'] = 0
                self.session_stats['positive_feedback_count'] = 0
            
            self.session_stats['feedback_count'] += 1
            if satisfied:
                self.session_stats['positive_feedback_count'] += 1
            else:
                self.session_stats['negative_feedback_count'] += 1
            
            # Agregar al historial
            if self.query_history:
                last_query = self.query_history[-1]
                last_query['user_satisfied'] = satisfied
                last_query['user_comment'] = comment
                last_query['feedback_timestamp'] = datetime.now().isoformat()
            
            return {
                'success': True,
                'message': 'Retroalimentación registrada exitosamente',
                'satisfied': satisfied,
                'session_will_be_saved': not satisfied
            }
            
        except Exception as e:
            self.logger.log_exception(e, "feedback", "Durante registro de retroalimentación")
            return {
                'success': False,
                'error': f'Error registrando feedback: {str(e)}'
            }


    def get_feedback_summary(self) -> Dict[str, Any]:
        """Obtener resumen de retroalimentación de la sesión"""
        total_feedback = self.session_stats.get('feedback_count', 0)
        positive = self.session_stats.get('positive_feedback_count', 0)
        negative = self.session_stats.get('negative_feedback_count', 0)
        
        satisfaction_rate = 0
        if total_feedback > 0:
            satisfaction_rate = (positive / total_feedback) * 100
        
        # Recopilar comentarios negativos
        negative_comments = []
        for query in self.query_history:
            if query.get('user_satisfied') is False and query.get('user_comment'):
                negative_comments.append({
                    'query': query.get('user_input', ''),
                    'comment': query.get('user_comment', ''),
                    'timestamp': query.get('feedback_timestamp', '')
                })
        
        return {
            'total_feedback_received': total_feedback,
            'positive_feedback': positive,
            'negative_feedback': negative,
            'satisfaction_rate': f"{satisfaction_rate:.1f}%",
            'negative_comments': negative_comments,
            'has_negative_feedback': negative > 0
        }
    

    
    def _clean_column_name(self, column_name: str) -> str:
        """Lógica para limpiar y formatear nombres de columna para la salida."""
        import re
        
        # Patrones para detectar operaciones SQL comunes
        patterns = [
            r'^SUM\((.+)\)$',
            r'^COUNT\((.+)\)$',
            r'^AVG\((.+)\)$',
            r'^MAX\((.+)\)$',
            r'^MIN\((.+)\)$',
            r'^TOTAL\((.+)\)$',
            r'^(toUInt.*|toInt.*|toFloat.*)\((.+)\)$' # Captura conversiones de tipo
        ]
        
        for pattern in patterns:
            match = re.match(pattern, column_name, re.IGNORECASE)
            if match:
                # Si hay dos grupos, toma el segundo (la columna interna)
                if len(match.groups()) > 1:
                    inner_column = match.group(2) 
                else:
                    inner_column = match.group(1) 
                    
                # Elimina backticks si los hubiera para la presentación
                inner_column = inner_column.strip('`').strip('"')
                
                # Devuelve el nombre de la operación o la columna
                operation = re.match(r'^([A-Z]+)', column_name, re.IGNORECASE)
                op_name = operation.group(1) if operation else ''
                
                return f"{op_name.upper()}({inner_column})"
                
        # Si no es una operación, devuelve el nombre original.
        return column_name.strip('`').strip('"') 
                
    
    def _format_clean_user_results(self, execution_result: Dict[str, Any], query_result: Dict[str, Any]) -> str:
        """
        Formatear resultados limpios para el usuario como tabla de texto ASCII.
        Se asume que execution_result['results'] es una lista de diccionarios.
        """
        if not execution_result.get('success', False):
            return f"❌ No se pudieron obtener resultados"
        
        results = execution_result.get('results', [])
        columns = execution_result.get('columns', [])
        
        if not results:
            return "🔭 No se encontraron resultados para tu consulta"
        
        output = []
        
        # Lógica para un solo resultado (formato clave: valor)
        if len(results) == 1 and len(columns) > 0 and len(results[0]) <= 2: 
            # Esto maneja agregaciones simples (ej. SELECT COUNT(*))
            result = results[0]
            for column in columns:
                value = result.get(column, 'N/A')
                
                # Conversión de valores grandes o flotantes
                if isinstance(value, (int, float)):
                    formatted_value = f"{value:,.2f}" if isinstance(value, float) and value % 1 != 0 else f"{value:,}"
                elif isinstance(value, datetime): # Manejo de fechas de ClickHouse
                    formatted_value = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_value = str(value)
                
                clean_column = self._clean_column_name(column)
                output.append(f"{clean_column}: {formatted_value}")
            return "\n".join(output)

        
        # Lógica para múltiples resultados (formato tabla)
        
        # 1. Calcular ancho dinámico de columnas
        col_widths = {}
        for col in columns:
            header_name = self._clean_column_name(col)
            max_width = len(header_name)
            
            for result in results:
                value = result.get(col, 'N/A')
                
                if isinstance(value, (int, float)):
                    formatted_value = f"{value:,.0f}" if isinstance(value, float) and value % 1 != 0 else f"{value:,}"
                elif isinstance(value, datetime):
                    formatted_value = value.strftime('%Y-%m-%d')
                else:
                    formatted_value = str(value)
                
                max_width = max(max_width, len(formatted_value))
                
            # Limitar ancho máximo para evitar tablas gigantes
            col_widths[col] = min(max_width, 50) 
        
        # 2. Encabezados (Header)
        header_parts = []
        separator_parts = []
        for col in columns:
            width = col_widths[col]
            clean_name = self._clean_column_name(col)
            
            header_parts.append(f"{clean_name:^{width}}")
            separator_parts.append("-" * width)
            
        header = " | ".join(header_parts)
        separator = "-+-".join(separator_parts) 
        
        output.append(header)
        output.append(separator)
        
        # 3. Procesar filas
# 3. Procesar filas
        for result in results:
            row_values = []
            for col in columns:
                value = result.get(col, 'N/A')
                width = col_widths[col]
                
                if isinstance(value, (int, float)):
                    # Formato numérico con separador de miles
                    formatted_value = f"{value:,.0f}" if isinstance(value, float) and value % 1 != 0 else f"{value:,}"
                    row_values.append(f"{formatted_value:>{width}}") # Alineación a la derecha para números
                elif isinstance(value, datetime):
                    formatted_value = value.strftime('%Y-%m-%d')
                    row_values.append(f"{formatted_value:<{width}}")
                else:
                    # CORRECCIÓN AQUÍ: Definición de formatted_value para texto
                    formatted_value = str(value) 
                    row_values.append(f"{formatted_value:<{width}}") # Alineación a la izquierda para texto
            
            row_str = " | ".join(row_values)
            output.append(row_str)
            
        return "\n".join(output)
    
    
    def get_master_table_info(self) -> Dict[str, Any]:
        """Obtener información de tabla (dual mode)"""
        if self.mode == ExecutionMode.TRADITIONAL:
            if not self.table_loaded:
                return {'loaded': False, 'mode': 'traditional', 'message': 'No hay tabla cargada en RAM'}
            
            return {
                'loaded': True,
                'mode': 'traditional',
                'data_source': 'RAM',
                'table_info': self.table_info,
                'analysis_summary': {
                    'dimensions_found': self.analysis_result.get('summary', {}).get('dimensions_count', 0),
                    'metrics_found': self.analysis_result.get('summary', {}).get('metrics_count', 0),
                    'total_columns': self.analysis_result.get('summary', {}).get('total_columns', 0)
                }
            }
        elif self.mode == ExecutionMode.CLICKHOUSE:
            if not self.clickhouse_manager or not self.clickhouse_manager.is_connected:
                return {'loaded': False, 'mode': 'clickhouse', 'message': 'No hay conexión ClickHouse activa'}
            
            ch_info = self.clickhouse_manager.get_table_info()
            
            return {
                'loaded': True,
                'mode': 'clickhouse',
                'data_source': 'CLICKHOUSE',
                'used_ram': False,
                'table_info': ch_info['table_info'],
                'schema': ch_info.get('schema', {}),
                'config': ch_info['config'],
                'tableanalyzer_metadata': ch_info.get('tableanalyzer_metadata', {}),
                'clickhouse_status': {
                    'connected': ch_info['connected'],
                    'table_name': self.selected_table,
                    'database': self.clickhouse_config.get('database', 'datos_imperiales'),
                    'host': self.clickhouse_config.get('host', 'localhost')
                }
            }
        else:
            return {'loaded': False, 'mode': 'unknown', 'message': f'Modo desconocido: {self.mode.value}'}
    
    
    def get_master_session_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de la sesión con info del pool"""
        duration = datetime.now() - self.session_stats['session_start']
        success_rate = 0
        if self.session_stats['total_queries'] > 0:
            success_rate = (self.session_stats['successful_queries'] / self.session_stats['total_queries']) * 100
        
        avg_confidence = 0
        if self.query_history:
            confidences = [entry.get('confidence', 0) for entry in self.query_history if entry.get('success')]
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        base_stats = {
            'session_duration': str(duration),
            'total_queries': self.session_stats['total_queries'],
            'successful_queries': self.session_stats['successful_queries'],
            'failed_queries': self.session_stats['failed_queries'],
            'success_rate': f"{success_rate:.1f}%",
            'average_confidence': f"{avg_confidence:.2f}",
            'current_mode': self.mode.value,
            'system_mode': f'MASTER_DUAL_{self.mode.value.upper()}'
        }
        
        # Agregar información específica del modo
        if self.mode == ExecutionMode.TRADITIONAL:
            base_stats.update({
                'table_loaded': self.table_loaded,
                'dataframe_available': self.current_dataframe is not None
            })
        elif self.mode == ExecutionMode.CLICKHOUSE:
            base_stats.update({
                'mysql_connected': self.mysql_manager.is_connected if self.mysql_manager else False,
                'mysql_table': self.selected_table,
                'mysql_database': self.mysql_config['database'] if self.mysql_config else None,
                'connection_type': 'pool'  # Indicar que usa pool
            })
            
            # ===== CAMBIO 8: Agregar estadísticas del pool si están disponibles =====
            if self.mysql_manager and hasattr(self.mysql_manager, 'query_manager'):
                pool_stats = self.mysql_manager.query_manager.get_manager_status()
                base_stats['pool_statistics'] = {
                    'active_queries': pool_stats['active_queries'],
                    'queued_queries': pool_stats['queued_queries'],
                    'total_pool_queries': pool_stats['stats']['total_queries'],
                    'avg_wait_time_ms': int(pool_stats['stats']['avg_wait_time'] * 1000),
                    'avg_execution_time_ms': int(pool_stats['stats']['avg_execution_time'] * 1000)
                }
        
        feedback_summary = self.get_feedback_summary()
        base_stats.update({
            'user_satisfaction': feedback_summary
        })
                    
        return base_stats
    
    
    # ===== NUEVO MÉTODO: Para obtener estadísticas del pool =====
    def get_pool_statistics(self) -> Dict[str, Any]:
        """Obtener estadísticas detalladas del pool de conexiones"""
        if self.mode != ExecutionMode.CLICKHOUSE:
            return {'error': 'No en modo MySQL', 'mode': self.mode.value}
        
        if not self.mysql_manager:
            return {'error': 'MySQL manager no disponible'}
        
        if not hasattr(self.mysql_manager, 'query_manager'):
            return {'error': 'Pool de conexiones no configurado'}
        
        # Obtener estado completo del pool y gestor
        manager_status = self.mysql_manager.query_manager.get_manager_status()
        
        return {
            'pool': manager_status['pool_status'],
            'queries': {
                'active': manager_status['active_queries'],
                'queued': manager_status['queued_queries'],
                'stats': manager_status['stats']
            },
            'performance': {
                'avg_wait_time_ms': int(manager_status['stats']['avg_wait_time'] * 1000),
                'avg_execution_time_ms': int(manager_status['stats']['avg_execution_time'] * 1000),
                'total_queries_processed': manager_status['stats']['total_queries']
            },
            'capacity': {
                'max_concurrent_queries': manager_status['max_concurrent'],
                'pool_size': manager_status['pool_status']['pool_size'],
                'current_active_connections': manager_status['pool_status']['active_connections']
            }
        }


# =============================================================================
# 11. INTERFAZ DE USUARIO
# =============================================================================

class MasterInterface:
    """Interfaz maestro principal con selector de modo y optimización MySQL"""
    
    def __init__(self):
        self.logger = create_logger()
        
        self.logger.dev_log("🎯 INICIALIZANDO INTERFAZ MAESTRO CON SELECTOR DE MODO", "main")
        
        # Configuración de modo
        self.mode = None
        self.mode_selected = False
        
        # Componentes principales
        self.executor = None
        self.running = True
        
        # Configuración MySQL
        self.mysql_config = None
        self.selected_table = None
    
    def run(self):
        """Ejecutar interfaz con selección de modo inicial"""
        print("\n" + "=" * 80)
        print("🎯 SISTEMA MAESTRO DE CONSULTAS")
        print("=" * 80)
        print("🧠 Consultas en lenguaje natural sobre tus datos")
        print("⚡ Análisis inteligente y respuestas instantáneas")
        print("🚀 Optimización automática de MySQL")
        print("=" * 80)
        
        # Selección de modo
        if not self._select_operation_mode():
            print("\n❌ No se pudo configurar el modo de operación")
            return
        
        # Inicializar executor según modo
        self._initialize_executor_for_mode()
        
        # Mostrar ayuda específica del modo
        self._show_mode_specific_help()
        
        # Loop principal
        while self.running:
            try:
                self._show_clean_status()
                command = input(f"\n🎯 Tu consulta ({self.mode}): ").strip()
                
                if not command:
                    continue
                
                self._process_clean_command(command)
                
            except KeyboardInterrupt:
                print("\n\n👋 ¡Hasta pronto!")
                break
            except Exception as e:
                self.logger.user_message("Error inesperado del sistema", "error")
                self.logger.dev_log(f"❌ Error inesperado en interfaz: {e}", "main", "error")
                self.logger.dev_log(traceback.format_exc(), "main", "error")
        
        self._show_clean_final_stats()
    
    def _select_operation_mode(self) -> bool:
        """Seleccionar modo de operación"""
        print(f"\n🔀 SELECCIÓN DE MODO DE OPERACIÓN")
        print("📊 Elige cómo quieres trabajar con tus datos:")
        print()
        print("1️⃣  TRADICIONAL - Cargar archivo y analizar")
        print("    • Sube un archivo CSV/Excel")
        print("    • Análisis completo con TableAnalyzer")
        print("    • Consultas sobre datos en memoria")
        print("    • Ideal para: análisis exploratorio")
        print()
        
        while True:
            try:
                choice = input("🎯 Selecciona modo (1=Tradicional, 2=clickhouse): ").strip()
                
                if choice == '1':
                    self.mode = 'traditional'
                    self.logger.user_message("✅ Modo TRADICIONAL seleccionado", "success")
                    self.logger.dev_log("🔀 MODO SELECCIONADO: TRADICIONAL", "main")
                    return True
                
                elif choice == '2':
                    self.mode = 'clickhouse'
                    self.logger.user_message("🚀 Modo CLICKHOUSE seleccionado", "success")
                    self.logger.dev_log("🔀 MODO SELECCIONADO: CLICKHOUSE", "main")
                    
                    # Configurar ClickHouse inmediatamente
                    if self._configure_clickhouse_mode():
                        return True
                    else:
                        self.logger.user_message("❌ Error configurando MySQL, volviendo a selección", "error")
                        continue
                
                else:
                    print("❌ Opción inválida. Usa 1 o 2")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Saliendo...")
                return False
        
    def _configure_clickhouse_mode(self) -> bool:
        """Configurar modo ClickHouse"""
        print(f"\n🔧 CONFIGURACIÓN CLICKHOUSE")
        print("=" * 40)
        
        # Usar configuración por defecto
        use_default = input("\n🎯 ¿Usar configuración por defecto? (s/n): ").strip().lower()
        
        if use_default in ['s', 'si', 'sí', 'y', 'yes', '']:
            self.clickhouse_config = CONFIG.DEFAULT_CLICKHOUSE_CONFIG.copy()
            print("✅ Usando configuración por defecto")
        else:
            print("\n📝 Ingresa configuración personalizada:")
            try:
                default_config = CONFIG.DEFAULT_CLICKHOUSE_CONFIG
                host = input(f"🌐 Host ({default_config['host']}): ").strip() or default_config['host']
                port = input(f"🚪 Puerto ({default_config['port']}): ").strip() or str(default_config['port'])
                database = input(f"🗄️ Base de datos ({default_config['database']}): ").strip() or default_config['database']
                user = input(f"👤 Usuario ({default_config['user']}): ").strip() or default_config['user']
                password = input("🔐 Contraseña: ").strip() or default_config['password']
                
                self.clickhouse_config = {
                    'host': host,
                    'port': int(port),
                    'database': database,
                    'user': user,
                    'password': password
                }
                
            except ValueError:
                self.logger.user_message("❌ Puerto debe ser un número", "error")
                return False
        
        # Probar conexión y listar tablas
        return self._test_clickhouse_connection_and_select_table()
        
        
    def _test_clickhouse_connection_and_select_table(self) -> bool:
        """Probar conexión y seleccionar tabla desde ClickHouse"""
        try:
            if not HAS_CLICKHOUSE:
                self.logger.user_message("❌ clickhouse-connect no instalado", "error")
                self.logger.user_message("💡 Instala con: pip install clickhouse-connect", "info")
                return False
            
            self.logger.user_message("🔌 Conectando a ClickHouse...", "processing")
            
            # Probar conexión
            client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                username=self.clickhouse_config.get('user', 'default'),
                password=self.clickhouse_config.get('password', ''),
                database=self.clickhouse_config.get('database', 'datos_imperiales')
            )
            
            # Obtener tablas desde metadata
            result = client.query("""
                SELECT table_name, original_filename, total_rows, total_columns
                FROM tablas_metadata 
                ORDER BY analyzed_at DESC
            """)
            
            tables = []
            for row in result.result_rows:
                tables.append({
                    'name': row[0],
                    'original': row[1],
                    'rows': row[2],
                    'cols': row[3]
                })
            
            if not tables:
                # Si no hay en metadata, mostrar todas las tablas
                result = client.query("""
                    SELECT name FROM system.tables 
                    WHERE database = 'datos_imperiales'
                    AND name NOT IN ('tablas_metadata', 'consultas_log')
                """)
                for row in result.result_rows:
                    tables.append({'name': row[0], 'original': 'N/A', 'rows': 0, 'cols': 0})
            
            if not tables:
                self.logger.user_message("❌ No hay tablas en ClickHouse", "error")
                return False
            
            print(f"\n📋 TABLAS DISPONIBLES:")
            for i, t in enumerate(tables, 1):
                print(f"{i}. {t['name']}")
                if t['original'] != 'N/A':
                    print(f"   Archivo: {t['original']}")
                    print(f"   Datos: {t['rows']:,} × {t['cols']}")
            
            # Seleccionar
            choice = int(input(f"\nSelecciona tabla (1-{len(tables)}): ")) - 1
            self.selected_table = tables[choice]['name']
            
            print(f"✅ Tabla seleccionada: {self.selected_table}")
            client.close()
            return True
            
        except Exception as e:
            self.logger.user_message(f"❌ Error: {e}", "error")
            return False
        
    def _initialize_executor_for_mode(self):
        """Inicializar executor según modo seleccionado"""
        if self.mode == 'traditional':
            self.executor = MasterQueryExecutor()
        elif self.mode == 'clickhouse':
            self.executor = MasterQueryExecutor()
            
            # Configurar modo ClickHouse
            self.executor.set_clickhouse_mode(self.clickhouse_config, self.selected_table)
            
            if not self.executor.clickhouse_manager or not self.executor.clickhouse_manager.is_connected:
                self.logger.user_message("❌ Error estableciendo conexión ClickHouse en executor", "error")
            
    def _show_mode_specific_help(self):
        """Mostrar ayuda específica del modo"""
        if self.mode == 'traditional':
            print(f"\n📊 MODO TRADICIONAL ACTIVO")
            print("=" * 50)
            print("📂 Primero carga un archivo con 'cargar'")
            print("🧠 Luego haz consultas en lenguaje natural")
            print("⚡ Los datos se procesan en memoria")
            self.show_clean_help()
            
        elif self.mode == 'mysql':
            print(f"\n🗄️ MODO MYSQL ACTIVO (TABLEANALYZER)")
            print("=" * 50)
            print(f"📋 Tabla activa: {self.selected_table}")
            print(f"🗄️ Base de datos: {self.mysql_config['database']}")
            print("🔧 Conectado a datos procesados por TableAnalyzer")
            print("🚀 MySQL optimizado automáticamente")
            print("🧠 Haz consultas directamente sobre la tabla normalizada")
            print("⚡ Sin necesidad de cargar archivos")
            print()
            print("💡 EJEMPLOS DE CONSULTAS:")
            print("• 'top 10 productos con mayor venta'")
            print("• 'suma total de ingresos'")
            print("• 'promedio de precio por categoría'")
            print("• 'ventas por tienda'")
            print("• 'total ventas entre semana 5 y 9'")
            print()
            print("🚀 COMANDOS DE OPTIMIZACIÓN:")
            print("• 'optimizar' - Optimizar MySQL manualmente")
            print("• 'estado mysql' - Ver estado de optimización")
    
    def _show_clean_status(self):
        """Mostrar estado según modo"""
        if self.mode == 'traditional':
            table_info = self.executor.get_master_table_info()
            
            if table_info['loaded']:
                info = table_info['table_info']
                print(f"\n📊 Datos cargados: {info['file_name']} ({info['rows']:,} filas)")
            else:
                print(f"\n📂 Sin datos cargados")
                
        elif self.mode == 'mysql':
            table_info = self.executor.get_master_table_info()
            
            if table_info['loaded'] and 'mysql_status' in table_info:
                mysql_status = table_info['mysql_status']
                ta_metadata = table_info.get('tableanalyzer_metadata', {})
                
                # Estado de optimización
                optimization_status = table_info.get('optimization_status', {})
                opt_icon = "🚀" if optimization_status.get('optimized', False) else "⚠️"
                
                if ta_metadata.get('source') == 'TableAnalyzer':
                    original_file = ta_metadata.get('original_filename', 'N/A')
                    rows = ta_metadata.get('total_rows', 0)
                    print(f"\n🗄️ MySQL {opt_icon}: {original_file} ({rows:,} filas) - Desde TableAnalyzer")
                else:
                    print(f"\n🗄️ MySQL {opt_icon}: {mysql_status['table_name']} - {mysql_status['database']}")
            else:
                print(f"\n🗄️ Sin conexión MySQL")
    
    
    def _process_clean_command(self, command: str):
        """Procesar comando del usuario"""
        cmd_lower = command.lower()
        
        # Comandos del sistema
        if cmd_lower in ['salir', 'exit', 'quit', 'bye']:
            self.running = False
            return
        
        elif cmd_lower in ['errores', 'errors', 'problemas']:
            self._show_error_sessions_report()
            return
        
        elif cmd_lower in ['limpiar', 'cleanup', 'clean']:
            self._cleanup_old_sessions()
            return
        
        elif cmd_lower.startswith('limpiar '):
            try:
                days = int(command.split()[1])
                self._cleanup_old_sessions(days)
            except (ValueError, IndexError):
                self.logger.user_message("❌ Uso: 'limpiar [días]' (ej: 'limpiar 30')", "error")
            return
        
        elif cmd_lower in ['ayuda', 'help', '?']:
            self.show_clean_help()
            return
        
        elif cmd_lower in ['cargar', 'load', 'datos']:
            self._handle_clean_load()
            return
        
        elif cmd_lower.startswith('cargar '):
            file_path = command[7:].strip()
            self._clean_load_table(file_path)
            return
        
        elif cmd_lower in ['info', 'tabla']:
            self._show_clean_table_info()
            return
        
        elif cmd_lower in ['stats', 'estadisticas']:
            self._show_clean_stats()
            return
        
        elif cmd_lower in ['ejemplo', 'ejemplos', 'examples']:
            self._show_examples()
            return
        
        elif cmd_lower in ['test paralelo', 'test parallel', 'verificar']:
            self._verify_parallel_config()
            return

        elif cmd_lower in ['reporte rendimiento', 'performance report']:
            self._show_performance_report()
            return
        
        # Validación por modo
        if self.mode == 'traditional':
            if not self.executor.table_loaded:
                self.logger.user_message("Primero necesitas cargar datos", "warning")
                self.logger.user_message("💡 Escribe: 'cargar' para subir un archivo", "info")
                return
        elif self.mode == 'mysql':
            if not self.executor.mysql_manager or not self.executor.mysql_manager.is_connected:
                self.logger.user_message("No hay conexión MySQL activa", "warning")
                self.logger.user_message("💡 Verifica la conexión a la base de datos", "info")
                return
        
        # Si no es comando del sistema, tratar como consulta natural
        self._handle_clean_natural_query(command)
    
    def _verify_parallel_config(self):
        if self.executor.mysql_manager:
            verification = self.executor.mysql_manager._verify_mysql80_optimizations()
            print(f"\n📊 Verificación de paralelización: {verification['success_rate']:.1f}%")

    def _show_performance_report(self):
        if self.executor.mysql_manager:
            report = self.executor.mysql_manager.get_query_performance_report()
            # Mostrar configuraciones actuales
        
    def _handle_clean_load(self):
        """Manejar carga limpia de tabla"""
        print("\n📂 CARGAR DATOS")
        print("-" * 40)
        
        file_path = input("📄 Ruta del archivo (CSV/Excel): ").strip()
        
        if not file_path:
            self.logger.user_message("Ruta vacía", "warning")
            return
        
        self._clean_load_table(file_path)
    
    def _clean_load_table(self, file_path: str):
        """Cargar tabla con experiencia completamente limpia"""
        result = self.executor.load_and_analyze_table(file_path)
        
        if result['success']:
            self._show_examples()
        else:
            error = result.get('error', 'Error desconocido')
            self.logger.user_message(f"No se pudo cargar: {error}", "error")
            self.logger.user_message("💡 Verifica que el archivo exista y sea válido", "info")
    
    def _handle_clean_natural_query(self, query: str):
        """Manejar consulta natural con interfaz completamente limpia"""
        self.executor.execute_complete_master_flow(query)
    
    def _show_clean_table_info(self):
        """Mostrar información básica y limpia de tabla al usuario"""
        table_info = self.executor.get_master_table_info()
        
        if not table_info['loaded']:
            self.logger.user_message("No hay datos cargados", "warning")
            if self.mode == 'traditional':
                self.logger.user_message("💡 Usa 'cargar' para subir un archivo", "info")
            return
        
        if self.mode == 'traditional':
            info = table_info['table_info']
            analysis = table_info['analysis_summary']
            
            print(f"\n📊 INFORMACIÓN DE LOS DATOS")
            print("=" * 50)
            print(f"📄 Archivo: {info['file_name']}")
            print(f"📊 Tamaño: {info['rows']:,} filas × {info['columns']} columnas")
            print(f"📅 Cargado: {info['loaded_at'][:19].replace('T', ' ')}")
            print(f"🎯 Análisis: {analysis['dimensions_found']} dimensiones, {analysis['metrics_found']} métricas")
            
            print(f"\n📋 COLUMNAS DISPONIBLES:")
            for i, col in enumerate(info['column_names'], 1):
                print(f"   {i:2d}. {col}")
        
        elif self.mode == 'mysql':
            print(self.executor.mysql_manager.show_table_preview())
    
    def _show_clean_stats(self):
        """Mostrar estadísticas básicas y limpias al usuario"""
        stats = self.executor.get_master_session_stats()
        
        print(f"\n📈 ESTADÍSTICAS DE SESIÓN")
        print("=" * 40)
        print(f"📊 Consultas realizadas: {stats['total_queries']}")
        print(f"✅ Exitosas: {stats['successful_queries']}")
        print(f"🎯 Tasa de éxito: {stats['success_rate']}")
        print(f"🧠 Confianza promedio: {stats['average_confidence']}")
        print(f"⏰ Tiempo activo: {stats['session_duration']}")
        print(f"🎯 Modo: {stats['current_mode']}")
        
        # Mostrar estado de optimización MySQL
        if self.mode == 'mysql' and stats.get('mysql_connected', False):
            if self.executor.mysql_manager:
                opt_status = self.executor.mysql_manager.get_optimization_status()
                opt_icon = "🚀" if opt_status.get('optimized', False) else "⚠️"
                print(f"🗄️ MySQL: {opt_icon} {'Optimizado' if opt_status.get('optimized', False) else 'Sin optimizar'}")
    
    def _show_examples(self):
        """Mostrar ejemplos de consultas limpios"""
        print(f"\n💡 EJEMPLOS DE CONSULTAS:")
        print("-" * 50)
        print("• 'top 10 productos con mayor venta'")
        print("• 'suma total de ingresos'")
        print("• 'cuenta cuántos clientes hay'")
        print("• 'promedio de precio por categoría'")
        print("• 'clientes con facturación mayor a 5000'")
        print("• 'ventas por mes del año pasado'")
        print("• 'producto más vendido'")
        print("• 'total de ventas por región'")
    
    def _show_error_sessions_report(self):
        """Mostrar reporte de sesiones con errores"""
        report = get_error_sessions_report()
        
        if report['status'] == 'no_error_sessions':
            self.logger.user_message("✅ No hay sesiones con errores registradas", "success")
            return
        
        print(f"\n📊 REPORTE DE SESIONES CON ERRORES")
        print("=" * 50)
        print(f"📈 Total sesiones problemáticas: {report['total_error_sessions']}")
        print(f"❌ Total errores: {report['total_errors']}")
        print(f"⚠️ Total warnings: {report['total_warnings']}")
        
        if report.get('most_common_errors'):
            print(f"\n🔍 TIPOS DE ERRORES MÁS COMUNES:")
            for error_type, count in sorted(report['most_common_errors'].items(), 
                                          key=lambda x: x[1], reverse=True):
                print(f"   • {error_type}: {count} veces")
    
    def _cleanup_old_sessions(self, days_old: int = None):
        """Limpiar sesiones antiguas"""
        if days_old is None:
            days_old = CONFIG.CLEANUP_DAYS_DEFAULT
        
        sessions_cleaned = cleanup_old_sessions(days_old)
        self.logger.user_message(f"🧹 Limpieza completada: {sessions_cleaned} sesiones eliminadas", "success")
    
    def _show_clean_final_stats(self):
        """Mostrar estadísticas finales limpias al finalizar"""
        stats = self.executor.get_master_session_stats()
        
        print(f"\n📊 RESUMEN DE SESIÓN")
        print("=" * 50)
        print(f"📊 Total consultas: {stats['total_queries']}")
        print(f"✅ Exitosas: {stats['successful_queries']}")
        print(f"🎯 Tasa de éxito: {stats['success_rate']}")
        print(f"⏰ Duración: {stats['session_duration']}")
        print(f"🎯 Modo: {stats['current_mode']}")
        
        # Estado de optimización MySQL
        if self.mode == 'mysql' and stats.get('mysql_connected', False):
            if self.executor.mysql_manager:
                opt_status = self.executor.mysql_manager.get_optimization_status()
                if opt_status.get('optimized', False):
                    print(f"🚀 MySQL: Optimizado durante toda la sesión")
        
        print(f"\n👋 ¡Gracias por usar el Sistema Maestro!")
        
        # Finalizar logger
        self.logger.finalize_session()
    
    def show_clean_help(self):
        """Mostrar ayuda según modo"""
        if self.mode == 'traditional':
            print(f"\n🤖 CÓMO USAR EL SISTEMA (MODO TRADICIONAL)")
            print("=" * 50)
            
            print(f"\n📂 CARGAR DATOS:")
            print("• Escribe 'cargar' para subir un archivo")
            print("• O 'cargar ruta/archivo.csv' directamente")
            
            print(f"\n🧠 HACER CONSULTAS:")
            print("• Escribe preguntas en lenguaje natural")
            print("• Ejemplos: 'top 5 productos', 'suma de ventas'")
            
        elif self.mode == 'mysql':
            print(f"\n🤖 CÓMO USAR EL SISTEMA (MODO MYSQL)")
            print("=" * 50)
            
            print(f"\n🧠 HACER CONSULTAS:")
            print("• Escribe preguntas en lenguaje natural")
            print("• Las consultas se ejecutan directamente en MySQL")
            print("• Ejemplos: 'top 5 productos', 'suma de ventas'")
            
            print(f"\n🚀 OPTIMIZACIÓN MYSQL:")
            print("• 'optimizar' - Optimizar rendimiento de MySQL")
            print("• 'optimizar forzar' - Re-optimizar forzadamente")
            print("• 'estado mysql' - Ver estado de optimización")
        
        # Comandos comunes
        print(f"\n📋 COMANDOS ÚTILES:")
        print("• 'info' - Ver información de tus datos")
        print("• 'stats' - Ver estadísticas de la sesión")
        print("• 'ejemplos' - Ver más ejemplos de consultas")
        print("• 'errores' - Ver reporte de sesiones con problemas")
        print("• 'limpiar' - Limpiar sesiones de error antiguas")
        print("• 'ayuda' - Mostrar esta ayuda")
        print("• 'salir' - Terminar")


# =============================================================================
# 12. FUNCIONES DE UTILIDAD Y REPORTES
# =============================================================================

def get_error_sessions_report() -> Dict[str, Any]:
    """Obtener reporte de sesiones con errores"""
    error_sessions_index = CONFIG.CONTROL_DIR / "error_sessions_index.json"
    
    if not error_sessions_index.exists():
        return {
            'status': 'no_error_sessions',
            'message': 'No se han registrado sesiones con errores'
        }
    
    try:
        with open(error_sessions_index, 'r', encoding='utf-8') as f:
            error_index = json.load(f)
        
        return {
            'status': 'success',
            'total_error_sessions': error_index.get('total_error_sessions', 0),
            'total_errors': error_index.get('total_errors', 0),
            'total_warnings': error_index.get('total_warnings', 0),
            'most_common_errors': error_index.get('error_types_frequency', {}),
            'components_with_most_errors': error_index.get('component_errors', {}),
            'error_sessions': error_index.get('error_sessions', []),
            'recent_error_sessions': error_index.get('error_sessions', [])[-10:]
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error leyendo reporte: {e}'
        }


def cleanup_old_sessions(days_old: int = 30) -> int:
    """Limpiar sesiones antiguas"""
    cutoff_date = datetime.now() - timedelta(days=days_old)
    sessions_cleaned = 0
    
    # Limpiar en errores_encontrados
    if CONFIG.ERRORS_DIR.exists():
        for session_dir in CONFIG.ERRORS_DIR.glob("*_*"):
            if session_dir.is_dir():
                try:
                    # Extraer fecha del nombre de directorio
                    parts = session_dir.name.split("_")
                    if len(parts) >= 2:
                        date_str = parts[1]
                        if len(date_str) == 8:  # YYYYMMDD
                            session_date = datetime.strptime(date_str, "%Y%m%d")
                            
                            if session_date < cutoff_date:
                                shutil.rmtree(session_dir)
                                sessions_cleaned += 1
                                print(f"🗑️ Sesión limpiada: {session_dir.name}")
                except (ValueError, IndexError):
                    continue
    
    return sessions_cleaned


def get_all_sessions_report() -> Dict[str, Any]:
    """Obtener reporte de todas las sesiones"""
    all_sessions_index = CONFIG.CONTROL_DIR / "all_sessions_index.json"
    
    if not all_sessions_index.exists():
        return {
            'status': 'no_sessions',
            'message': 'No se han registrado sesiones'
        }
    
    try:
        with open(all_sessions_index, 'r', encoding='utf-8') as f:
            all_index = json.load(f)
        
        return {
            'status': 'success',
            'total_sessions': all_index.get('total_sessions', 0),
            'sessions': all_index.get('sessions', []),
            'structure_info': {
                'control_dir': str(CONFIG.CONTROL_DIR),
                'errors_dir': str(CONFIG.ERRORS_DIR)
            }
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Error leyendo reporte: {e}'
        }

# =============================================================================
# 13. GESTIÓN GLOBAL DE SESIONES DE USUARIOS
# =============================================================================

_session_manager = UserSessionManager()


def get_user_session_manager() -> UserSessionManager:
    """Obtener el session manager global"""
    return _session_manager


def get_user_logger(user_id: Optional[str] = None) -> BaseLogger:
    """Función principal para obtener logger por usuario"""
    return _session_manager.get_or_create_session(user_id)


def close_user_logger(user_id: Optional[str] = None):
    """Cerrar logger de usuario"""
    _session_manager.close_user_session(user_id)


def get_active_user_sessions() -> Dict[str, str]:
    """Obtener sesiones activas"""
    return _session_manager.get_active_sessions()


def shutdown_session_manager():
    """Cerrar todas las sesiones al salir"""
    _session_manager.close_all_sessions()

# Registrar shutdown automático
atexit.register(shutdown_session_manager)


def get_user_session_report() -> Dict[str, Any]:
    """Reporte de sesiones por usuario"""
    active_sessions = get_active_user_sessions()
    
    user_errors = {}
    if CONFIG.ERRORS_DIR.exists():
        for session_dir in CONFIG.ERRORS_DIR.glob("user_*"):
            if session_dir.is_dir():
                parts = session_dir.name.split("_")
                if len(parts) >= 2:
                    user_id = parts[1]
                    if user_id not in user_errors:
                        user_errors[user_id] = 0
                    user_errors[user_id] += 1
    
    return {
        'active_sessions': active_sessions,
        'users_with_errors': user_errors,
        'total_active_users': len(active_sessions),
        'mode': 'single_user_ready_for_multi'
    }

# =============================================================================
# 14. FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal con auto-detección de modo developer"""
    logger = create_logger()
    
    try:
        # Mensaje según modo
        if CONFIG.DEVELOPER_MODE:
            print("🔧 [DEV] ═══ INICIANDO SISTEMA EN MODO DEVELOPER ═══")
            logger.user_message("Sistema iniciado en MODO DEVELOPER", "info")
            logger.dev_log("🚀 SISTEMA MAESTRO EN MODO DEVELOPER", "main")
        else:
            logger.user_message("Iniciando Sistema Maestro de Consultas", "info")
            logger.dev_log("🚀 INICIANDO SISTEMA MAESTRO PRINCIPAL", "main")
        
        logger.dev_log("=" * 80, "main")
        logger.dev_log(f"🎯 Modo de ejecución: {'DEVELOPER' if CONFIG.DEVELOPER_MODE else 'NORMAL'}", "main")
        
        # Ejecutar interfaz
        interface = MasterInterface()
        interface.run()
        
        logger.dev_log("👋 Sistema maestro finalizado normalmente", "main")
        
    except Exception as e:
        if CONFIG.DEVELOPER_MODE:
            print(f"\n🔧 [DEV-ERROR] ❌ Error del sistema: {e}")
            print(f"🔧 [DEV-ERROR] 📋 Traceback completo:")
            traceback.print_exc()
        else:
            print(f"\n❌ Error del sistema. Revisa los logs para más detalles.")
        
        if logger:
            logger.log_exception(e, component="main", context="Durante ejecución principal")
    
    finally:
        if logger:
            try:
                logger.finalize_session()
                if not CONFIG.DEVELOPER_MODE:
                    if hasattr(logger, 'session_dir_errores'):
                        print(f"\n📂 Logs guardados en: {logger.session_dir_errores}")
            except Exception as e:
                print(f"⚠️ Error finalizando logger: {e}")

# =============================================================================
# 15. FUNCIONES DE COMPATIBILIDAD Y TESTING
# =============================================================================

def test_sql_adapter():
    """Función de testing para el adaptador SQL"""
    print("🧪 TESTING ADAPTADOR SQL SQLITE → MYSQL")

    
    class MockLogger:
        def dev_log(self, msg, component, level="info"):
            print(f"[{component.upper()}] {msg}")
    
    # Crear adaptador
    adapter = SQLiteToMySQLAdapter("tabla_productos_20241203_143022_a1b2c3", MockLogger())
    
    # Consultas de prueba
    test_queries = [
        'SELECT "Store", SUM("Sell-Out") FROM datos WHERE "Week" BETWEEN 202505 AND 202509 GROUP BY "Store" ORDER BY SUM("Sell-Out") DESC LIMIT 6;',
        'SELECT COUNT(*) FROM datos WHERE "Category" = "Electronics";',
        'SELECT "Product", AVG("Price") FROM datos GROUP BY "Product";',
        'SELECT * FROM datos WHERE "Date" > "2024-01-01" ORDER BY "Revenue" DESC;'
    ]
    
    print(f"\n📋 PROBANDO {len(test_queries)} CONSULTAS:")
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. CONSULTA ORIGINAL:")
        print(f"   {query}")
        
        result = adapter.convert_sqlite_to_mysql(query)
        
        if result['success']:
            print(f"✅ CONVERTIDA:")
            print(f"   {result['converted_query']}")
            changes = result['changes_made']
            print(f"🔧 Cambios: {changes['quotes_replaced']} comillas, tabla: {changes['table_replaced']}")
        else:
            print(f"❌ ERROR: {result['error']}")
    
    # Mostrar estadísticas
    print(f"\n📊 ESTADÍSTICAS FINALES:")
    stats = adapter.get_conversion_stats()
    print(f"   Total conversiones: {stats['total_conversions']}")
    print(f"   Total comillas reemplazadas: {stats['total_quotes_replaced']}")
    print(f"   Total tablas reemplazadas: {stats['total_table_replacements']}")
    print(f"   Tabla objetivo: {stats['mysql_table_name']}")


def verify_logger_compatibility():
    """Verificar que el logger tenga todos los métodos necesarios"""
    required_methods = [
        'user_message', 'dev_log', 'start_operation', 'update_operation',
        'complete_operation', 'log_exception', 'show_user_results',
        'finalize_session', 'get_suppressed_content', 'get_session_info'
    ]
    
    # Verificar DeveloperLogger
    dev_logger = DeveloperLogger()
    missing_methods = []
    
    for method in required_methods:
        if not hasattr(dev_logger, method):
            missing_methods.append(method)
    
    if missing_methods:
        print(f"❌ [DEV] Métodos faltantes en DeveloperLogger: {missing_methods}")
        return False
    else:
        print(f"✅ [DEV] DeveloperLogger tiene todos los métodos necesarios")
        return True

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    main()