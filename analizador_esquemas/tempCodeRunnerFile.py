import clickhouse_connect
import pandas as pd
import numpy as np
import os
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Union
from dataclasses import dataclass
from collections import defaultdict
import chardet
import json  
from datetime import datetime 
from enum import Enum
import mysql.connector
from sqlalchemy import create_engine
import hashlib



# -------------------------
# ------ CONEXIONES -------
# -------------------------


# ----- RUTA FIJA PARA DICCIONARIO TEMPORAL -----

TEMPORAL_DICT_DIR = "diccionarios/temporales/"
TEMPORAL_DICT_FILE = "diccionario_temporal_actual.json"
TEMPORAL_DICT_BACKUP_DIR = "diccionarios_temporales/backup"



# Rutas completas

DEFAULT_TEMPORAL_PATH = os.path.join(TEMPORAL_DICT_DIR, TEMPORAL_DICT_FILE)
BACKUP_TEMPORAL_PATH = TEMPORAL_DICT_BACKUP_DIR


# ----- RUTA Diccionarios JSON operacionales -----

DEFAULT_JSON_DICT_PATH = "diccionarios/complejos/"


# =============================================
# CLICKHOUSE INTEGRATION CORREGIDA
# =============================================

class ClickHouseIntegration:
    """
    Integración ClickHouse corregida - sin now() en PARTITION BY
    """
    
    def __init__(self):
        self.config = {
            "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
            "database": os.getenv("CLICKHOUSE_DATABASE", "datos_imperiales"),
            "username": os.getenv("CLICKHOUSE_USER", "default"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
            "secure": bool(os.getenv("CLICKHOUSE_SECURE", "False").lower() == "true")
        }
        
        self.connection_available = False
        self.client = None
        self._test_connection()
    
    def _test_connection(self):
        """Probar conexión ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                username=self.config["username"],
                password=self.config["password"],
                secure=self.config["secure"]
            )
            
            result = self.client.query("SELECT 1")
            self.connection_available = True
            print(f"✅ ClickHouse disponible: {self.config['host']}/{self.config['database']}")
            
        except Exception as e:
            self.connection_available = False
            self.client = None
            print(f"⚠️ ClickHouse no disponible: {e}")
    
    def generate_table_name(self, file_path: str) -> str:
        """Generar nombre único para tabla ClickHouse"""
        file_name = Path(file_path).stem
        
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
        clean_name = re.sub(r'_+', '_', clean_name)
        clean_name = clean_name.strip('_')[:50]
        
        if clean_name and clean_name[0].isdigit():
            clean_name = f"tabla_{clean_name}"
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_hash = hashlib.md5(file_path.encode()).hexdigest()[:6]
        
        return f"datos_{clean_name}_{timestamp}_{path_hash}"
    
    def _prepare_dataframe_for_clickhouse(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Preparar DataFrame para ClickHouse"""
        df_clean = dataframe.copy()
        
        column_mapping = {}
        for col in df_clean.columns:
            clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', str(col))
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')[:64]
            
            if clean_col and clean_col[0].isdigit():
                clean_col = f"col_{clean_col}"
                
            if not clean_col:
                clean_col = f"column_{len(column_mapping)}"
            
            if clean_col != col:
                column_mapping[col] = clean_col
        
        if column_mapping:
            df_clean = df_clean.rename(columns=column_mapping)
            print(f"🔧 Columnas renombradas para ClickHouse: {len(column_mapping)}")
        
        # Convertir tipos de datos
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].astype(str)
            elif df_clean[col].dtype in ['float64', 'int64']:
                df_clean[col] = df_clean[col].fillna(0)
        
        return df_clean
    
    def _create_table_schema(self, dataframe: pd.DataFrame, table_name: str) -> str:
        """Crear esquema de tabla para ClickHouse - CORREGIDO"""
        
        schema_parts = []
        
        for col_name, dtype in dataframe.dtypes.items():
            if dtype == 'object' or dtype.name.startswith('string'):
                ch_type = 'String'
            elif dtype == 'int64':
                ch_type = 'Int64'
            elif dtype == 'float64':
                ch_type = 'Float64'
            elif dtype == 'bool':
                ch_type = 'UInt8'
            elif dtype.name.startswith('datetime'):
                ch_type = 'DateTime'
            else:
                ch_type = 'String'
            
            schema_parts.append(f"`{col_name}` {ch_type}")
        
        # Agregar columnas de sistema
        schema_parts.insert(0, "`id` UInt64")
        schema_parts.append("`created_at` DateTime DEFAULT now()")
        
        schema = ",\n    ".join(schema_parts)
        
        # CORREGIDO: Usar created_at en lugar de now() para partición
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {schema}
        ) 
        ENGINE = MergeTree()
        ORDER BY id
        PARTITION BY toYYYYMM(created_at)
        SETTINGS index_granularity = 8192
        """
        
        return create_sql
    
    def upload_table(self, dataframe: pd.DataFrame, table_name: str, 
                    file_path: str, analysis_info: Dict) -> Dict:
        """Subir tabla a ClickHouse - CORREGIDO"""
        
        if not self.connection_available:
            return {
                'success': False,
                'error': 'ClickHouse no está disponible'
            }
        
        try:
            print(f"Iniciando subida de tabla a ClickHouse: {table_name}")
            
            df_clean = self._prepare_dataframe_for_clickhouse(dataframe)
            
            # Crear tabla con esquema corregido
            create_sql = self._create_table_schema(df_clean, table_name)
            print("Creando tabla en ClickHouse...")
            self.client.command(create_sql)
            print(f"Tabla {table_name} creada exitosamente")
            
            # Preparar datos con IDs y timestamp
            print("Preparando datos para inserción...")
            
            base_id = int(datetime.now().timestamp() * 1000000)
            current_time = datetime.now()
            
            data_rows = []
            for i, (_, row) in enumerate(df_clean.iterrows()):
                # Insertar ID y timestamp al principio y final
                row_data = [base_id + i] + row.tolist() + [current_time]
                data_rows.append(row_data)
            
            # Insertar datos en lotes
            print("Insertando datos en ClickHouse...")
            batch_size = 50000
            total_inserted = 0
            
            # Incluir created_at en column_names
            column_names = ['id'] + list(df_clean.columns) + ['created_at']
            
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i+batch_size]
                self.client.insert(table_name, batch, column_names=column_names)
                total_inserted += len(batch)
                print(f"Insertadas {total_inserted}/{len(data_rows)} filas")
            
            print(f"Tabla {table_name} subida exitosamente a ClickHouse")
            
            # Guardar metadata
            print("Guardando metadata...")
            self._save_metadata(table_name, file_path, analysis_info, len(df_clean), len(df_clean.columns))
            print("Metadata guardada exitosamente")
            
            return {
                'success': True,
                'table_name': table_name,
                'database': self.config['database'],
                'host': self.config['host'],
                'rows': len(df_clean),
                'columns': len(df_clean.columns),
                'message': f'Tabla {table_name} subida exitosamente a ClickHouse'
            }
            
        except Exception as e:
            print(f"ERROR subiendo tabla a ClickHouse: {str(e)}")
            import traceback
            print(f"Traceback completo: {traceback.format_exc()}")
            
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'traceback': traceback.format_exc()
            }
    
    def _save_metadata(self, table_name: str, file_path: str, analysis_info: Dict, rows: int, cols: int):
        """Guardar metadata en ClickHouse - CORREGIDO"""
        try:
            print(f"Iniciando guardado de metadata para tabla: {table_name}")
            
            # CORREGIDO: Crear tabla de metadata sin now() en PARTITION BY
            create_metadata_table = """
            CREATE TABLE IF NOT EXISTS tablas_metadata (
                id UInt64,
                table_name String,
                original_filename String,
                file_path String,
                total_rows UInt64,
                total_columns UInt32,
                dimensions_count UInt32 DEFAULT 0,
                metrics_count UInt32 DEFAULT 0,
                analyzed_at DateTime DEFAULT now()
            ) 
            ENGINE = MergeTree()
            ORDER BY (id, table_name)
            PARTITION BY toYYYYMM(analyzed_at)
            SETTINGS index_granularity = 8192
            """
            
            self.client.command(create_metadata_table)
            print("Tabla tablas_metadata verificada/creada")
            
            # Preparar datos de metadata
            file_info = analysis_info.get('file_info', {})
            summary = analysis_info.get('summary', {})
            
            original_filename = file_info.get('name', Path(file_path).name if file_path else 'unknown')
            dimensions_count = summary.get('dimensions_count', 0)
            metrics_count = summary.get('metrics_count', 0)
            
            metadata_id = int(datetime.now().timestamp() * 1000000)
            current_time = datetime.now()
            
            print(f"Datos a insertar:")
            print(f"  - id: {metadata_id}")
            print(f"  - table_name: {table_name}")
            print(f"  - original_filename: {original_filename}")
            print(f"  - total_rows: {rows}")
            print(f"  - total_columns: {cols}")
            
            metadata_row = [
                metadata_id, table_name, original_filename, file_path or '',
                rows, cols, dimensions_count, metrics_count, current_time
            ]
            
            self.client.insert(
                'tablas_metadata', 
                [metadata_row],
                column_names=['id', 'table_name', 'original_filename', 'file_path', 
                            'total_rows', 'total_columns', 'dimensions_count', 
                            'metrics_count', 'analyzed_at']
            )
            
            print(f"Metadata guardada exitosamente para tabla: {table_name}")
            
        except Exception as e:
            print(f"ERROR CRÍTICO guardando metadata para {table_name}: {str(e)}")
            import traceback
            print(f"Traceback completo: {traceback.format_exc()}")
            raise e
    
    def list_tables(self) -> List[Dict]:
        """Listar tablas en ClickHouse"""
        if not self.connection_available:
            return []
        
        try:
            query = """
            SELECT table_name, original_filename, total_rows, 
                   total_columns, dimensions_count, metrics_count, analyzed_at
            FROM tablas_metadata 
            ORDER BY analyzed_at DESC
            """
            
            result = self.client.query_df(query)
            return result.to_dict('records')
            
        except Exception as e:
            print(f"❌ Error listando tablas ClickHouse: {e}")
            return []
    
    def get_table_stats(self, table_name: str) -> Dict:
        """Obtener estadísticas de una tabla específica"""
        if not self.connection_available:
            return {}
        
        try:
            system_query = f"""
            SELECT 
                sum(rows) as total_rows,
                sum(data_compressed_bytes) as compressed_size_bytes,
                sum(data_uncompressed_bytes) as uncompressed_size_bytes,
                (sum(data_uncompressed_bytes) / sum(data_compressed_bytes)) as compression_ratio
            FROM system.parts 
            WHERE database = '{self.config['database']}' AND table = '{table_name}'
            """
            
            system_result = self.client.query_df(system_query)
            
            metadata_query = f"""
            SELECT * FROM tablas_metadata 
            WHERE table_name = '{table_name}'
            ORDER BY analyzed_at DESC
            LIMIT 1
            """
            
            metadata_result = self.client.query_df(metadata_query)
            
            stats = {}
            if not system_result.empty:
                stats.update(system_result.iloc[0].to_dict())
            
            if not metadata_result.empty:
                stats.update(metadata_result.iloc[0].to_dict())
            
            return stats
            
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def test_connection_detailed(self) -> Dict:
        """Prueba detallada de conexión"""
        try:
            if not self.client:
                self._test_connection()
            
            if not self.connection_available:
                return {
                    'success': False,
                    'error': 'No se pudo establecer conexión'
                }
            
            version_result = self.client.query("SELECT version()")
            database_result = self.client.query(f"SELECT count() FROM system.tables WHERE database = '{self.config['database']}'")
            
            return {
                'success': True,
                'version': version_result.first_row[0] if version_result.row_count > 0 else 'Unknown',
                'database': self.config['database'],
                'tables_count': database_result.first_row[0] if database_result.row_count > 0 else 0,
                'config': {
                    'host': self.config['host'],
                    'port': self.config['port'],
                    'database': self.config['database'],
                    'username': self.config['username']
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }





# =========================================================
# ========== SISTEMA DE CARGA JSON INDEPENDIENTE ==========
# =========================================================

class ComponentType(Enum):
    """Tipos de componentes que puede identificar el parser"""
    DIMENSION = "dimension"
    METRIC = "metric" 
    OPERATION = "operation"
    COLUMN_VALUE = "column_value"
    TEMPORAL = "temporal"
    VALUE = "value"
    CONNECTOR = "connector"
    UNKNOWN = "unknown"

class OperationType(Enum):
    """Tipos de operaciones disponibles"""
    MAXIMUM = "máximo"
    MINIMUM = "mínimo"
    SUM = "suma"
    AVERAGE = "promedio"
    COUNT = "conteo"



# ===============================================
# ========== CARGA DE DICCIONARIO JSON ==========
# ===============================================


class JSONDictionaryLoader:
    """
    🔥 CARGADOR JSON COMPATIBLE CON PALABRAS ANCLA - Mantiene compatibilidad total
    """
    
    def __init__(self, json_path: str = DEFAULT_JSON_DICT_PATH):
        """Inicializar cargador JSON compatible con palabras ancla"""
        
        self.json_path = Path(json_path)
        self.last_loaded = None
        self.load_successful = False
        
        # Diccionarios que se cargarán desde JSON (SETS PLANOS para compatibilidad)
        self.dimensiones = set()
        self.metricas = set()
        self.operaciones = {}
        self.columnas_conocidas = {}
        self.valores_comunes = set()
        self.synonym_groups = {}
        self.frases_compuestas = {}
        self.conectores = set()
        self.numeros_palabras = {}
        self.correcciones_tipograficas = {}
        self.indicadores_temporales = {}
        self.unidades_tiempo = {}
        
        # Para almacenar estructura original de anclas (opcional)
        self.dimension_anchors = {}
        self.metric_anchors = {}
        
        # Intentar cargar desde JSON
        self._load_all_dictionaries()
    
    def _load_all_dictionaries(self) -> bool:
        """Carga todos los diccionarios desde archivos JSON con palabras ancla"""
        
        print(f"📁 CARGANDO DICCIONARIOS CON PALABRAS ANCLA")
        print(f"📂 Ruta: {self.json_path.absolute()}")
        print("="*60)
        
        try:
            # Verificar que existe la estructura básica (ACTUALIZADA)
            if not self._verify_json_structure():
                print(f"❌ Estructura de archivos JSON incompleta")
                self._load_fallback_dictionaries()
                return False
            
            # Cargar archivos core (obligatorios) - NUEVAS RUTAS
            success_count = 0
            total_files = 0
            
            # 🚨 NUEVAS CARGAS: Desde anchors/
            if self._load_dimension_anchors():
                success_count += 1
            total_files += 1
            
            if self._load_metric_anchors():
                success_count += 1
            total_files += 1
            
            if self._load_operations():
                success_count += 1
            total_files += 1
            
            # Core optional files
            self._load_known_columns()
            self._load_common_values()
            
            # Linguistic files (mantener compatibilidad si existen)
            self._load_connectors()
            self._load_word_numbers()
            self._load_typo_corrections()
            
            # Temporal files
            self._load_temporal_indicators()
            self._load_temporal_units()
            
            self.last_loaded = datetime.now()
            self.load_successful = success_count >= 2  # Al menos dimension_anchors y metric_anchors
            
            print(f"\n📊 RESULTADO DE CARGA CON PALABRAS ANCLA:")
            print(f"   ✅ Archivos básicos cargados: {success_count}/{total_files}")
            print(f"   📂 Dimensiones (expandidas): {len(self.dimensiones)}")
            print(f"   📊 Métricas (expandidas): {len(self.metricas)}")
            print(f"   ⚡ Operaciones: {len(self.operaciones)}")
            print(f"   📍 Anclas de dimensión: {len(self.dimension_anchors)}")
            print(f"   📍 Anclas de métrica: {len(self.metric_anchors)}")
            print(f"   🕐 Cargado: {self.last_loaded.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if self.load_successful:
                print(f"✅ Diccionarios con palabras ancla cargados exitosamente")
                print(f"🔥 TODAS LAS VARIACIONES DISPONIBLES: {len(self.dimensiones) + len(self.metricas)} términos")
            else:
                print(f"⚠️ Carga parcial - usando fallback donde sea necesario")
            
            return self.load_successful
            
        except Exception as e:
            print(f"❌ Error cargando diccionarios con palabras ancla: {e}")
            self._load_fallback_dictionaries()
            return False
    
    def _verify_json_structure(self) -> bool:
        """Verifica que existan los archivos JSON de palabras ancla"""
        
        # 🚨 NUEVAS RUTAS REQUERIDAS
        required_files = [
            "anchors/dimension_anchors.json",
            "anchors/metric_anchors.json", 
            "core/operations.json"
        ]
        
        missing_files = []
        for file_path in required_files:
            full_path = self.json_path / file_path
            if not full_path.exists():
                missing_files.append(file_path)
        
        if missing_files:
            print(f"❌ Archivos JSON faltantes:")
            for file in missing_files:
                print(f"   • {file}")
            return False
        
        print(f"✅ Estructura JSON de palabras ancla verificada")
        return True
    
    
    def _load_json_file(self, relative_path: str, default_value=None):
        """Carga un archivo JSON específico"""
        
        file_path = self.json_path / relative_path
        
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"   ✅ {relative_path}")
                return data
            else:
                print(f"   ⚠️ No encontrado: {relative_path}")
                return default_value
        except Exception as e:
            print(f"   ❌ Error en {relative_path}: {e}")
            return default_value
    
    
    def _load_dimension_anchors(self) -> bool:
        """🚨 NUEVO: Carga dimension_anchors y convierte a set plano"""
        data = self._load_json_file("anchors/dimension_anchors.json", {})
        if data:
            # Guardar estructura original de anclas
            self.dimension_anchors = data
            
            # Convertir a set plano para compatibilidad
            flat_dimensions = set()
            for anchor, synonyms in data.items():
                # Agregar la palabra ancla misma
                flat_dimensions.add(anchor)
                # Agregar todos los sinónimos
                for synonym in synonyms:
                    flat_dimensions.add(synonym.lower())
            
            self.dimensiones = flat_dimensions
            print(f"   🔥 Expandido: {len(data)} anclas → {len(flat_dimensions)} dimensiones")
            return True
        return False
    
    def _load_metric_anchors(self) -> bool:
        """🚨 NUEVO: Carga metric_anchors y convierte a set plano"""
        data = self._load_json_file("anchors/metric_anchors.json", {})
        if data:
            # Guardar estructura original de anclas
            self.metric_anchors = data
            
            # Convertir a set plano para compatibilidad
            flat_metrics = set()
            for anchor, synonyms in data.items():
                # Agregar la palabra ancla misma
                flat_metrics.add(anchor)
                # Agregar todos los sinónimos
                for synonym in synonyms:
                    flat_metrics.add(synonym.lower())
            
            self.metricas = flat_metrics
            print(f"   🔥 Expandido: {len(data)} anclas → {len(flat_metrics)} métricas")
            return True
        return False
    
    def _load_operations(self) -> bool:
        """Carga operaciones desde JSON y convierte a enums"""
        data = self._load_json_file("core/operations.json", {})
        if data:
            # Mapeo de strings a enums
            string_to_enum = {
                "máximo": OperationType.MAXIMUM,
                "mínimo": OperationType.MINIMUM,
                "suma": OperationType.SUM,
                "promedio": OperationType.AVERAGE,
                "conteo": OperationType.COUNT
            }
            
            for key, value in data.items():
                self.operaciones[key] = string_to_enum.get(value, value)
            
            return True
        return False
    
    def _load_known_columns(self):
        """Carga columnas conocidas desde JSON"""
        data = self._load_json_file("core/known_columns.json", {})
        self.columnas_conocidas = data or {}
    
    def _load_common_values(self):
        """Carga valores comunes desde JSON"""
        data = self._load_json_file("core/common_values.json", [])
        self.valores_comunes = set(data) if data else set()
    
    def _load_connectors(self):
        """Carga conectores desde JSON"""
        data = self._load_json_file("linguistic/connectors.json", [])
        self.conectores = set(data) if data else set()
    
    def _load_word_numbers(self):
        """Carga números en palabras desde JSON"""
        data = self._load_json_file("linguistic/word_numbers.json", {})
        self.numeros_palabras = data or {}
    
    def _load_typo_corrections(self):
        """Carga correcciones tipográficas desde JSON"""
        data = self._load_json_file("linguistic/typo_corrections.json", {})
        self.correcciones_tipograficas = data or {}
    
    def _load_temporal_indicators(self):
        """Carga indicadores temporales desde JSON"""
        data = self._load_json_file("temporal/temporal_indicators.json", {})
        self.indicadores_temporales = data or {}
    
    def _load_temporal_units(self):
        """Carga unidades temporales desde JSON y convierte a enums"""
        data = self._load_json_file("temporal/temporal_units.json", {})
        if data:
            # Mapeo de strings a enums (si necesitas usar enums)
            # Por ahora mantengo como strings para simplicidad
            self.unidades_tiempo = data
    
    def _load_fallback_dictionaries(self):
        """Carga diccionarios básicos de fallback (EXPANDIDO)"""
        
        print(f"🔄 CARGANDO DICCIONARIOS DE FALLBACK EXPANDIDOS")
        
        # 🚨 FALLBACK EXPANDIDO: Incluye algunas variaciones básicas
        self.dimensiones = {
            # Básicos originales
            'vendedor', 'empleado', 'personal', 'categoria', 'marca', 'linea', 'tipo', 'clase',
            'region', 'zona', 'area', 'territorio', 'ciudad', 'pais', 'estado', 'provincia',
            'store', 'tienda', 'sucursal', 'establecimiento', 'stock_out', 'account', 'cliente', 
            'usuario', 'item', 'producto', 'articulo', 'product_group', 'grupo',
            
            # 🔥 VARIACIONES BÁSICAS para fallback
            'empleados', 'vendedores', 'categorias', 'marcas', 'tipos', 'clases',
            'regiones', 'zonas', 'areas', 'territorios', 'ciudades', 'paises', 'estados',
            'tiendas', 'sucursales', 'local', 'locales', 'clientes', 'usuarios',
            'productos', 'articulos', 'items', 'customer', 'customers', 'stores'
        }
        
        self.metricas = {
            # Básicos originales
            'ventas', 'venta', 'ingresos', 'revenue', 'facturacion', 'ganancias', 'beneficio',
            'cantidad', 'unidades', 'volumen', 'precio', 'costo', 'inventory', 'inventario',
            'week', 'semana', 'denominador', 'numerador',
            
            # 🔥 VARIACIONES BÁSICAS para fallback
            'sales', 'sale', 'profit', 'profits', 'benefits', 'benefits', 'quantity',
            'quantities', 'units', 'volumes', 'price', 'prices', 'cost', 'costs',
            'weeks', 'semanas', 'gain', 'gains', 'earning', 'earnings'
        }
        
        self.operaciones = {
            'mas': OperationType.MAXIMUM, 'mayor': OperationType.MAXIMUM, 'maximo': OperationType.MAXIMUM,
            'menos': OperationType.MINIMUM, 'menor': OperationType.MINIMUM, 'minimo': OperationType.MINIMUM,
            'suma': OperationType.SUM, 'total': OperationType.SUM,
            'promedio': OperationType.AVERAGE, 'media': OperationType.AVERAGE,
            'contar': OperationType.COUNT, 'count': OperationType.COUNT
        }
        
        print(f"   📂 Dimensiones fallback: {len(self.dimensiones)}")
        print(f"   📊 Métricas fallback: {len(self.metricas)}")
        print(f"   ⚡ Operaciones fallback: {len(self.operaciones)}")
    
    def reload_from_json(self) -> bool:
        """Recarga todos los diccionarios desde JSON"""
        print(f"🔄 RECARGANDO DICCIONARIOS CON PALABRAS ANCLA...")
        return self._load_all_dictionaries()
    
    def get_statistics(self) -> Dict:
        """Obtiene estadísticas de los diccionarios cargados"""
        return {
            'total_dimensiones': len(self.dimensiones),
            'total_metricas': len(self.metricas),
            'total_operaciones': len(self.operaciones),
            'total_columnas': len(self.columnas_conocidas),
            'total_valores': len(self.valores_comunes),
            'total_conectores': len(self.conectores),
            'total_numeros_palabra': len(self.numeros_palabras),
            'total_correcciones': len(self.correcciones_tipograficas),
            'dimension_anchors_count': len(self.dimension_anchors),
            'metric_anchors_count': len(self.metric_anchors),
            'mode': 'JSON_Anchors',
            'json_path': str(self.json_path.absolute()),
            'last_loaded': self.last_loaded.isoformat() if self.last_loaded else None,
            'load_successful': self.load_successful
        }
    
    def get_mode_info(self) -> Dict:
        """Obtiene información sobre el modo actual"""
        return {
            'mode': 'JSON_Anchors',
            'json_path': str(self.json_path.absolute()),
            'last_loaded': self.last_loaded.isoformat() if self.last_loaded else None,
            'load_successful': self.load_successful,
            'can_reload': True,
            'structure_verified': self._verify_json_structure(),
            'uses_anchor_system': True
        }
    
    # 🚨 NUEVO MÉTODO: Para obtener palabra ancla (útil para debugging)
    def get_anchor_for_term(self, term: str) -> Optional[str]:
        """Encuentra la palabra ancla para un término dado"""
        term_lower = term.lower()
        
        # Buscar en dimension anchors
        for anchor, synonyms in self.dimension_anchors.items():
            if term_lower == anchor.lower() or term_lower in [s.lower() for s in synonyms]:
                return anchor
        
        # Buscar en metric anchors
        for anchor, synonyms in self.metric_anchors.items():
            if term_lower == anchor.lower() or term_lower in [s.lower() for s in synonyms]:
                return anchor
        
        return None
    
    
    
    # ===============================================
    # === MÉTODOS DE COMPATIBILIDAD CON CÓDIGO ORIGINAL ===
    # ===============================================
    
    def normalize_compound_phrases(self, text: str) -> str:
        """Normaliza frases compuestas - MANTIENE COMPATIBILIDAD"""
        # Si no hay frases compuestas cargadas, devolver texto original
        if not hasattr(self, 'frases_compuestas') or not self.frases_compuestas:
            return text.lower()
        
        text_lower = text.lower()
        
        # Ordenar por longitud descendente para evitar reemplazos parciales
        sorted_phrases = sorted(self.frases_compuestas.keys(), key=len, reverse=True)
        
        for phrase in sorted_phrases:
            if phrase in text_lower:
                normalized = self.frases_compuestas[phrase]
                text_lower = text_lower.replace(phrase, normalized)
        
        return text_lower
    
    def get_component_type(self, word: str) -> ComponentType:
        """Determina el tipo de componente - MANTIENE COMPATIBILIDAD TOTAL"""
        
        # Letras individuales mayúsculas son VALORES
        if len(word) == 1 and word.isupper() and word.isalpha():
            return ComponentType.VALUE
        
        word_lower = word.lower()
        
        # 🚨 BÚSQUEDA EN SETS EXPANDIDOS (funciona igual que antes)
        if word_lower in self.dimensiones:
            return ComponentType.DIMENSION
        elif word_lower in self.operaciones:
            return ComponentType.OPERATION
        elif word_lower in self.metricas:
            return ComponentType.METRIC
        elif word_lower in self.conectores:
            return ComponentType.CONNECTOR
        elif word_lower in self.indicadores_temporales or word_lower in self.unidades_tiempo:
            return ComponentType.TEMPORAL
        elif word.isdigit() or word_lower in self.numeros_palabras:
            return ComponentType.VALUE
        else:
            return ComponentType.UNKNOWN
    
    def get_operation_type(self, word: str) -> Optional[OperationType]:
        """Obtiene el tipo de operación para una palabra - MANTIENE COMPATIBILIDAD"""
        return self.operaciones.get(word.lower(), None)
    
    def correct_typo(self, word: str) -> str:
        """Corrige errores tipográficos comunes - MANTIENE COMPATIBILIDAD"""
        return self.correcciones_tipograficas.get(word.lower(), word)



# =================================================
# ========== FUNCIONES DE COMPATIBILIDAD ==========
# =================================================

# Instancia global del cargador JSON
_DICT_LOADER = None

def _get_dict_loader() -> JSONDictionaryLoader:
    """Obtiene la instancia global del cargador de diccionarios"""
    global _DICT_LOADER
    if _DICT_LOADER is None:
        _DICT_LOADER = JSONDictionaryLoader()
    return _DICT_LOADER

def get_dictionaries() -> JSONDictionaryLoader:
    """Función para obtener la instancia de diccionarios (compatibilidad)"""
    return _get_dict_loader()

def get_dimensions() -> Set[str]:
    """Obtiene el conjunto de dimensiones (compatibilidad)"""
    return _get_dict_loader().dimensiones

def get_operations() -> Dict[str, OperationType]:
    """Obtiene el diccionario de operaciones (compatibilidad)"""
    return _get_dict_loader().operaciones

def get_metrics() -> Set[str]:  
    """Obtiene el conjunto de métricas (compatibilidad)"""
    return _get_dict_loader().metricas

def get_current_mode() -> Dict:
    """Obtiene información del modo actual (compatibilidad)"""
    return _get_dict_loader().get_mode_info()

def reload_dictionaries_from_json() -> bool:
    """Recarga diccionarios desde JSON (nueva función)"""
    global _DICT_LOADER
    if _DICT_LOADER:
        return _DICT_LOADER.reload_from_json()
    else:
        _DICT_LOADER = JSONDictionaryLoader()
        return _DICT_LOADER.load_successful



# ===============================================
# ========== DATACLASSES ORIGINALES =============
# ===============================================

@dataclass
class ColumnClassification:
    """Clasificación de una columna"""
    name: str
    original_name: str
    type: str  # 'dimension', 'metric', 'identifier', 'temporal', 'other'
    data_type: str  # 'numeric', 'categorical', 'text', 'datetime'
    mapped_term: Optional[str] = None
    confidence: float = 0.0
    sample_values: List = None
    unique_count: int = 0
    detection_method: str = "unknown"

@dataclass
class TemporalValue:
    """Información de un valor temporal detectado"""
    original_value: str
    column_name: str
    column_type: str
    variants: List[str]
    confidence: float = 1.0



class MySQLIntegration:
    """
    🗄️ INTEGRACIÓN MYSQL PARA TABLEANALYZER
    Maneja la conexión y subida automática de tablas normalizadas
    """
    
    def __init__(self):
        # Configuración por defecto - MODIFICA ESTOS VALORES
        self.config = {
            "host": os.getenv("MYSQL_HOST", "localhost"),
            "port": int(os.getenv("MYSQL_PORT", "3306")),
            "database": os.getenv("MYSQL_DATABASE", "datos_imperiales"),
            "user": os.getenv("MYSQL_USER", "root"),
            "password": os.getenv("MYSQL_PASSWORD", "zeldapolis0"),
            "charset": "utf8mb4",
            "auth_plugin": "mysql_native_password", 
            "autocommit": True
        }
        
        self.connection_available = False
        self._test_connection()
    
    def _test_connection(self):
        """Probar conexión MySQL"""
        try:
            conn = mysql.connector.connect(**self.config)
            conn.close()
            self.connection_available = True
            print(f"✅ MySQL disponible: {self.config['host']}/{self.config['database']}")
        except Exception as e:
            self.connection_available = False
            print(f"⚠️ MySQL no disponible: {e}")
            print(f"⚠️ Las tablas no se subirán automáticamente")
    
    def generate_table_name(self, file_path: str) -> str:
        """Generar nombre único para tabla MySQL"""
        file_name = Path(file_path).stem
        
        # Limpiar nombre
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
        clean_name = re.sub(r'_+', '_', clean_name)
        clean_name = clean_name.strip('_')[:50]
        
        # Timestamp + hash para unicidad
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path_hash = hashlib.md5(file_path.encode()).hexdigest()[:6]
        
        return f"datos_{clean_name}_{timestamp}_{path_hash}"
    
    
    def upload_table(self, dataframe: pd.DataFrame, table_name: str, 
                    file_path: str, analysis_info: Dict) -> Dict:
        """Subir tabla a MySQL con metadata completa"""
        
        if not self.connection_available:
            return {
                'success': False,
                'error': 'MySQL no está disponible'
            }
        
        try:
            print(f"Iniciando subida de tabla: {table_name}")
            print(f"Base de datos: {self.config['database']}")
            print(f"Filas: {len(dataframe)}, Columnas: {len(dataframe.columns)}")
            
            # Optimizaciones MySQL antes de cargar
            temp_conn = mysql.connector.connect(**self.config)
            cursor = temp_conn.cursor()
            
            # Optimizar MySQL para bulk loading
            cursor.execute("SET SESSION foreign_key_checks = 0")
            cursor.execute("SET SESSION unique_checks = 0") 
            cursor.execute("SET SESSION autocommit = 0")
            cursor.execute("SET SESSION bulk_insert_buffer_size = 256*1024*1024")
            
            temp_conn.commit()
            cursor.close()
            temp_conn.close()
            print("Optimizaciones MySQL aplicadas")
            
            # DataFrame preparado
            df_clean = self._prepare_dataframe_for_mysql(dataframe)
            print(f"DataFrame preparado: {len(df_clean)} filas, {len(df_clean.columns)} columnas")
            
            # Conexión optimizada
            connection_string = (
                f"mysql+mysqlconnector://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
                f"?charset={self.config['charset']}&autocommit=false"
            )
            
            engine = create_engine(connection_string, pool_pre_ping=True)
            
            # Carga optimizada
            print("Subiendo tabla a MySQL...")
            df_clean.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False,
                method=None,
                chunksize=50000
            )
            print(f"Tabla {table_name} subida exitosamente a MySQL")
            
            # CRÍTICO: Guardar metadata después de subir exitosamente
            print("Guardando metadata...")
            self._save_metadata(
                table_name=table_name,
                file_path=file_path,
                analysis_info=analysis_info,
                rows=len(df_clean),
                cols=len(df_clean.columns)
            )
            print("Metadata guardada exitosamente")
            
            return {
                'success': True,
                'table_name': table_name,
                'database': self.config['database'],
                'host': self.config['host'],
                'rows': len(df_clean),
                'columns': len(df_clean.columns),
                'message': f'Tabla {table_name} subida exitosamente con metadata'
            }
            
        except Exception as e:
            print(f"ERROR subiendo tabla {table_name}: {str(e)}")
            import traceback
            print(f"Traceback completo: {traceback.format_exc()}")
            
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'traceback': traceback.format_exc()
            }
            
        
    def _prepare_dataframe_for_mysql(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Preparar DataFrame para MySQL"""
        df_clean = dataframe.copy()
        
        # Limpiar nombres de columnas
        column_mapping = {}
        for col in df_clean.columns:
            clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', str(col))
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')[:64]
            
            if clean_col and clean_col[0].isdigit():
                clean_col = f"col_{clean_col}"
            
            if clean_col != col:
                column_mapping[col] = clean_col
        
        if column_mapping:
            df_clean = df_clean.rename(columns=column_mapping)
            print(f"🔧 Columnas renombradas: {len(column_mapping)}")
        
        return df_clean
    
    def _save_metadata(self, table_name: str, file_path: str, analysis_info: Dict, rows: int, cols: int):
        """Guardar metadata básica con logging detallado"""
        try:
            print(f"Iniciando guardado de metadata para tabla: {table_name}")
            print(f"Conectando a: {self.config['host']}@{self.config['database']}")
            
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor()
            
            # Crear tabla de metadata si no existe
            print("Verificando/creando tabla tablas_metadata...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tablas_metadata (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    table_name VARCHAR(255) UNIQUE,
                    original_filename VARCHAR(500),
                    file_path TEXT,
                    total_rows BIGINT,
                    total_columns INT,
                    dimensions_count INT DEFAULT 0,
                    metrics_count INT DEFAULT 0,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_table_name (table_name),
                    INDEX idx_analyzed_at (analyzed_at)
                )
            """)
            print("Tabla tablas_metadata verificada/creada")
            
            # Extraer información del análisis
            file_info = analysis_info.get('file_info', {})
            summary = analysis_info.get('summary', {})
            
            original_filename = file_info.get('name', Path(file_path).name if file_path else 'unknown')
            dimensions_count = summary.get('dimensions_count', 0)
            metrics_count = summary.get('metrics_count', 0)
            
            print(f"Datos a insertar:")
            print(f"  - table_name: {table_name}")
            print(f"  - original_filename: {original_filename}")
            print(f"  - file_path: {file_path}")
            print(f"  - total_rows: {rows}")
            print(f"  - total_columns: {cols}")
            print(f"  - dimensions_count: {dimensions_count}")
            print(f"  - metrics_count: {metrics_count}")
            
            # Insertar metadata
            cursor.execute("""
                INSERT INTO tablas_metadata 
                (table_name, original_filename, file_path, total_rows, total_columns, dimensions_count, metrics_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                total_rows = VALUES(total_rows),
                total_columns = VALUES(total_columns),
                dimensions_count = VALUES(dimensions_count),
                metrics_count = VALUES(metrics_count),
                analyzed_at = CURRENT_TIMESTAMP
            """, (
                table_name,
                original_filename,
                file_path or '',
                rows,
                cols,
                dimensions_count,
                metrics_count
            ))
            
            # Verificar que se insertó correctamente
            affected_rows = cursor.rowcount
            print(f"Filas afectadas en tablas_metadata: {affected_rows}")
            
            conn.commit()
            
            # Verificación final
            cursor.execute("SELECT * FROM tablas_metadata WHERE table_name = %s", (table_name,))
            result = cursor.fetchone()
            if result:
                print(f"Verificación exitosa: metadata existe para {table_name}")
            else:
                print(f"WARNING: No se encontró metadata después de insertar para {table_name}")
            
            cursor.close()
            conn.close()
            
            print(f"Metadata guardada exitosamente para tabla: {table_name}")
            
        except Exception as e:
            print(f"ERROR CRÍTICO guardando metadata para {table_name}: {str(e)}")
            import traceback
            print(f"Traceback completo: {traceback.format_exc()}")
            raise e  # Re-lanzar para que se vea en upload_table()
        
        
    def list_tables(self) -> List[Dict]:
        """Listar tablas subidas"""
        if not self.connection_available:
            return []
        
        try:
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT table_name, original_filename, total_rows, 
                    total_columns, dimensions_count, metrics_count, analyzed_at
                FROM tablas_metadata 
                ORDER BY analyzed_at DESC
            """)
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"❌ Error listando tablas: {e}")
            return []


    def upload_via_load_data(self, csv_file_path: str, table_name: str):
        """Método ultra-rápido usando LOAD DATA INFILE"""
        try:
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor()
            
            # Crear tabla basada en el CSV
            cursor.execute(f"""
                LOAD DATA LOCAL INFILE '{csv_file_path}'
                INTO TABLE {table_name}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error con LOAD DATA INFILE: {e}")
            return False


# =============================================
# ========== ANALIZADOR DE TABLAS =============
# =============================================


class TableAnalyzer:
    """
    🔥 ANALIZADOR COMPATIBLE CON PALABRAS ANCLA - Mantiene toda la funcionalidad
    """
    
    def __init__(self, json_path: str = DEFAULT_JSON_DICT_PATH):
        """Inicializar analizador compatible con palabras ancla"""
        
        print("🎯 INICIALIZANDO ANALIZADOR COMPATIBLE CON PALABRAS ANCLA")
        print("="*70)
        
        # 🔥 NUEVA CONFIGURACIÓN: Ruta JSON configurable
        self.json_path = json_path
        
        # ✅ CARGA DIRECTA DESDE JSON CON PALABRAS ANCLA
        try:
            # Obtener instancia de diccionarios (carga automática desde JSON)
            self.dictionaries = get_dictionaries()
            self.dimensions_dict = get_dimensions()
            self.metrics_dict = get_metrics()
            self.operations_dict = get_operations()
            
            # Verificar carga exitosa
            mode_info = get_current_mode()
            print(f"✅ Diccionarios con palabras ancla cargados:")
            print(f"📁 Ruta: {mode_info['json_path']}")
            print(f"🕐 Última carga: {mode_info['last_loaded']}")
            print(f"✅ Carga exitosa: {mode_info['load_successful']}")
            print(f"🔥 Sistema de anclas: {mode_info['uses_anchor_system']}")
            
            # Mostrar estadísticas de carga
            stats = self.dictionaries.get_statistics()
            print(f"📊 Estadísticas cargadas:")
            print(f"   📂 Dimensiones (expandidas): {stats['total_dimensiones']}")
            print(f"   📊 Métricas (expandidas): {stats['total_metricas']}")
            print(f"   ⚡ Operaciones: {stats['total_operaciones']}")
            print(f"   📍 Anclas de dimensión: {stats['dimension_anchors_count']}")
            print(f"   📍 Anclas de métrica: {stats['metric_anchors_count']}")
            
            # 🔥 DEMO: Mostrar algunas expansiones
            if stats['dimension_anchors_count'] > 0:
                print(f"\n🔥 EJEMPLO DE EXPANSIONES CARGADAS:")
                sample_anchors = list(self.dictionaries.dimension_anchors.items())[:2]
                for anchor, synonyms in sample_anchors:
                    print(f"   📂 '{anchor}': {len(synonyms)} variaciones → {synonyms[:5]}...")
            
        except Exception as e:
            print(f"⚠️ Error cargando diccionarios con palabras ancla: {e}")
            print(f"⚠️ Trabajando sin diccionario externo")
            self.dictionaries = None
            self.dimensions_dict = set()
            self.metrics_dict = set()
            self.operations_dict = {}
        
        # ✅ TU ESTRUCTURA ORIGINAL (SIN CAMBIOS)
        self.current_table = None
        self.current_file_path = None
        self.classified_columns = {}
        
        # 🔥 SISTEMA DE DICCIONARIO TEMPORAL (SIN CAMBIOS)
        self.temporal_dictionary = {}
        self.temporal_values_by_column = {}
        self.temporal_generation_stats = {}
        self.temporal_enabled = False
        
        # Configuración de ruta fija para temporal
        self.temporal_save_path = DEFAULT_TEMPORAL_PATH
        self.auto_save_temporal = True
        self.create_backups = False
        
        # 🔥 CONFIGURACIÓN DE OPTIMIZACIÓN (SIN CAMBIOS)
        self.skip_single_value_columns = True
        self.skip_binary_values = True
        self.min_unique_values = 1
        
        self._ensure_temporal_directories()
        print("🔥 Analizador compatible con palabras ancla listo")
        
        # Integración MySQL (opcional)
        self.mysql_integration = MySQLIntegration()
        print("🔥 Analizador con opción MySQL listo")
        
        # NUEVA: Integración ClickHouse (adicional)
        self.clickhouse_integration = ClickHouseIntegration()
        
        print("🔥 Analizador con opciones MySQL + ClickHouse listo")


    def get_dictionary_info(self) -> Dict:
        """🔥 Obtener información detallada del sistema de diccionarios con palabras ancla"""
        
        if not self.dictionaries:
            return {
                'status': 'no_dictionary',
                'mode': 'none',
                'message': 'No hay diccionarios cargados'
            }
        
        mode_info = get_current_mode()
        stats = self.dictionaries.get_statistics()
        
        return {
            'status': 'active',
            'mode': 'JSON_Anchors_Compatible',
            'json_path': mode_info['json_path'],
            'last_loaded': mode_info['last_loaded'],
            'load_successful': mode_info['load_successful'],
            'uses_anchor_system': mode_info['uses_anchor_system'],
            'can_reload': True,
            'statistics': stats,
            'anchor_info': {
                'dimension_anchors': len(self.dictionaries.dimension_anchors),
                'metric_anchors': len(self.dictionaries.metric_anchors),
                'total_expansions': stats['total_dimensiones'] + stats['total_metricas']
            }
        }

    def reload_dictionaries(self) -> bool:
        """🔥 Recargar diccionarios con palabras ancla"""
        
        print("🔄 Recargando diccionarios con palabras ancla...")
        
        try:
            success = reload_dictionaries_from_json()
            
            if success:
                # Actualizar referencias locales
                self.dictionaries = get_dictionaries()
                self.dimensions_dict = get_dimensions()
                self.metrics_dict = get_metrics()
                self.operations_dict = get_operations()
                
                print("✅ Diccionarios con palabras ancla recargados exitosamente")
                
                # Mostrar estadísticas actualizadas
                stats = self.dictionaries.get_statistics()
                print(f"📊 Nuevas estadísticas:")
                print(f"   📂 Dimensiones (expandidas): {stats['total_dimensiones']}")
                print(f"   📊 Métricas (expandidas): {stats['total_metricas']}")
                print(f"   📍 Anclas de dimensión: {stats['dimension_anchors_count']}")
                print(f"   📍 Anclas de métrica: {stats['metric_anchors_count']}")
                
                return True
            else:
                print("❌ Error recargando diccionarios")
                return False
        
        except Exception as e:
            print(f"❌ Error durante recarga: {e}")
            return False

    # 🚨 NUEVO MÉTODO: Para debugging y demostración
    def test_anchor_recognition(self, test_terms: List[str] = None):
        """Prueba el reconocimiento de términos con palabras ancla"""
        
        if not self.dictionaries:
            print("❌ No hay diccionarios cargados")
            return
        
        if test_terms is None:
            # Términos de prueba con variaciones
            test_terms = [
                'tienda', 'sucursal', 'outlet', 'store', 'local',
                'ventas', 'revenue', 'ganancias', 'profit', 'sales',
                'cliente', 'customer', 'usuario', 'account',
                'inventario', 'inventory', 'stock', 'existencias',
                'ejecutivo', 'vendedor', 'employee', 'staff'
            ]
        
        print(f"\n🧪 PROBANDO RECONOCIMIENTO CON PALABRAS ANCLA:")
        print("="*60)
        
        dimensions_found = []
        metrics_found = []
        unknown_found = []
        
        for term in test_terms:
            component_type = self.dictionaries.get_component_type(term)
            anchor = self.dictionaries.get_anchor_for_term(term)
            
            status_icon = "✅" if component_type != ComponentType.UNKNOWN else "❌"
            anchor_info = f"→ {anchor}" if anchor else ""
            
            if component_type == ComponentType.DIMENSION:
                dimensions_found.append(term)
                print(f"   {status_icon} 📂 DIMENSIÓN: '{term}' {anchor_info}")
            elif component_type == ComponentType.METRIC:
                metrics_found.append(term)
                print(f"   {status_icon} 📊 MÉTRICA: '{term}' {anchor_info}")
            else:
                unknown_found.append(term)
                print(f"   {status_icon} ❓ NO RECONOCIDO: '{term}'")
        
        print(f"\n📊 RESUMEN DE PRUEBA:")
        print(f"   📂 Dimensiones reconocidas: {len(dimensions_found)}/{len(test_terms)}")
        print(f"   📊 Métricas reconocidas: {len(metrics_found)}/{len(test_terms)}")
        print(f"   ❓ No reconocidos: {len(unknown_found)}/{len(test_terms)}")
        
        success_rate = ((len(dimensions_found) + len(metrics_found)) / len(test_terms)) * 100
        print(f"   🎯 Tasa de éxito: {success_rate:.1f}%")
        
        return {
            'dimensions_found': dimensions_found,
            'metrics_found': metrics_found,
            'unknown_found': unknown_found,
            'success_rate': success_rate
        }

    def configure_temporal_optimization(self, skip_single_value: bool = True, skip_binary_values: bool = True, min_unique: int = 2):
        """🔥 CONFIGURAR optimización del diccionario temporal (SIN CAMBIOS)"""
        
        self.skip_single_value_columns = skip_single_value
        self.skip_binary_values = skip_binary_values
        self.min_unique_values = min_unique
        
        print(f"✅ Configuración de optimización:")
        print(f"   🚫 Omitir columnas con 1 valor: {skip_single_value}")
        print(f"   🚫 Omitir valores binarios individuales (Y, N, 1, 0): {skip_binary_values}")
        print(f"   📏 Mínimo valores únicos: {min_unique}")

    def _ensure_temporal_directories(self):
        """Asegurar que existan los directorios necesarios (SIN CAMBIOS)"""
        try:
            os.makedirs(TEMPORAL_DICT_DIR, exist_ok=True)
            if self.create_backups:
                os.makedirs(BACKUP_TEMPORAL_PATH, exist_ok=True)
            print(f"📁 Directorios temporales verificados")
        except Exception as e:
            print(f"⚠️ Error creando directorios: {e}")
            self.temporal_save_path = TEMPORAL_DICT_FILE

    def _is_binary_simple_value(self, value: str) -> bool:
        """🔥 DETECTAR si un VALOR individual es binario simple (SIN CAMBIOS)"""
        
        if not value:
            return False
        
        value_clean = str(value).strip()
        value_lower = value_clean.lower()
        
        simple_binary_values = {
            'y', 'n', 'yes', 'no', 'si', 'sí', 'no',
            '0', '1', 
            'true', 'false', 'verdadero', 'falso',
            'on', 'off', 'ok', 'error',
            's', 'f', 't', 'a', 'i'
        }
        
        if value_lower in simple_binary_values:
            return True
        
        if len(value_clean) == 1 and (value_clean.isalnum()):
            return True
        
        return False

    # ===============================================================
    # === TU LÓGICA ORIGINAL DE CLASIFICACIÓN (COMPATIBLE CON PALABRAS ANCLA) ===
    # ===============================================================

    def diagnose_file(self, file_path: str) -> Dict:
        """Diagnosticar problemas con el archivo antes de cargarlo (SIN CAMBIOS)"""
        
        print(f"\n🔍 DIAGNOSTICANDO ARCHIVO")
        print("="*40)
        
        diagnosis = {
            'exists': False,
            'readable': False,
            'size': 0,
            'extension': '',
            'encoding_detected': '',
            'separators_found': [],
            'errors': [],
            'warnings': []
        }
        
        if not os.path.exists(file_path):
            diagnosis['errors'].append(f"❌ Archivo no existe: {file_path}")
            return diagnosis
        
        diagnosis['exists'] = True
        diagnosis['size'] = os.path.getsize(file_path)
        diagnosis['extension'] = Path(file_path).suffix.lower()
        
        print(f"✅ Archivo existe: {Path(file_path).name}")
        print(f"📏 Tamaño: {diagnosis['size']:,} bytes")
        print(f"📄 Extensión: {diagnosis['extension']}")
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(min(10000, diagnosis['size']))
            diagnosis['readable'] = True
        except Exception as e:
            diagnosis['errors'].append(f"❌ No se puede leer el archivo: {e}")
            return diagnosis
        
        try:
            encoding_result = chardet.detect(raw_data)
            diagnosis['encoding_detected'] = encoding_result['encoding']
            print(f"🔤 Encoding detectado: {diagnosis['encoding_detected']} (confianza: {encoding_result['confidence']:.2f})")
        except Exception as e:
            diagnosis['warnings'].append(f"⚠️ No se pudo detectar encoding: {e}")
        
        if diagnosis['extension'] == '.csv':
            try:
                with open(file_path, 'r', encoding=diagnosis['encoding_detected'] or 'utf-8', errors='ignore') as f:
                    sample_lines = [f.readline() for _ in range(5)]
                
                separators = [',', ';', '\t', '|']
                for sep in separators:
                    for line in sample_lines:
                        if line.count(sep) > 0:
                            if sep not in diagnosis['separators_found']:
                                diagnosis['separators_found'].append(sep)
                
                print(f"🔍 Separadores encontrados: {diagnosis['separators_found']}")
                
                print(f"🔍 Primeras líneas:")
                for i, line in enumerate(sample_lines[:3], 1):
                    print(f"   {i}: {line.strip()[:100]}...")
                
            except Exception as e:
                diagnosis['warnings'].append(f"⚠️ Error analizando CSV: {e}")
        
        return diagnosis

    def _load_file_robust(self, file_path: str) -> Optional[pd.DataFrame]:
        """Cargar archivo con diagnóstico mejorado (SIN CAMBIOS)"""
        
        print(f"\n📥 CARGANDO ARCHIVO...")
        
        diagnosis = self.diagnose_file(file_path)
        
        if diagnosis['errors']:
            for error in diagnosis['errors']:
                print(error)
            return None
        
        file_ext = diagnosis['extension']
        
        try:
            if file_ext == '.csv':
                return self._load_csv_robust(file_path, diagnosis)
            elif file_ext in ['.xlsx', '.xls']:
                return self._load_excel_robust(file_path)
            else:
                print(f"❌ Formato no soportado: {file_ext}")
                return None
        
        except Exception as e:
            print(f"❌ Error general cargando archivo: {e}")
            return None

    def _load_csv_robust(self, file_path: str, diagnosis: Dict) -> Optional[pd.DataFrame]:
        """Cargar CSV con múltiples estrategias (SIN CAMBIOS)"""
        
        print(f"📊 Intentando cargar CSV...")
        
        encodings = [diagnosis['encoding_detected'], 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        encodings = [enc for enc in encodings if enc]
        
        separators = diagnosis['separators_found'] if diagnosis['separators_found'] else [',', ';', '\t', '|']
        
        attempts = []
        
        for encoding in encodings:
            for sep in separators:
                try:
                    print(f"🔄 Probando: encoding={encoding}, sep='{sep}'")
                    
                    try:
                        df = pd.read_csv(
                            file_path, 
                            encoding=encoding, 
                            sep=sep,
                            low_memory=False,
                            on_bad_lines='skip'
                        )
                    except TypeError:
                        df = pd.read_csv(
                            file_path, 
                            encoding=encoding, 
                            sep=sep,
                            low_memory=False,
                            error_bad_lines=False
                        )
                    
                    if len(df.columns) > 1 and len(df) > 0:
                        print(f"✅ ¡Éxito! Cargado con encoding={encoding}, sep='{sep}'")
                        print(f"📊 Resultado: {len(df)} filas × {len(df.columns)} columnas")
                        print(f"📋 Columnas: {list(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
                        return df
                    else:
                        attempts.append(f"encoding={encoding}, sep='{sep}' → DataFrame vacío o una sola columna")
                
                except Exception as e:
                    attempts.append(f"encoding={encoding}, sep='{sep}' → {str(e)[:50]}...")
        
        print(f"❌ No se pudo cargar el CSV. Intentos realizados:")
        for attempt in attempts:
            print(f"   • {attempt}")
        
        return None

    def _load_excel_robust(self, file_path: str) -> Optional[pd.DataFrame]:
        """Cargar Excel con diagnóstico (SIN CAMBIOS)"""
        
        print(f"📊 Cargando archivo Excel...")
        
        try:
            df = pd.read_excel(file_path, engine='openpyxl' if file_path.endswith('.xlsx') else 'xlrd')
            
            print(f"✅ Excel cargado exitosamente")
            print(f"📊 Resultado: {len(df)} filas × {len(df.columns)} columnas")
            
            return df
            
        except Exception as e:
            print(f"❌ Error cargando Excel: {e}")
            return None


    def analyze_table(self, file_path: str) -> Dict:
        """🔥 TU ANÁLISIS ORIGINAL CON PALABRAS ANCLA + GENERACIÓN TEMPORAL"""
        
        print(f"\n📊 ANALIZANDO TABLA - CON PALABRAS ANCLA + TEMPORAL")
        print("="*70)
        print(f"📂 Archivo: {file_path}")
        
        # Mostrar información del sistema de diccionarios
        dict_info = self.get_dictionary_info()
        print(f"🔧 Sistema de diccionarios: {dict_info['mode']} ({dict_info['status']})")
        print(f"📁 Carga desde: {dict_info['json_path']}")
        print(f"🔥 Sistema de anclas: {dict_info['uses_anchor_system']}")
        
        if not self._validate_file(file_path):
            return {'success': False, 'error': 'Archivo inválido'}
        
        df = self._load_file_robust(file_path)
        if df is None:
            return {'success': False, 'error': 'No se pudo cargar el archivo'}
        
        print(f"✅ Archivo cargado: {len(df)} filas × {len(df.columns)} columnas")
        
        self.current_table = df
        self.current_file_path = file_path
        
        # ✅ TU LÓGICA ORIGINAL DE CLASIFICACIÓN (AHORA CON PALABRAS ANCLA)
        print(f"\n🔍 CLASIFICANDO COLUMNAS USANDO PALABRAS ANCLA:")
        
        dimensions_found = {}
        metrics_found = {}
        other_columns = {}
        
        for col_name in df.columns:
            classification = self._classify_column_with_dictionaries(df, col_name)
            self.classified_columns[col_name] = classification
            
            confidence_icon = "🟢" if classification.confidence > 0.8 else "🟡" if classification.confidence > 0.5 else "🔴"
            
            # 🚨 AGREGAR INFO DE PALABRA ANCLA si está disponible
            anchor_info = ""
            if self.dictionaries and hasattr(self.dictionaries, 'get_anchor_for_term'):
                anchor = self.dictionaries.get_anchor_for_term(col_name)
                if anchor:
                    anchor_info = f" [ancla: {anchor}]"
            
            if classification.type == 'dimension':
                dimensions_found[col_name] = classification
                print(f"   {confidence_icon} 📂 DIMENSIÓN: {col_name} → {classification.mapped_term or 'sin mapeo'} ({classification.detection_method}){anchor_info}")
            elif classification.type == 'metric':
                metrics_found[col_name] = classification
                print(f"   {confidence_icon} 📊 MÉTRICA: {col_name} → {classification.mapped_term or 'sin mapeo'} ({classification.detection_method}){anchor_info}")
            else:
                other_columns[col_name] = classification
                print(f"   {confidence_icon} 📄 OTRA: {col_name} ({classification.type}) ({classification.detection_method})")
                
                
        # NORMALIZAR COLUMNAS A PALABRAS ANCLA
        print(f"\n🔄 APLICANDO NORMALIZACIÓN DE COLUMNAS...")
        normalization_success = self._normalize_dataframe_columns_to_anchors()
        
        # Actualizar las referencias después de la normalización
        if normalization_success:
            # Reconstruir dimensions_found y metrics_found con los nuevos nombres
            dimensions_found = {}
            metrics_found = {}
            other_columns = {}
            
            for col_name, classification in self.classified_columns.items():
                if classification.type == 'dimension':
                    dimensions_found[col_name] = classification
                elif classification.type == 'metric':
                    metrics_found[col_name] = classification
                else:
                    other_columns[col_name] = classification
            
            print(f"📋 Clasificaciones actualizadas después de normalización:")
            print(f"   📂 Dimensiones: {len(dimensions_found)}")
            print(f"   📊 Métricas: {len(metrics_found)}")    
        
        # 🔥 SISTEMA TEMPORAL (SIN CAMBIOS)
        temporal_success = False
        if dimensions_found:
            print(f"\n🔥 Generando diccionario temporal para {len(dimensions_found)} dimensiones detectadas...")
            temporal_success = self._generate_temporal_dictionary_integrated()
        else:
            print(f"\n⚠️ No se encontraron dimensiones - omitiendo diccionario temporal")
        
        result = {
            'success': True,
            'file_info': {
                'path': file_path,
                'name': Path(file_path).stem,
                'rows': len(df),
                'columns': len(df.columns)
            },
            'classification': {
                'dimensions': dimensions_found,
                'metrics': metrics_found,
                'other': other_columns
            },
            'summary': {
                'total_columns': len(df.columns),
                'dimensions_count': len(dimensions_found),
                'metrics_count': len(metrics_found),
                'mapped_count': len([c for c in self.classified_columns.values() if c.mapped_term]),
                'unmapped_count': len([c for c in self.classified_columns.values() if not c.mapped_term])
            },
            'temporal_dictionary': {
                'generated': temporal_success,
                'enabled': self.temporal_enabled,
                'entries_count': len(self.temporal_dictionary),
                'stats': self.temporal_generation_stats
            },
            'dictionary_system': dict_info  # 🔥 INFO del sistema con palabras ancla
        }
        
        self._show_analysis_summary(result)
        
        return result
    

    def _classify_column_with_dictionaries(self, df: pd.DataFrame, col_name: str) -> ColumnClassification:
        """✅ TU LÓGICA ORIGINAL DE CLASIFICACIÓN (AHORA CON PALABRAS ANCLA EXPANDIDAS)"""
        
        series = df[col_name]
        
        classification = ColumnClassification(
            name=col_name.lower().replace(' ', '_'),
            original_name=col_name,
            type='other',
            data_type=self._detect_data_type(series),
            sample_values=series.dropna().head(5).tolist(),
            unique_count=series.nunique(),
            detection_method="unknown"
        )
        
        # ✅ ESTRATEGIA 1: Método de normalización (COMPATIBLE)
        if self.dictionaries and hasattr(self.dictionaries, 'normalize_compound_phrases'):
            result = self._detect_with_compound_phrases(col_name)
            if result['detected']:
                classification.type = result['type']
                classification.mapped_term = result['concept']
                classification.confidence = result['confidence']
                classification.detection_method = "compound_phrases"
                return classification
        
        # ✅ ESTRATEGIA 2: Método get_component_type (PALABRAS ANCLA EXPANDIDAS)
        if self.dictionaries and hasattr(self.dictionaries, 'get_component_type'):
            result = self._detect_with_component_type(col_name)
            if result['detected']:
                classification.type = result['type']
                classification.mapped_term = result['concept']
                classification.confidence = result['confidence']
                classification.detection_method = "component_type_anchors"
                return classification
        
        # ✅ ESTRATEGIA 3: Búsqueda directa (PALABRAS ANCLA EXPANDIDAS)
        result = self._detect_with_direct_search(col_name)
        if result['detected']:
            classification.type = result['type']
            classification.mapped_term = result['concept']
            classification.confidence = result['confidence']
            classification.detection_method = "direct_search_anchors"
            return classification
        
        # ✅ ESTRATEGIA 4: Búsqueda por similitud (PALABRAS ANCLA EXPANDIDAS)
        result = self._detect_with_similarity(col_name)
        if result['detected']:
            classification.type = result['type']
            classification.mapped_term = result['concept']
            classification.confidence = result['confidence']
            classification.detection_method = "similarity_anchors"
            return classification
        
        # ✅ ESTRATEGIA 5: Fallback por tipo de datos (SIN CAMBIOS)
        result = self._detect_by_data_type(series, col_name)
        classification.type = result['type']
        classification.mapped_term = result['concept']
        classification.confidence = result['confidence']
        classification.detection_method = "data_type_inference"
        
        return classification

    def _detect_with_compound_phrases(self, col_name: str) -> Dict:
        """✅ TU MÉTODO ORIGINAL (COMPATIBLE)"""
        
        try:
            normalized = self.dictionaries.normalize_compound_phrases(col_name)
            
            if hasattr(self.dictionaries, 'frases_compuestas') and normalized in self.dictionaries.frases_compuestas:
                concept_key = self.dictionaries.frases_compuestas[normalized]
                
                # Verificar en dimensiones
                if concept_key in self.dimensions_dict:
                    return {
                        'detected': True,
                        'type': 'dimension',
                        'concept': concept_key,
                        'confidence': 0.95
                    }
                # Verificar en métricas
                elif concept_key in self.metrics_dict:
                    return {
                        'detected': True,
                        'type': 'metric',
                        'concept': concept_key,
                        'confidence': 0.95
                    }
            
            original_lower = col_name.lower()
            if hasattr(self.dictionaries, 'frases_compuestas') and original_lower in self.dictionaries.frases_compuestas:
                concept_key = self.dictionaries.frases_compuestas[original_lower]
                
                if concept_key in self.dimensions_dict:
                    return {
                        'detected': True,
                        'type': 'dimension',
                        'concept': concept_key,
                        'confidence': 0.90
                    }
                elif concept_key in self.metrics_dict:
                    return {
                        'detected': True,
                        'type': 'metric',
                        'concept': concept_key,
                        'confidence': 0.90
                    }
            
        except Exception as e:
            print(f"   Debug: Error en compound_phrases: {e}")
        
        return {'detected': False}

    def _detect_with_component_type(self, col_name: str) -> Dict:
        """✅ TU MÉTODO ORIGINAL (AHORA CON PALABRAS ANCLA EXPANDIDAS)"""
        
        try:
            component_type = self.dictionaries.get_component_type(col_name)
            
            # Verificar si es string o enum
            component_value = component_type.value if hasattr(component_type, 'value') else str(component_type)
            
            if component_value == 'dimension':
                # 🚨 NUEVO: Intentar obtener palabra ancla
                anchor = None
                if hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(col_name)
                
                return {
                    'detected': True,
                    'type': 'dimension',
                    'concept': anchor or col_name.lower(),
                    'confidence': 0.90  # 🔥 Mayor confianza con palabras ancla
                }
            elif component_value == 'metric':
                # 🚨 NUEVO: Intentar obtener palabra ancla
                anchor = None
                if hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(col_name)
                
                return {
                    'detected': True,
                    'type': 'metric',
                    'concept': anchor or col_name.lower(),
                    'confidence': 0.90  # 🔥 Mayor confianza con palabras ancla
                }
            
        except Exception as e:
            print(f"   Debug: Error en component_type: {e}")
        
        return {'detected': False}

    def _detect_with_direct_search(self, col_name: str) -> Dict:
        """✅ TU MÉTODO ORIGINAL (PALABRAS ANCLA EXPANDIDAS - maneja sets)"""
        
        col_lower = col_name.lower()
        col_normalized = col_lower.replace(' ', '_').replace('-', '_')
        
        # 🔥 BÚSQUEDA EXPANDIDA: Ahora incluye todas las variaciones de palabras ancla
        # Verificar en métricas (set expandido)
        if (col_lower in self.metrics_dict or col_normalized in self.metrics_dict):
            # 🚨 NUEVO: Intentar obtener palabra ancla
            anchor = None
            if hasattr(self.dictionaries, 'get_anchor_for_term'):
                anchor = self.dictionaries.get_anchor_for_term(col_name)
            
            return {
                'detected': True,
                'type': 'metric',
                'concept': anchor or col_lower,
                'confidence': 0.95  # 🔥 Alta confianza con búsqueda directa
            }
        
        # Verificar en dimensiones (set expandido)
        if (col_lower in self.dimensions_dict or col_normalized in self.dimensions_dict):
            # 🚨 NUEVO: Intentar obtener palabra ancla
            anchor = None
            if hasattr(self.dictionaries, 'get_anchor_for_term'):
                anchor = self.dictionaries.get_anchor_for_term(col_name)
            
            return {
                'detected': True,
                'type': 'dimension',
                'concept': anchor or col_lower,
                'confidence': 0.95  # 🔥 Alta confianza con búsqueda directa
            }
        
        return {'detected': False}

    def _detect_with_similarity(self, col_name: str) -> Dict:
        """✅ TU MÉTODO ORIGINAL (PALABRAS ANCLA EXPANDIDAS - maneja sets)"""
        
        col_lower = col_name.lower()
        best_match = {'detected': False, 'confidence': 0.0}
        
        # 🔥 BÚSQUEDA DE SIMILITUD EXPANDIDA: Ahora con muchas más variaciones
        # Manejar métricas (set expandido)
        for metric in self.metrics_dict:
            similarity = self._calculate_similarity(col_lower, str(metric).lower())
            if similarity > 0.6 and similarity > best_match['confidence']:
                # 🚨 NUEVO: Intentar obtener palabra ancla
                anchor = None
                if hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(str(metric))
                
                best_match = {
                    'detected': True,
                    'type': 'metric',
                    'concept': anchor or str(metric),
                    'confidence': similarity
                }
        
        # Manejar dimensiones (set expandido)
        for dimension in self.dimensions_dict:
            similarity = self._calculate_similarity(col_lower, str(dimension).lower())
            if similarity > 0.6 and similarity > best_match['confidence']:
                # 🚨 NUEVO: Intentar obtener palabra ancla
                anchor = None
                if hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(str(dimension))
                
                best_match = {
                    'detected': True,
                    'type': 'dimension',
                    'concept': anchor or str(dimension),
                    'confidence': similarity
                }
        
        return best_match

    def _detect_by_data_type(self, series: pd.Series, col_name: str) -> Dict:
        """✅ TU MÉTODO ORIGINAL (SIN CAMBIOS)"""
        
        data_type = self._detect_data_type(series)
        unique_ratio = series.nunique() / len(series) if len(series) > 0 else 0
        
        if data_type == 'numeric':
            return {
                'type': 'metric',
                'concept': 'numeric_value',
                'confidence': 0.4
            }
        
        elif data_type in ['categorical', 'text'] and unique_ratio < 0.1:
            return {
                'type': 'dimension',
                'concept': 'categorical_value',
                'confidence': 0.3
            }
        
        elif any(word in col_name.lower() for word in ['id', 'code', 'key']):
            if unique_ratio > 0.8:
                return {
                    'type': 'identifier',
                    'concept': 'identifier',
                    'confidence': 0.5
                }
            else:
                return {
                    'type': 'dimension',
                    'concept': 'categorical_identifier',
                    'confidence': 0.4
                }
        
        return {
            'type': 'other',
            'concept': 'unknown',
            'confidence': 0.0
        }

    def _detect_data_type(self, series: pd.Series) -> str:
        """✅ TU MÉTODO ORIGINAL (SIN CAMBIOS)"""
        
        if pd.api.types.is_numeric_dtype(series):
            return 'numeric'
        
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'datetime'
        
        if series.dtype == 'object':
            unique_ratio = series.nunique() / len(series) if len(series) > 0 else 0
            
            if unique_ratio < 0.1 or series.nunique() < 20:
                return 'categorical'
            else:
                return 'text'
        
        return 'other'

    def _calculate_similarity(self, col_name: str, dict_term: str) -> float:
        """✅ TU MÉTODO ORIGINAL (SIN CAMBIOS)"""
        
        if col_name == dict_term:
            return 1.0
        
        if dict_term in col_name or col_name in dict_term:
            return 0.8
        
        col_words = set(col_name.replace('_', ' ').replace('-', ' ').split())
        dict_words = set(dict_term.replace('_', ' ').replace('-', ' ').split())
        
        if col_words & dict_words:
            return len(col_words & dict_words) / len(col_words | dict_words)
        
        return 0.0


    def _normalize_dataframe_columns_to_anchors(self):
        """
        🔄 NORMALIZAR COLUMNAS DEL DATAFRAME A PALABRAS ANCLA
        Renombra las columnas del DataFrame para usar las palabras ancla correspondientes
        """
        if not self.dictionaries or not hasattr(self.dictionaries, 'get_anchor_for_term'):
            print("⚠️ No hay sistema de anclas disponible para normalización")
            return False
        
        print(f"\n🔄 NORMALIZANDO COLUMNAS DEL DATAFRAME A PALABRAS ANCLA")
        print("="*60)
        
        column_mapping = {}
        
        # Iterar sobre todas las columnas actuales
        for col_name in self.current_table.columns:
            anchor = self.dictionaries.get_anchor_for_term(col_name)
            
            if anchor and anchor != col_name:
                column_mapping[col_name] = anchor
                print(f"   🔄 '{col_name}' → '{anchor}'")
            else:
                print(f"   ✅ '{col_name}' → Sin cambio (ya es ancla o sin mapeo)")
        
        if column_mapping:
            # Renombrar columnas en el DataFrame
            self.current_table = self.current_table.rename(columns=column_mapping)
            
            # Actualizar también las clasificaciones internas
            updated_classifications = {}
            for old_name, new_name in column_mapping.items():
                if old_name in self.classified_columns:
                    classification = self.classified_columns[old_name]
                    # Actualizar el nombre en la clasificación
                    classification.name = new_name.lower().replace(' ', '_')
                    classification.original_name = new_name  # Ahora el "original" es el ancla
                    updated_classifications[new_name] = classification
                
            # Actualizar el diccionario de clasificaciones
            for old_name, new_name in column_mapping.items():
                if old_name in self.classified_columns:
                    del self.classified_columns[old_name]
            self.classified_columns.update(updated_classifications)
            
            print(f"\n✅ DATAFRAME NORMALIZADO:")
            print(f"   🔄 Columnas renombradas: {len(column_mapping)}")
            print(f"   📊 Nuevas columnas: {list(self.current_table.columns)}")
            
            return True
        else:
            print(f"   ℹ️ No se necesitaron cambios en las columnas")
            return False
        

    def _validate_file(self, file_path: str) -> bool:
        """✅ TU MÉTODO ORIGINAL (SIN CAMBIOS)"""
        
        if not os.path.exists(file_path):
            print(f"❌ Archivo no existe: {file_path}")
            return False
        
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext not in allowed_extensions:
            print(f"❌ Formato no soportado: {file_ext}")
            return False
        
        return True

    # ===============================================================
    # === 🔥 SISTEMA DE DICCIONARIO TEMPORAL (SIN CAMBIOS) ===
    # ===============================================================

    def _generate_temporal_dictionary_integrated(self) -> bool:
        """🔥 Generar diccionario temporal (SIN CAMBIOS)"""
        
        print(f"\n🔥 GENERANDO DICCIONARIO TEMPORAL DINÁMICO")
        print("="*60)
        
        self.temporal_dictionary = {}
        self.temporal_values_by_column = {}
        total_values = 0
        total_variants = 0
        skipped_columns = []
        
        dimension_columns = []
        for col_name, classification in self.classified_columns.items():
            if classification.type == 'dimension':
                dimension_columns.append(col_name)
        
        print(f"📂 Dimensiones detectadas: {len(dimension_columns)}")
        for col in dimension_columns:
            classification = self.classified_columns[col]
            print(f"   ✅ {col} ({classification.detection_method})")
        
        if not dimension_columns:
            print(f"❌ No hay dimensiones para procesar")
            return False
        
        for col_name in dimension_columns:
            print(f"\n🔄 Procesando columna dimensión: {col_name}")
            
            unique_values = self._extract_unique_values_for_column(col_name)
            
            if not unique_values:
                print(f"   ⚠️ No hay valores válidos en {col_name}")
                skipped_columns.append((col_name, "sin_valores_validos"))
                continue
            
            if self.skip_single_value_columns and len(unique_values) < self.min_unique_values:
                if len(unique_values) == 1:
                    print(f"   🚫 DESCARTADA: Solo tiene 1 valor único ('{unique_values[0]}')")
                    skipped_columns.append((col_name, "un_solo_valor"))
                else:
                    print(f"   🚫 DESCARTADA: Solo tiene {len(unique_values)} valores únicos (mínimo: {self.min_unique_values})")
                    skipped_columns.append((col_name, f"pocos_valores_{len(unique_values)}"))
                continue
            
            values_to_process = unique_values
            if self.skip_binary_values:
                filtered_values = []
                binary_count = 0
                for value in unique_values:
                    if self._is_binary_simple_value(value):
                        binary_count += 1
                    else:
                        filtered_values.append(value)
                
                if len(filtered_values) == 0:
                    print(f"   🚫 DESCARTADA: Todos los valores ({len(unique_values)}) son binarios simples")
                    skipped_columns.append((col_name, f"todos_binarios_{len(unique_values)}"))
                    continue
                elif binary_count > 0:
                    print(f"   🔄 Filtrados {binary_count} valores binarios simples, quedan {len(filtered_values)}")
                    values_to_process = filtered_values
                else:
                    values_to_process = unique_values
            
            self.temporal_values_by_column[col_name] = values_to_process
            total_values += len(values_to_process)
            
            column_variants = 0
            for original_value in values_to_process:
                variants = self._generate_value_variants(original_value)
                column_variants += len(variants)
                
                temporal_value = TemporalValue(
                    original_value=original_value,
                    column_name=col_name,
                    column_type='dimension',
                    variants=variants,
                    confidence=1.0
                )
                
                for variant in variants:
                    variant_key = variant.lower().strip()
                    
                    if variant_key in self.temporal_dictionary:
                        existing = self.temporal_dictionary[variant_key]
                        if len(original_value) < len(existing.original_value):
                            self.temporal_dictionary[variant_key] = temporal_value
                    else:
                        self.temporal_dictionary[variant_key] = temporal_value
            
            total_variants += column_variants
            print(f"   ✅ {len(values_to_process)} valores → {column_variants} variantes")
        
        processed_columns = len(self.temporal_values_by_column)
        
        self.temporal_generation_stats = {
            'dimension_columns_detected': len(dimension_columns),
            'dimension_columns_processed': processed_columns,
            'dimension_columns_skipped': len(skipped_columns),
            'skipped_reasons': skipped_columns,
            'total_values': total_values,
            'total_variants': total_variants,
            'dictionary_entries': len(self.temporal_dictionary),
            'values_per_column': {col: len(vals) for col, vals in self.temporal_values_by_column.items()}
        }
        
        self.temporal_enabled = True
        
        print(f"\n📊 DICCIONARIO TEMPORAL GENERADO:")
        print(f"   📂 Dimensiones detectadas: {len(dimension_columns)}")
        print(f"   ✅ Columnas procesadas: {processed_columns}")
        print(f"   🚫 Columnas omitidas: {len(skipped_columns)}")
        print(f"   📦 Valores únicos: {total_values}")
        print(f"   🔄 Variantes generadas: {total_variants}")
        print(f"   📖 Entradas en diccionario: {len(self.temporal_dictionary)}")
        
        if skipped_columns:
            print(f"\n🚫 COLUMNAS OMITIDAS DEL DICCIONARIO TEMPORAL:")
            for col_name, reason in skipped_columns:
                if reason == "un_solo_valor":
                    print(f"   • {col_name}: Solo 1 valor único (no aporta filtros)")
                elif reason == "sin_valores_validos":
                    print(f"   • {col_name}: Sin valores válidos")
                elif reason.startswith("todos_binarios_"):
                    count = reason.replace("todos_binarios_", "")
                    print(f"   • {col_name}: Todos los valores son binarios simples ({count} valores)")
                elif reason.startswith("pocos_valores_"):
                    count = reason.replace("pocos_valores_", "")
                    print(f"   • {col_name}: Solo {count} valores únicos")
                else:
                    print(f"   • {col_name}: {reason}")
        
        self._show_temporal_examples()
        
        if self.auto_save_temporal:
            success = self.auto_save_temporal_dictionary()
            if success:
                print(f"🎯 ACCESO DIRECTO: El problemizador puede cargar desde '{self.temporal_save_path}'")
        
        return True

    def _extract_unique_values_for_column(self, col_name: str) -> List[str]:
        """Extraer valores únicos de una columna (SIN CAMBIOS)"""
        
        if col_name not in self.current_table.columns:
            return []
        
        unique_series = self.current_table[col_name].dropna().unique()
        
        unique_values = []
        for val in unique_series:
            str_val = str(val).strip()
            if str_val and str_val.lower() not in ['nan', 'none', 'null', '', 'n/a']:
                unique_values.append(str_val)
        
        if len(unique_values) > 100000:
            print(f"   ⚠️ Limitando a 1000 valores (original: {len(unique_values)})")
            unique_values = unique_values[:100000]
        
        return unique_values

    def _generate_value_variants(self, original_value: str) -> List[str]:
        """Generar hasta 5 variantes por valor (SIN CAMBIOS)"""
        
        variants = set()
        
        variants.add(original_value)
        variants.add(original_value.lower())
        variants.add(original_value.upper())
        
        no_spaces = original_value.replace(' ', '').replace('_', '').replace('-', '')
        if no_spaces != original_value and no_spaces:
            variants.add(no_spaces)
        
        with_underscores = original_value.replace(' ', '_').replace('-', '_')
        if with_underscores != original_value:
            variants.add(with_underscores)
        
        no_accents = self._remove_accents(original_value)
        if no_accents != original_value:
            variants.add(no_accents)
        
        variants_list = list(variants)
        
        priority_order = [
            original_value,
            original_value.lower(),
            original_value.upper(),
            no_spaces,
            with_underscores
        ]
        
        final_variants = []
        for priority_variant in priority_order:
            if priority_variant in variants_list and len(final_variants) < 5:
                final_variants.append(priority_variant)
                variants_list.remove(priority_variant)
        
        for variant in variants_list:
            if len(final_variants) < 5:
                final_variants.append(variant)
        
        return final_variants[:5]

    def _remove_accents(self, text: str) -> str:
        """Remover acentos de un texto (SIN CAMBIOS)"""
        try:
            return ''.join(c for c in unicodedata.normalize('NFD', text)
                        if unicodedata.category(c) != 'Mn')
        except:
            return text

    def _show_temporal_examples(self):
        """Mostrar ejemplos del diccionario temporal generado (SIN CAMBIOS)"""
        
        print(f"\n🔄 EJEMPLOS DE VARIANTES GENERADAS:")
        
        examples_by_column = defaultdict(list)
        for variant_key, temporal_value in self.temporal_dictionary.items():
            if len(examples_by_column[temporal_value.column_name]) < 3:
                examples_by_column[temporal_value.column_name].append(temporal_value)
        
        for col_name, temporal_values in examples_by_column.items():
            print(f"   📂 {col_name}:")
            seen_originals = set()
            for temp_val in temporal_values:
                if temp_val.original_value not in seen_originals:
                    seen_originals.add(temp_val.original_value)
                    print(f"      🔹 '{temp_val.original_value}' → {len(temp_val.variants)} variantes")
                    for i, variant in enumerate(temp_val.variants[:3], 1):
                        print(f"          {i}. '{variant}'")

    def search_temporal_value(self, search_term: str) -> Optional[TemporalValue]:
        """🔍 Buscar un valor en el diccionario temporal (SIN CAMBIOS)"""
        
        if not self.temporal_enabled or not self.temporal_dictionary:
            return None
        
        search_key = search_term.lower().strip()
        
        if search_key in self.temporal_dictionary:
            return self.temporal_dictionary[search_key]
        
        for variant_key, temporal_value in self.temporal_dictionary.items():
            if search_key in variant_key or variant_key in search_key:
                return temporal_value
        
        return None

    def auto_save_temporal_dictionary(self) -> bool:
        """Guardar automáticamente el diccionario temporal (SIN CAMBIOS)"""
        
        if not self.auto_save_temporal or not self.temporal_enabled:
            return False
        
        print(f"\n💾 GUARDANDO DICCIONARIO TEMPORAL AUTOMÁTICAMENTE")
        print("="*60)
        
        try:
            self._create_backup_if_exists()
            
            export_data = self.export_for_problemizador()
            
            export_data['metadata'] = {
                'created_at': datetime.now().isoformat(),
                'source_file': self.current_file_path,
                'analizador_version': '6.0_anchor_compatible_con_temporal',
                'auto_saved': True,
                'classification_method': 'anchor_compatible_perfect_logic',
                'dictionary_system': self.get_dictionary_info()
            }
            
            with open(self.temporal_save_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Diccionario temporal guardado en: {self.temporal_save_path}")
            print(f"📊 Datos guardados:")
            print(f"   📖 Entradas: {len(export_data['temporal_dictionary'])}")
            print(f"   📂 Columnas: {len(export_data['dimension_columns'])}")
            print(f"   📅 Fecha: {export_data['metadata']['created_at']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error guardando diccionario temporal: {e}")
            return False

    def _create_backup_if_exists(self):
        """Crear respaldo del archivo anterior si existe (SIN CAMBIOS)"""
        
        if not self.create_backups:
            return
        
        if os.path.exists(self.temporal_save_path):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = f"diccionario_temporal_{timestamp}.json"
                backup_path = os.path.join(BACKUP_TEMPORAL_PATH, backup_name)
                
                import shutil
                shutil.copy2(self.temporal_save_path, backup_path)
                
                print(f"💾 Respaldo creado: {backup_name}")
                
            except Exception as e:
                print(f"⚠️ Error creando respaldo: {e}")

    def export_for_problemizador(self) -> Dict:
        """Exportar datos completos para el problemizador (CON INFO DE PALABRAS ANCLA)"""
        
        return {
            'temporal_dictionary': {
                variant_key: {
                    'original_value': temp_val.original_value,
                    'column_name': temp_val.column_name,
                    'column_type': temp_val.column_type,
                    'variants': temp_val.variants,
                    'confidence': temp_val.confidence
                }
                for variant_key, temp_val in self.temporal_dictionary.items()
            },
            'dimension_columns': [col for col, cls in self.classified_columns.items() 
                                if cls.type == 'dimension'],
            'processed_dimension_columns': list(self.temporal_values_by_column.keys()),
            'column_mappings': {col: cls.mapped_term for col, cls in self.classified_columns.items()
                            if cls.mapped_term},
            'generation_stats': self.temporal_generation_stats,
            'table_info': {
                'file_path': self.current_file_path,
                'rows': len(self.current_table) if self.current_table is not None else 0,
                'columns': len(self.current_table.columns) if self.current_table is not None else 0
            },
            'classification_summary': {
                'dimensions_found': len([c for c in self.classified_columns.values() if c.type == 'dimension']),
                'metrics_found': len([c for c in self.classified_columns.values() if c.type == 'metric']),
                'temporal_enabled': self.temporal_enabled,
                'perfect_classification': True,
                'optimization_applied': True,
                'binary_value_optimization': self.skip_binary_values,
                'single_value_optimization': self.skip_single_value_columns,
                'uses_anchor_system': True  # 🔥 NUEVO
            },
            'dictionary_system_info': self.get_dictionary_info()  # 🔥 INFO del sistema con palabras ancla
        }

    def _show_analysis_summary(self, result: Dict):
        """✅ TU RESUMEN ORIGINAL + Info del sistema con palabras ancla"""
        
        print(f"\n📋 RESUMEN DEL ANÁLISIS CON PALABRAS ANCLA + TEMPORAL")
        print("="*70)
        
        file_info = result['file_info']
        summary = result['summary']
        classification = result['classification']
        temporal_info = result['temporal_dictionary']
        dict_system = result.get('dictionary_system', {})
        
        print(f"📂 Archivo: {file_info['name']}")
        print(f"📊 Tamaño: {file_info['rows']:,} filas × {file_info['columns']} columnas")
        
        # 🔥 SECCIÓN: Info del sistema con palabras ancla
        print(f"\n🔧 SISTEMA DE DICCIONARIOS CON PALABRAS ANCLA:")
        print(f"   📋 Modo: {dict_system.get('mode', 'Desconocido')}")
        print(f"   📊 Estado: {dict_system.get('status', 'Desconocido')}")
        print(f"   📁 Carga desde: {dict_system.get('json_path', 'N/A')}")
        print(f"   ✅ Carga exitosa: {dict_system.get('load_successful', 'N/A')}")
        print(f"   🔥 Sistema de anclas: {dict_system.get('uses_anchor_system', 'N/A')}")
        if 'statistics' in dict_system:
            stats = dict_system['statistics']
            print(f"   📂 Dimensiones (expandidas): {stats.get('total_dimensiones', 'N/A')}")
            print(f"   📊 Métricas (expandidas): {stats.get('total_metricas', 'N/A')}")
        if 'anchor_info' in dict_system:
            anchor_info = dict_system['anchor_info']
            print(f"   📍 Anclas de dimensión: {anchor_info.get('dimension_anchors', 'N/A')}")
            print(f"   📍 Anclas de métrica: {anchor_info.get('metric_anchors', 'N/A')}")
            print(f"   🔥 Total expansiones: {anchor_info.get('total_expansions', 'N/A')}")
        
        print(f"\n🎯 CLASIFICACIÓN:")
        print(f"   📂 Dimensiones encontradas: {summary['dimensions_count']}")
        print(f"   📊 Métricas encontradas: {summary['metrics_count']}")
        print(f"   📄 Otras columnas: {len(classification['other'])}")
        
        if self.dimensions_dict or self.metrics_dict:
            print(f"\n🔗 MAPEO CON DICCIONARIO EXPANDIDO:")
            print(f"   ✅ Columnas mapeadas: {summary['mapped_count']}")
            print(f"   ❌ Sin mapear: {summary['unmapped_count']}")
            success_rate = summary['mapped_count']/summary['total_columns']*100 if summary['total_columns'] > 0 else 0
            print(f"   📊 Tasa de éxito: {success_rate:.1f}%")
        
        # 🔥 SECCIÓN TEMPORAL (MEJORADA)
        print(f"\n🔥 DICCIONARIO TEMPORAL:")
        if temporal_info['generated']:
            print(f"   ✅ Generado exitosamente")
            print(f"   📖 Entradas: {temporal_info['entries_count']:,}")
            print(f"   📂 Dimensiones detectadas: {temporal_info['stats'].get('dimension_columns_detected', 0)}")
            print(f"   ✅ Columnas procesadas: {temporal_info['stats'].get('dimension_columns_processed', 0)}")
            print(f"   🚫 Columnas omitidas: {temporal_info['stats'].get('dimension_columns_skipped', 0)}")
            print(f"   📦 Valores únicos: {temporal_info['stats'].get('total_values', 0):,}")
            print(f"   🔄 Variantes generadas: {temporal_info['stats'].get('total_variants', 0):,}")
        else:
            print(f"   ❌ No generado (sin dimensiones válidas)")
        
        if classification['dimensions']:
            print(f"\n📂 DIMENSIONES DETECTADAS CON PALABRAS ANCLA:")
            for col_name, info in classification['dimensions'].items():
                mapping = f"→ {info.mapped_term}" if info.mapped_term else "(sin mapeo)"
                method = f"[{info.detection_method}]"
                confidence = f"({info.confidence:.1%})"
                
                # 🚨 AGREGAR INFO DE PALABRA ANCLA
                anchor_info = ""
                if self.dictionaries and hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(col_name)
                    if anchor:
                        anchor_info = f" 🔥[ancla: {anchor}]"
                
                temporal_values = len(self.temporal_values_by_column.get(col_name, []))
                temporal_info_str = f"→ {temporal_values} valores en diccionario temporal" if temporal_values > 0 else ""
                
                print(f"   ✅ {col_name} {mapping} {confidence} {method}{anchor_info}")
                if temporal_info_str:
                    print(f"      🔥 {temporal_info_str}")
        
        if classification['metrics']:
            print(f"\n📊 MÉTRICAS DETECTADAS CON PALABRAS ANCLA:")
            for col_name, info in classification['metrics'].items():
                mapping = f"→ {info.mapped_term}" if info.mapped_term else "(sin mapeo)"
                method = f"[{info.detection_method}]"
                confidence = f"({info.confidence:.1%})"
                
                # 🚨 AGREGAR INFO DE PALABRA ANCLA
                anchor_info = ""
                if self.dictionaries and hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(col_name)
                    if anchor:
                        anchor_info = f" 🔥[ancla: {anchor}]"
                
                print(f"   ✅ {col_name} {mapping} {confidence} {method}{anchor_info}")
        
        # ✅ TU PREVIEW ORIGINAL (SIN CAMBIOS)
        if self.current_table is not None:
            print(f"\n📋 PREVIEW COMPLETO DE TUS DATOS")
            print("=" * 80)
            
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 25)
            pd.set_option('display.expand_frame_repr', False)
            
            sample = self.current_table.head(10)
            print(f"📊 Mostrando 10 filas de {len(self.current_table):,} totales:")
            print(f"📋 Todas las columnas ({len(self.current_table.columns)}):")
            print()
            print(sample.to_string(index=True))
            
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
            pd.reset_option('display.max_colwidth')
            pd.reset_option('display.expand_frame_repr')
            
            print(f"\n🔍 DETALLE DE COLUMNAS:")
            for i, (col_name, classification) in enumerate(self.classified_columns.items(), 1):
                tipo_info = f"{classification.type}"
                if classification.mapped_term:
                    tipo_info += f" → {classification.mapped_term}"
                
                method_info = f"[{classification.detection_method}]"
                confidence_info = f"({classification.confidence:.1%})" if classification.confidence > 0 else ""
                
                # 🚨 AGREGAR INFO DE PALABRA ANCLA
                anchor_info = ""
                if self.dictionaries and hasattr(self.dictionaries, 'get_anchor_for_term'):
                    anchor = self.dictionaries.get_anchor_for_term(col_name)
                    if anchor:
                        anchor_info = f" 🔥[ancla: {anchor}]"
                
                if classification.type == 'dimension' and classification.unique_count <= 10:
                    unique_vals = self.current_table[col_name].unique()[:5]
                    valores_ejemplo = f" (ej: {', '.join(map(str, unique_vals))})"
                else:
                    valores_ejemplo = f" ({classification.unique_count} valores únicos)"
                
                print(f"   {i:2d}. {col_name}: {tipo_info} {confidence_info} {method_info}{anchor_info}{valores_ejemplo}")
                
                
    # =============================================================
    # NUEVA FUNCION PARA CARGAR BASES DE DATOS NORMALIZADAS A MYSQL
    # =============================================================

    def upload_current_table_to_mysql(self) -> bool:
        """Subir tabla actual a MySQL con manejo completo de errores"""
        
        # Verificar que hay tabla cargada
        if self.current_table is None:
            print(f"No hay tabla cargada para subir")
            print(f"Primero analiza una tabla con la opción 2")
            return False
        
        # Verificar conexión MySQL
        if not self.mysql_integration.connection_available:
            print(f"MySQL no está disponible")
            print(f"Verifica tu configuración de MySQL")
            return False
        
        # Mostrar información de la tabla actual
        print(f"\nTABLA ACTUAL LISTA PARA SUBIR:")
        print("="*60)
        print(f"Archivo: {Path(self.current_file_path).name if self.current_file_path else 'Desconocido'}")
        print(f"Datos: {len(self.current_table)} filas × {len(self.current_table.columns)} columnas")
        print(f"Dimensiones: {len([c for c in self.classified_columns.values() if c.type == 'dimension'])}")
        print(f"Métricas: {len([c for c in self.classified_columns.values() if c.type == 'metric'])}")
        
        # Confirmar con el usuario
        print(f"\n¿Subir esta tabla a MySQL?")
        print(f"Base de datos: {self.mysql_integration.config['database']}")
        print(f"Servidor: {self.mysql_integration.config['host']}")
        
        confirm = input(f"\nConfirmar subida (s/n): ").strip().lower()
        
        if confirm not in ['s', 'si', 'sí', 'y', 'yes']:
            print(f"Subida cancelada por el usuario")
            return False
        
        # Proceder con la subida
        try:
            print(f"\nINICIANDO SUBIDA A MYSQL...")
            
            # Generar nombre único
            mysql_table_name = self.mysql_integration.generate_table_name(self.current_file_path or "tabla_temporal")
            print(f"Nombre de tabla generado: {mysql_table_name}")
            
            # Preparar información de análisis
            analysis_info = {
                'file_info': {
                    'name': Path(self.current_file_path).stem if self.current_file_path else 'tabla_temporal',
                    'path': self.current_file_path
                },
                'summary': {
                    'dimensions_count': len([c for c in self.classified_columns.values() if c.type == 'dimension']),
                    'metrics_count': len([c for c in self.classified_columns.values() if c.type == 'metric']),
                    'total_columns': len(self.current_table.columns)
                }
            }
            
            # Subir tabla
            upload_result = self.mysql_integration.upload_table(
                dataframe=self.current_table,
                table_name=mysql_table_name,
                file_path=self.current_file_path or "",
                analysis_info=analysis_info
            )
            
            if upload_result['success']:
                print(f"\n¡TABLA SUBIDA EXITOSAMENTE!")
                print("="*60)
                print(f"Nombre MySQL: {mysql_table_name}")
                print(f"Base de datos: {upload_result['database']}")
                print(f"Servidor: {upload_result['host']}")
                print(f"Datos subidos: {upload_result['rows']:,} filas × {upload_result['columns']} columnas")
                print(f"\n¡Tu tabla está lista para consultas desde MySQL!")
                print(f"Usa el nombre: {mysql_table_name}")
                
                return True
                
            else:
                print(f"Error en la subida: {upload_result['error']}")
                if 'traceback' in upload_result:
                    print(f"Detalles técnicos:\n{upload_result['traceback']}")
                return False
                
        except Exception as e:
            print(f"Error inesperado: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return False
        

    def show_mysql_status(self):
        print(f"\n🗄️ ESTADO DE MYSQL")
        print("="*50)

        if self.mysql_integration.connection_available:
            config = self.mysql_integration.config
            print(f"✅ Conexión: Disponible")
            print(f"🌐 Servidor: {config['host']}:{config['port']}")
            print(f"📁 Base de datos: {config['database']}")
            print(f"👤 Usuario: {config['user']}")
        else:
            print(f"❌ Conexión: No disponible")
            print(f"💡 Verifica tu configuración MySQL")
            return

        # Obtener tablas
        tables = self.mysql_integration.list_tables()
        print("DEBUG: list_tables result:", tables)

        # Validación del resultado:
        if tables is None:
            print("❌ list_tables devolvió None. No se pudo obtener la lista de tablas.")
            return
        if not isinstance(tables, list):
            print(f"❌ list_tables devolvió un tipo inesperado: {type(tables).__name__}")
            return

        print(f"\n📋 TABLAS EN MYSQL ({len(tables)}):")
        print("-" * 70)

        if tables:
            for i, table in enumerate(tables, 1):
                print(f"{i:2d}. 📊 {table['table_name']}")
                print(f"     📁 {table['original_filename']}")
                print(f"     📊 {table['total_rows']:,} filas × {table['total_columns']} columnas")
                print(f"     📂 {table['dimensions_count']} dims, {table['metrics_count']} métricas")
                print(f"     📅 {table['analyzed_at']}")
                print()
        else:
            print(f"📭 No hay tablas subidas aún")
            print(f"💡 Analiza una tabla y luego súbela con la opción correspondiente")


    def configure_mysql(self):
        """Configurar conexión MySQL interactivamente"""
        
        print(f"\n🔧 CONFIGURACIÓN MYSQL")
        print("="*40)
        
        print(f"📊 Configuración actual:")
        config = self.mysql_integration.config
        print(f"   🌐 Host: {config['host']}")
        print(f"   📁 Base de datos: {config['database']}")
        print(f"   👤 Usuario: {config['user']}")
        print(f"   🔌 Estado: {'✅ Conectado' if self.mysql_integration.connection_available else '❌ Sin conexión'}")
        
        print(f"\n¿Deseas cambiar la configuración? (s/n): ", end="")
        if input().strip().lower() not in ['s', 'si', 'sí', 'y', 'yes']:
            return
        
        # Solicitar nueva configuración
        new_config = {}
        new_config['host'] = input(f"Host ({config['host']}): ").strip() or config['host']
        new_config['port'] = int(input(f"Puerto ({config['port']}): ").strip() or config['port'])
        new_config['database'] = input(f"Base de datos ({config['database']}): ").strip() or config['database']
        new_config['user'] = input(f"Usuario ({config['user']}): ").strip() or config['user']
        new_config['password'] = input(f"Contraseña: ").strip() or config['password']
        new_config['charset'] = 'utf8mb4'
        
        # Actualizar configuración
        self.mysql_integration.config = new_config
        
        # Probar nueva conexión
        print(f"\n🧪 Probando nueva configuración...")
        self.mysql_integration._test_connection()
        
        if self.mysql_integration.connection_available:
            print(f"✅ Nueva configuración funcionando correctamente")
        else:
            print(f"❌ Error con la nueva configuración")
            print(f"💡 Revisa los datos ingresados")               
                
                
    def upload_current_table_to_clickhouse(self) -> bool:
        """Subir tabla actual a ClickHouse"""
        
        if self.current_table is None:
            print("No hay tabla cargada para subir")
            return False
        
        if not self.clickhouse_integration.connection_available:
            print("ClickHouse no está disponible")
            return False
        
        print("\nTABLA ACTUAL LISTA PARA SUBIR A CLICKHOUSE:")
        print("="*60)
        print(f"Archivo: {Path(self.current_file_path).name if self.current_file_path else 'Desconocido'}")
        print(f"Datos: {len(self.current_table)} filas × {len(self.current_table.columns)} columnas")
        
        confirm = input("\nConfirmar subida a ClickHouse (s/n): ").strip().lower()
        
        if confirm not in ['s', 'si', 'sí', 'y', 'yes']:
            print("Subida cancelada")
            return False
        
        try:
            ch_table_name = self.clickhouse_integration.generate_table_name(
                self.current_file_path or "tabla_temporal"
            )
            
            analysis_info = {
                'file_info': {
                    'name': Path(self.current_file_path).stem if self.current_file_path else 'tabla_temporal',
                    'path': self.current_file_path
                },
                'summary': {
                    'dimensions_count': len([c for c in self.classified_columns.values() if c.type == 'dimension']),
                    'metrics_count': len([c for c in self.classified_columns.values() if c.type == 'metric']),
                    'total_columns': len(self.current_table.columns)
                }
            }
            
            upload_result = self.clickhouse_integration.upload_table(
                dataframe=self.current_table,
                table_name=ch_table_name,
                file_path=self.current_file_path or "",
                analysis_info=analysis_info
            )
            
            if upload_result['success']:
                print("\n¡TABLA SUBIDA EXITOSAMENTE A CLICKHOUSE!")
                print("="*60)
                print(f"Nombre: {ch_table_name}")
                print(f"Filas: {upload_result['rows']:,}")
                print(f"Columnas: {upload_result['columns']}")
                return True
            else:
                print(f"Error: {upload_result['error']}")
                return False
                
        except Exception as e:
            print(f"Error inesperado: {str(e)}")
            return False

    def show_database_status(self):
        """Mostrar estado de ambas bases de datos"""
        print("\n📊 ESTADO DE BASES DE DATOS")
        print("="*60)
        
        # Estado MySQL
        print("🗄️ MYSQL:")
        if self.mysql_integration.connection_available:
            config = self.mysql_integration.config
            print(f"   ✅ Disponible: {config['host']}/{config['database']}")
            
            mysql_tables = self.mysql_integration.list_tables()
            print(f"   📋 Tablas: {len(mysql_tables)}")
        else:
            print("   ❌ No disponible")
        
        print()
        
        # Estado ClickHouse
        print("🏪 CLICKHOUSE:")
        if self.clickhouse_integration.connection_available:
            config = self.clickhouse_integration.config
            print(f"   ✅ Disponible: {config['host']}/{config['database']}")
            
            ch_tables = self.clickhouse_integration.list_tables()
            print(f"   📋 Tablas: {len(ch_tables)}")
        else:
            print("   ❌ No disponible")

    def show_mysql_tables(self):
        """Mostrar solo tablas MySQL"""
        print("\n🗄️ TABLAS EN MYSQL")
        print("="*50)
        
        if not self.mysql_integration.connection_available:
            print("❌ MySQL no está disponible")
            return
        
        tables = self.mysql_integration.list_tables()
        if tables:
            for i, table in enumerate(tables, 1):
                print(f"{i:2d}. 📊 {table['table_name']}")
                print(f"     📁 {table['original_filename']}")
                print(f"     📊 {table['total_rows']:,} filas × {table['total_columns']} columnas")
                print()
        else:
            print("📭 No hay tablas en MySQL")

    def show_clickhouse_tables(self):
        """Mostrar solo tablas ClickHouse"""
        print("\n🏪 TABLAS EN CLICKHOUSE")
        print("="*50)
        
        if not self.clickhouse_integration.connection_available:
            print("❌ ClickHouse no está disponible")
            return
        
        tables = self.clickhouse_integration.list_tables()
        if tables:
            for i, table in enumerate(tables, 1):
                print(f"{i:2d}. 📊 {table['table_name']}")
                print(f"     📁 {table['original_filename']}")
                print(f"     📊 {table['total_rows']:,} filas × {table['total_columns']} columnas")
                print()
        else:
            print("📭 No hay tablas en ClickHouse")

    def configure_clickhouse(self):
        """Configurar ClickHouse"""
        print("\n🔧 CONFIGURACIÓN CLICKHOUSE")
        print("="*40)
        
        config = self.clickhouse_integration.config
        print(f"Host actual: {config['host']}")
        print(f"Puerto: {config['port']}")
        print(f"Base de datos: {config['database']}")
        print(f"Usuario: {config['username']}")
        
        if input("\n¿Cambiar configuración? (s/n): ").strip().lower().startswith('s'):
            new_config = {}
            new_config['host'] = input(f"Host ({config['host']}): ").strip() or config['host']
            new_config['port'] = int(input(f"Puerto ({config['port']}): ").strip() or config['port'])
            new_config['database'] = input(f"BD ({config['database']}): ").strip() or config['database']
            new_config['username'] = input(f"Usuario ({config['username']}): ").strip() or config['username']
            new_config['password'] = input("Password: ").strip() or config['password']
            new_config['secure'] = input("Secure (true/false): ").strip().lower() == 'true'
            
            self.clickhouse_integration.config = new_config
            self.clickhouse_integration._test_connection()
                
                
                
# =============================================
# MODIFICAR TU FUNCIÓN run_table_analyzer()
# =============================================

def run_table_analyzer():
    """🔥 FUNCIÓN PRINCIPAL CON OPCIONES MYSQL + CLICKHOUSE"""
    
    print("🎯 ANALIZADOR COMPATIBLE CON PALABRAS ANCLA")
    print("="*80)
    print("✨ Tu lógica de clasificación perfecta")
    print("📁 + Carga desde archivos JSON con palabras ancla")
    print("🔥 + Sistema expandido: 10+ variaciones por término")
    print("🔥 + Sistema de diccionario temporal optimizado")
    print("🗄️ + Opción para subir a MySQL")
    print("🏪 + Opción para subir a ClickHouse")  # ⬅️ NUEVA LÍNEA
    print("="*80)
    
    analyzer = TableAnalyzer()
    
    # Mostrar información inicial del sistema con palabras ancla
    dict_info = analyzer.get_dictionary_info()
    print(f"\n🔧 ESTADO INICIAL DEL SISTEMA:")
    print(f"   📋 Modo: {dict_info['mode']}")
    print(f"   📊 Estado: {dict_info['status']}")
    print(f"   📁 Carga desde: {dict_info['json_path']}")
    print(f"   ✅ Carga exitosa: {dict_info['load_successful']}")
    print(f"   🔥 Sistema de anclas: {dict_info['uses_anchor_system']}")
    if 'statistics' in dict_info:
        stats = dict_info['statistics']
        print(f"   📂 Dimensiones: {stats.get('total_dimensiones', 'N/A')}")
        print(f"   📊 Métricas: {stats.get('total_metricas', 'N/A')}")
    
    # MOSTRAR ESTADO DE AMBAS BASES DE DATOS
    print(f"\n📊 BASES DE DATOS DISPONIBLES:")
    print(f"🗄️ MYSQL:")
    if analyzer.mysql_integration.connection_available:
        config = analyzer.mysql_integration.config
        print(f"   ✅ Disponible: {config['host']}/{config['database']}")
    else:
        print(f"   ❌ No disponible (configurar más tarde)")
    
    print(f"🏪 CLICKHOUSE:")
    if analyzer.clickhouse_integration.connection_available:
        config = analyzer.clickhouse_integration.config
        print(f"   ✅ Disponible: {config['host']}/{config['database']}")
    else:
        print(f"   ❌ No disponible (configurar más tarde)")
    
    while True:
        print(f"\n📋 OPCIONES:")
        print("1. 🔍 Diagnosticar archivo")
        print("2. 📊 Analizar tabla (palabras ancla + temporal)")
        print("3. ⚙️ Configurar optimización temporal")
        print("4. 👀 Ver datos de tabla actual")
        print("5. 🔥 Probar diccionario temporal")
        print("6. 📤 Exportar para problemizador")
        print("7. 💾 Guardar diccionario temporal")
        print("8. 🔧 Gestionar sistema con palabras ancla")
        print("9. 🧪 Probar reconocimiento de palabras ancla")
        print()
        print("📊 BASES DE DATOS:")
        print("10. 🗄️ Subir tabla actual a MySQL")
        print("11. 🏪 Subir tabla actual a ClickHouse")         # ⬅️ NUEVA OPCIÓN
        print("12. 📋 Ver estado de ambas bases de datos")      # ⬅️ NUEVA OPCIÓN
        print("13. 🗄️ Ver tablas en MySQL")
        print("14. 🏪 Ver tablas en ClickHouse")                # ⬅️ NUEVA OPCIÓN
        print("15. 🔧 Configurar MySQL")
        print("16. 🔧 Configurar ClickHouse")                   # ⬅️ NUEVA OPCIÓN
        print("17. 🚪 Salir")
        
        choice = input(f"\n🎯 Selecciona (1-17): ").strip()
        
        if choice == '1':
            file_path = input(f"📂 Ruta de tu archivo: ").strip()
            if file_path:
                diagnosis = analyzer.diagnose_file(file_path)
                if not diagnosis['errors']:
                    print(f"\n✅ Archivo parece estar bien. ¿Intentar cargarlo? (s/n): ", end="")
                    if input().lower().startswith('s'):
                        result = analyzer.analyze_table(file_path)
        
        elif choice == '2':
            file_path = input(f"📂 Ruta de tu archivo: ").strip()
            if file_path:
                result = analyzer.analyze_table(file_path)
                
                if result['success']:
                    print(f"\n✅ Análisis completado exitosamente")
                    print(f"📂 Dimensiones: {result['summary']['dimensions_count']}")
                    print(f"📊 Métricas: {result['summary']['metrics_count']}")
                    print(f"📄 Otras: {len(result['classification']['other'])}")
                    
                    if result['temporal_dictionary']['generated']:
                        print(f"🔥 Diccionario temporal: {result['temporal_dictionary']['entries_count']} entradas")
                    else:
                        print(f"⚠️ No se generó diccionario temporal")
                    
                    # SUGERIR SUBIDA A AMBAS BASES DE DATOS
                    print(f"\n💡 Tabla analizada y normalizada.")
                    if analyzer.mysql_integration.connection_available:
                        print(f"💡 Puedes subirla a MySQL usando la opción 10")
                    if analyzer.clickhouse_integration.connection_available:
                        print(f"💡 Puedes subirla a ClickHouse usando la opción 11")
                    
                else:
                    print(f"\n❌ Error: {result['error']}")
        
        elif choice == '3':
            single_status = "ACTIVADA" if analyzer.skip_single_value_columns else "DESACTIVADA"
            binary_status = "ACTIVADA" if analyzer.skip_binary_values else "DESACTIVADA"
            print(f"\n⚙️ CONFIGURACIÓN DE OPTIMIZACIÓN TEMPORAL")
            print(f"   🚫 Omitir columnas con 1 valor: {single_status}")
            print(f"   🚫 Omitir columnas binarias (Y/N, 1/0): {binary_status}")
            print(f"   📏 Mínimo valores únicos: {analyzer.min_unique_values}")
            
            print(f"\n🔧 OPCIONES:")
            print("1. Optimización COMPLETA (omitir 1 valor + binarias + mínimo 3)")
            print("2. Optimización BÁSICA (omitir 1 valor + binarias)")
            print("3. Solo omitir columnas con 1 valor")
            print("4. Solo omitir columnas binarias (Y/N, 1/0)")
            print("5. Desactivar toda optimización")
            print("6. Configuración personalizada")
            print("7. Mantener configuración actual")
            
            opt_choice = input(f"Selecciona (1-7): ").strip()
            
            if opt_choice == '1':
                analyzer.configure_temporal_optimization(skip_single_value=True, skip_binary_values=True, min_unique=3)
            elif opt_choice == '2':
                analyzer.configure_temporal_optimization(skip_single_value=True, skip_binary_values=True, min_unique=2)
            elif opt_choice == '3':
                analyzer.configure_temporal_optimization(skip_single_value=True, skip_binary_values=False, min_unique=2)
            elif opt_choice == '4':
                analyzer.configure_temporal_optimization(skip_single_value=False, skip_binary_values=True, min_unique=1)
            elif opt_choice == '5':
                analyzer.configure_temporal_optimization(skip_single_value=False, skip_binary_values=False, min_unique=1)
            elif opt_choice == '6':
                print("Configuración personalizada:")
                try:
                    skip_single = input("¿Omitir columnas con 1 valor? (s/n): ").lower().startswith('s')
                    skip_binary = input("¿Omitir columnas binarias (Y/N, 1/0)? (s/n): ").lower().startswith('s')
                    min_val = int(input("Mínimo valores únicos requeridos: "))
                    analyzer.configure_temporal_optimization(skip_single_value=skip_single, skip_binary_values=skip_binary, min_unique=min_val)
                except ValueError:
                    print("❌ Valor inválido")
            else:
                print("✅ Configuración mantenida")
        
        elif choice == '4':
            if analyzer.current_table is not None:
                print(f"\n📊 TABLA ACTUAL: {len(analyzer.current_table)} filas × {len(analyzer.current_table.columns)} columnas")
                
                print(f"\n📋 COLUMNAS Y CLASIFICACIÓN:")
                for i, (col_name, classification) in enumerate(analyzer.classified_columns.items(), 1):
                    tipo_info = f"{classification.type}"
                    if hasattr(classification, 'mapped_term') and classification.mapped_term:
                        tipo_info += f" → {classification.mapped_term}"
                    if hasattr(classification, 'detection_method'):
                        tipo_info += f" [{classification.detection_method}]"
                    
                    # AGREGAR INFO DE PALABRA ANCLA
                    anchor_info = ""
                    if analyzer.dictionaries and hasattr(analyzer.dictionaries, 'get_anchor_for_term'):
                        anchor = analyzer.dictionaries.get_anchor_for_term(col_name)
                        if anchor:
                            anchor_info = f" 🔥[ancla: {anchor}]"
                    
                    if classification.type == 'dimension' and col_name in analyzer.temporal_values_by_column:
                        temporal_count = len(analyzer.temporal_values_by_column[col_name])
                        tipo_info += f" (🔥 {temporal_count} valores temporales)"
                    
                    print(f"   {i:2d}. {col_name}: {tipo_info}{anchor_info}")
                
                print(f"\n📊 MUESTRA DE DATOS:")
                print(analyzer.current_table.head().to_string())
            else:
                print(f"\n❌ No hay tabla cargada")
        
        elif choice == '5':
            if analyzer.temporal_enabled and analyzer.temporal_dictionary:
                print(f"\n🔥 PROBANDO DICCIONARIO TEMPORAL")
                print(f"📖 Entradas disponibles: {len(analyzer.temporal_dictionary)}")
                
                print(f"\n📋 ALGUNOS VALORES DISPONIBLES:")
                count = 0
                for col_name, values in analyzer.temporal_values_by_column.items():
                    if count < 3:
                        print(f"   📂 {col_name}: {', '.join(values[:5])}{'...' if len(values) > 5 else ''}")
                        count += 1
                
                while True:
                    search_term = input(f"\n🔍 Buscar valor (o 'salir'): ").strip()
                    if search_term.lower() == 'salir':
                        break
                    
                    result = analyzer.search_temporal_value(search_term)
                    if result:
                        print(f"   ✅ ENCONTRADO: '{search_term}' → '{result.original_value}'")
                        print(f"      📂 Columna: {result.column_name}")
                        print(f"      🔄 Variantes: {result.variants}")
                    else:
                        print(f"   ❌ No encontrado: '{search_term}'")
            else:
                print(f"\n❌ No hay diccionario temporal generado")
        
        elif choice == '6':
            if analyzer.current_table is not None:
                export_data = analyzer.export_for_problemizador()
                print(f"\n📤 DATOS PARA PROBLEMIZADOR:")
                print(f"   📖 Entradas diccionario temporal: {len(export_data['temporal_dictionary'])}")
                print(f"   📂 Columnas dimensión: {len(export_data['dimension_columns'])}")
                print(f"   ✅ Columnas procesadas: {len(export_data['processed_dimension_columns'])}")
                print(f"   🔗 Mapeos disponibles: {len(export_data['column_mappings'])}")
                print(f"   📊 Estadísticas: {export_data['generation_stats']}")
                print(f"   🔧 Sistema: {export_data['dictionary_system_info']['mode']}")
                print(f"   🔥 Anclas: {export_data['classification_summary']['uses_anchor_system']}")
                
                print(f"\n✅ Datos listos para cargar en problemizador")
            else:
                print(f"\n❌ No hay tabla cargada")
        
        elif choice == '7':
            if analyzer.temporal_enabled and analyzer.temporal_dictionary:
                export_data = analyzer.export_for_problemizador()
                
                if analyzer.current_file_path:
                    base_name = Path(analyzer.current_file_path).stem
                else:
                    base_name = "tabla_desconocida"
                
                filename = f"temporal_data_{base_name}.json"
                
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(export_data, f, indent=2, ensure_ascii=False)
                    print(f"\n✅ Diccionario temporal guardado en: {filename}")
                    print(f"📊 Datos guardados:")
                    print(f"   📖 Entradas: {len(export_data['temporal_dictionary'])}")
                    print(f"   📂 Columnas: {len(export_data['dimension_columns'])}")
                    print(f"   ✅ Procesadas: {len(export_data['processed_dimension_columns'])}")
                    print(f"   🔧 Sistema: {export_data['dictionary_system_info']['mode']}")
                    print(f"   🔥 Anclas: {export_data['classification_summary']['uses_anchor_system']}")
                except Exception as e:
                    print(f"\n❌ Error guardando archivo: {e}")
            else:
                print(f"\n❌ No hay diccionario temporal para guardar")
        
        elif choice == '8':
            # GESTIÓN DEL SISTEMA CON PALABRAS ANCLA
            print(f"\n🔧 GESTIÓN DEL SISTEMA CON PALABRAS ANCLA")
            print("="*50)
            
            dict_info = analyzer.get_dictionary_info()
            print(f"📋 Modo: {dict_info['mode']}")
            print(f"📊 Estado: {dict_info['status']}")
            print(f"📁 Carga desde: {dict_info['json_path']}")
            print(f"✅ Carga exitosa: {dict_info['load_successful']}")
            print(f"🔥 Sistema de anclas: {dict_info['uses_anchor_system']}")
            
            if 'statistics' in dict_info:
                stats = dict_info['statistics']
                print(f"📂 Dimensiones (expandidas): {stats.get('total_dimensiones', 'N/A')}")
                print(f"📊 Métricas (expandidas): {stats.get('total_metricas', 'N/A')}")
            
            if 'anchor_info' in dict_info:
                anchor_info = dict_info['anchor_info']
                print(f"📍 Anclas de dimensión: {anchor_info.get('dimension_anchors', 'N/A')}")
                print(f"📍 Anclas de métrica: {anchor_info.get('metric_anchors', 'N/A')}")
                print(f"🔥 Total expansiones: {anchor_info.get('total_expansions', 'N/A')}")
            
            print(f"\n🔧 OPCIONES:")
            print("1. 🔄 Recargar diccionarios desde JSON")
            print("2. 📊 Ver estadísticas detalladas")
            print("3. 🔍 Ver información completa del sistema")
            print("4. 📁 Verificar estructura de archivos JSON")
            print("5. 📍 Ver información de palabras ancla")
            print("6. 🔙 Volver al menú principal")
            
            sub_choice = input(f"Selecciona (1-6): ").strip()
            
            if sub_choice == '1':
                print(f"\n🔄 Recargando diccionarios con palabras ancla...")
                success = analyzer.reload_dictionaries()
                if success:
                    print(f"✅ Diccionarios recargados exitosamente")
                    new_info = analyzer.get_dictionary_info()
                    print(f"📋 Modo: {new_info['mode']}")
                    print(f"✅ Carga exitosa: {new_info['load_successful']}")
                    print(f"🔥 Sistema de anclas: {new_info['uses_anchor_system']}")
                else:
                    print(f"❌ Error recargando diccionarios")
            
            elif sub_choice == '2':
                print(f"\n📊 ESTADÍSTICAS DETALLADAS:")
                if 'statistics' in dict_info:
                    stats = dict_info['statistics']
                    for key, value in stats.items():
                        print(f"   {key}: {value}")
                else:
                    print(f"   ❌ No hay estadísticas disponibles")
            
            elif sub_choice == '3':
                print(f"\n🔍 INFORMACIÓN COMPLETA DEL SISTEMA:")
                for key, value in dict_info.items():
                    print(f"   {key}: {value}")
            
            elif sub_choice == '4':
                print(f"\n📁 VERIFICANDO ESTRUCTURA JSON...")
                # Verificar archivos JSON
                loader = get_dictionaries()
                structure_ok = loader._verify_json_structure()
                if structure_ok:
                    print(f"   ✅ Estructura JSON verificada correctamente")
                else:
                    print(f"   ❌ Estructura JSON incompleta")
                    print(f"   📂 Revisa la carpeta: {dict_info['json_path']}")
            
            elif sub_choice == '5':
                print(f"\n📍 INFORMACIÓN DE PALABRAS ANCLA:")
                if analyzer.dictionaries:
                    print(f"   📂 Anclas de dimensión: {len(analyzer.dictionaries.dimension_anchors)}")
                    print(f"   📊 Anclas de métrica: {len(analyzer.dictionaries.metric_anchors)}")
                    
                    # Mostrar algunas anclas de ejemplo
                    print(f"\n🔍 EJEMPLOS DE ANCLAS DE DIMENSIÓN:")
                    for i, (anchor, synonyms) in enumerate(list(analyzer.dictionaries.dimension_anchors.items())[:3], 1):
                        print(f"   {i}. '{anchor}': {len(synonyms)} variaciones")
                        print(f"      Ejemplos: {synonyms[:5]}")
                    
                    print(f"\n🔍 EJEMPLOS DE ANCLAS DE MÉTRICA:")
                    for i, (anchor, synonyms) in enumerate(list(analyzer.dictionaries.metric_anchors.items())[:3], 1):
                        print(f"   {i}. '{anchor}': {len(synonyms)} variaciones")
                        print(f"      Ejemplos: {synonyms[:5]}")
                else:
                    print(f"   ❌ No hay información de anclas disponible")
            
            else:
                print(f"🔙 Volviendo al menú principal...")
        
        elif choice == '9':
            # NUEVA OPCIÓN: Probar reconocimiento de palabras ancla
            print(f"\n🧪 PROBANDO RECONOCIMIENTO DE PALABRAS ANCLA")
            print("="*50)
            
            print(f"1. Prueba automática con términos predefinidos")
            print(f"2. Prueba manual (ingresa tus propios términos)")
            print(f"3. Volver al menú principal")
            
            test_choice = input(f"Selecciona (1-3): ").strip()
            
            if test_choice == '1':
                print(f"\n🔄 Ejecutando prueba automática...")
                test_result = analyzer.test_anchor_recognition()
                
                print(f"\n📊 RESULTADOS DE LA PRUEBA:")
                print(f"   📂 Dimensiones reconocidas: {len(test_result['dimensions_found'])}")
                print(f"   📊 Métricas reconocidas: {len(test_result['metrics_found'])}")
                print(f"   ❓ No reconocidos: {len(test_result['unknown_found'])}")
                print(f"   🎯 Tasa de éxito: {test_result['success_rate']:.1f}%")
                
                if test_result['unknown_found']:
                    print(f"\n❓ TÉRMINOS NO RECONOCIDOS:")
                    for term in test_result['unknown_found']:
                        print(f"   • {term}")
            
            elif test_choice == '2':
                print(f"\n✏️ PRUEBA MANUAL:")
                print(f"Ingresa términos separados por comas (ej: tienda, ventas, profit)")
                user_terms = input(f"Términos: ").strip()
                
                if user_terms:
                    terms_list = [term.strip() for term in user_terms.split(',')]
                    test_result = analyzer.test_anchor_recognition(terms_list)
                    
                    print(f"\n📊 RESULTADOS:")
                    print(f"   🎯 Tasa de éxito: {test_result['success_rate']:.1f}%")
                else:
                    print(f"❌ No se ingresaron términos")
            
            else:
                print(f"🔙 Volviendo al menú principal...")
        
        # OPCIONES DE BASES DE DATOS
        elif choice == '10':
            # Subir tabla actual a MySQL
            success = analyzer.upload_current_table_to_mysql()
            if success:
                print(f"\n🎉 ¡Perfecto! Tu tabla está lista para consultas MySQL")
        
        elif choice == '11':
            # NUEVA: Subir tabla actual a ClickHouse
            success = analyzer.upload_current_table_to_clickhouse()
            if success:
                print(f"\n🎉 ¡Perfecto! Tu tabla está lista para consultas ClickHouse")
        
        elif choice == '12':
            # NUEVA: Ver estado de ambas bases de datos
            analyzer.show_database_status()
        
        elif choice == '13':
            # Ver tablas en MySQL
            analyzer.show_mysql_status()
        
        elif choice == '14':
            # NUEVA: Ver tablas en ClickHouse
            analyzer.show_clickhouse_tables()
        
        elif choice == '15':
            # Configurar MySQL
            analyzer.configure_mysql()
        
        elif choice == '16':
            # NUEVA: Configurar ClickHouse
            analyzer.configure_clickhouse()
        
        elif choice == '17':
            print(f"\n👋 ¡Hasta luego!")
            
            # Mostrar resumen final de ambas bases
            print(f"\n📊 RESUMEN FINAL:")
            if analyzer.mysql_integration.connection_available:
                mysql_tables = analyzer.mysql_integration.list_tables()
                if mysql_tables:
                    print(f"🗄️ MySQL: {len(mysql_tables)} tabla(s) disponible(s)")
                else:
                    print(f"🗄️ MySQL: No hay tablas aún")
            else:
                print(f"🗄️ MySQL: No disponible")
            
            if analyzer.clickhouse_integration.connection_available:
                ch_tables = analyzer.clickhouse_integration.list_tables()
                if ch_tables:
                    print(f"🏪 ClickHouse: {len(ch_tables)} tabla(s) disponible(s)")
                else:
                    print(f"🏪 ClickHouse: No hay tablas aún")
            else:
                print(f"🏪 ClickHouse: No disponible")
            
            print(f"🎯 Todas las tablas están listas para consultas desde tu ejecutor")
            break
        
        else:
            print(f"❌ Opción inválida")


if __name__ == "__main__":
    run_table_analyzer()