import re
import json
import os
import pandas
from typing import Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
from pathlib import Path




# -----------------------------------------------------------
# ---------------- PROBLEMIZADOR (NLP) ----------------------
# -----------------------------------------------------------

# codigo encargado de procesar el input del usuario, interpreta  la pregunta 
# escrita en lenguaje natural, funciona como un NLP (Natural Lenguaje Proccesing) 
# de tipo rule based ya que depende de reglas establecidas.



# ------ Rule Based NLP (Natural Lenguaje Proccesor ) -------

# 1 - ENTRADA Y VALIDACIÓN -----→ Recibe y valida input
# 2 - NORMALIZACIÓN ------------→ Limpia y estandariza texto
# 3 - TOKENIZACIÓN Y PATRONES --→ Divide y detecta patrones
# 4 - CLASIFICACIÓN ------------→ Identifica tipos semánticos
# 5 - ESTRUCTURA SEMÁNTICA -----→ Organiza componentes
# 6 - VALIDACIÓN Y PATRONES ----→ Verifica y clasifica intención
# 7 - GENERACIÓN SQL -----------→ Convierte a código ejecutable
# 8 - RESULTADO ----------------→ Formatea respuesta final



# -------------------------
# ------ CONEXIONES -------
# -------------------------


# ------ Conexion con diccionario de sinonimos -------
# importamos las clases que contienen los diccionarios necesarios

class ComponentType(Enum):
    DIMENSION = "dimension"
    METRIC = "metric" 
    OPERATION = "operation"
    COLUMN_VALUE = "column_value"
    TEMPORAL = "temporal"
    VALUE = "value"
    CONNECTOR = "connector"
    UNKNOWN = "unknown"


class OperationType(Enum):
    MAXIMUM = "máximo"
    MINIMUM = "mínimo"
    SUM = "suma"
    AVERAGE = "promedio"
    COUNT = "conteo"


class TemporalUnit(Enum):
    DAYS = "days"
    WEEKS = "weeks"
    MONTHS = "months"
    YEARS = "years"
    QUARTERS = "quarters"


class QueryPattern(Enum):
    """Patrones de Consulta Identificados"""
    UNKNOWN = "unknown"
    AGGREGATION = "aggregation"
    REFERENCED = "referenced"
    TOP_N = "top_n"
    TEMPORAL_CONDITIONAL = "temporal_conditional"
    LIST_ALL = "list_all"
    SHOW_ROWS = "show_rows"
    MULTI_DIMENSION = "multi_dimension"
    MULTI_METRIC = "multi_metric"  


class RankingDirection(Enum):
    TOP = "top"
    BOTTOM = "bottom"
    UNKNOWN = "unknown"


class RankingUnit(Enum):
    COUNT = "count"
    PERCENTAGE = "percentage"


class ExclusionType(Enum):
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"



# ----- Dataclasses necesarias -----

@dataclass
class QueryComponent:
    """Componente de consulta identificado"""
    text: str
    type: ComponentType
    confidence: float
    subtype: Optional[str] = None
    value: Optional[Union[str, int, float]] = None
    column_name: Optional[str] = None
    linguistic_info: Dict = field(default_factory=dict)


@dataclass
class ColumnValuePair:
    """Par columna-valor identificado"""
    column_name: str
    value: str
    confidence: float
    raw_text: str


@dataclass
class TemporalFilter:
    indicator: str
    quantity: Optional[int] = None
    unit: TemporalUnit = TemporalUnit.DAYS
    confidence: float = 0.8
    filter_type: str = "range"
    
    # Campos adicionales necesarios
    start_value: Optional[int] = None  # Para BETWEEN X AND Y
    end_value: Optional[int] = None    # Para BETWEEN X AND Y
    week_number: Optional[int] = None   # Para "week 5"
    year: Optional[int] = None         # Para especificar año


@dataclass
class CompoundCriteria:
    """Criterio individual dentro de una consulta compuesta"""
    operation: QueryComponent  # mas, menor, mayor, etc.
    metric: QueryComponent     # inventario, venta, etc.
    confidence: float
    raw_tokens: List[str]      # tokens originales que forman este criterio


@dataclass 
class RankingCriteria:
    """Criterios de ranking detectados"""
    direction: RankingDirection  # top/bottom
    unit: RankingUnit           # count/percentage  
    value: Union[int, float]    # 5, 10, 25.5
    metric: Optional[QueryComponent] = None      # ventas, margen, inventario
    operation: Optional[QueryComponent] = None   # máximo, suma, promedio
    confidence: float = 0.0
    raw_tokens: List[str] = field(default_factory=list)


@dataclass 
class ExclusionFilter:
    """Filtros de exclusión detectados"""
    exclusion_type: ExclusionType
    column_name: str
    value: str
    confidence: float
    raw_tokens: List[str] = field(default_factory=list)


@dataclass
class QueryStructure:
    
    """Estructura completa de la consulta - EXPANDIDA para rankings complejos"""
    main_dimension: Optional[QueryComponent]
    operations: List[QueryComponent]
    metrics: List[QueryComponent]
    column_conditions: List[ColumnValuePair]
    temporal_filters: List[TemporalFilter]
    values: List[QueryComponent]
    connectors: List[QueryComponent]
    unknown_tokens: List[QueryComponent]
    
    # Campos existentes para consultas compuestas
    compound_criteria: List[CompoundCriteria] = field(default_factory=list)
    is_compound_query: bool = False
    
    # Campos para rankings complejos
    ranking_criteria: Optional[RankingCriteria] = None
    exclusion_filters: List[ExclusionFilter] = field(default_factory=list)
    is_ranking_query: bool = False
    
    # Canpos para Multidimensiones
    main_dimensions: List[QueryComponent] = field(default_factory=list)  
    is_multi_dimension_query: bool = False 
    
    # Campos de control
    query_pattern: QueryPattern = QueryPattern.AGGREGATION
    reference_metric: Optional[QueryComponent] = None
    is_single_result: bool = False
    limit_value: Optional[int] = 1
    confidence_score: float = 0.0
    


    # ====================================
    # AGREGAR Intent semántico (pre-mapeo)
    # ====================================
    
    original_semantic_intent: str = 'DEFAULT'
    
    def get_complexity_level(self) -> str:
        """Calcula el nivel de complejidad"""
        complexity_score = 0
        
        complexity_score += len(self.column_conditions) * 2
        complexity_score += len(self.temporal_filters) * 3
        complexity_score += len(self.operations) * 1
        complexity_score += len(self.unknown_tokens) * -1
        
        # Complejidad por consultas compuestas
        if self.is_compound_query:
            complexity_score += len(self.compound_criteria) * 2
        
        # Complejidad por rankings
        if self.is_ranking_query:
            complexity_score += 3  # Base por ser ranking
            if self.ranking_criteria and self.ranking_criteria.unit == RankingUnit.PERCENTAGE:
                complexity_score += 2  # Extra por porcentajes
            complexity_score += len(self.exclusion_filters) * 2  # Por exclusiones
        
        # Agregar complejidad por patrón
        if self.query_pattern == QueryPattern.REFERENCED:
            complexity_score += 2
        elif self.query_pattern == QueryPattern.LIST_ALL:
            complexity_score += 1
            
        if complexity_score <= 0:
            return "simple"
        elif complexity_score <= 3:
            return "moderada"
        elif complexity_score <= 6:
            return "compleja"
        elif complexity_score <= 10:
            return "muy_compleja"
        else:
            return "extrema"



# ----- Dataclass para palabras desconocidas -----

@dataclass
class UnknownWord:
    """Información de palabra desconocida"""
    word: str
    position: int
    context_before: List[str]
    context_after: List[str]
    suggested_type: str
    confidence: float
    timestamp: str
    full_query: str


# ----- Descripcion Consultas fallidas -----

@dataclass
class QueryFailure:
    """Información de consulta fallida"""
    original_query: str
    unknown_words: List[UnknownWord]
    timestamp: str
    session_id: str
    user_feedback: Optional[str] = None
    resolved: bool = False


# ----- Condiciones temporales -----

@dataclass
class AdvancedTemporalInfo:
    """Información temporal avanzada - complementa TemporalFilter existente"""
    original_filter: TemporalFilter
    is_range_from: bool = False    # "desde semana 8"
    is_range_between: bool = False # "de semana 8 a 4"  
    is_range_to: bool = False      # "hasta semana 5"
    start_value: Optional[int] = None
    end_value: Optional[int] = None
    raw_tokens: List[str] = field(default_factory=list)
    
    def to_sql_condition(self) -> str:
        """Convierte a condición SQL avanzada"""
        if self.is_range_from:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week_number >= {self.start_value}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month_number >= {self.start_value}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day_number >= {self.start_value}"
                
        elif self.is_range_between:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week_number BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month_number BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day_number BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
                
        elif self.is_range_to:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week_number <= {self.end_value}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month_number <= {self.end_value}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day_number <= {self.end_value}"
        
        
        # Si no es ningún patrón avanzado, usar lógica original
        if self.original_filter.filter_type == "specific":
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week_number = {self.original_filter.quantity}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month_number = {self.original_filter.quantity}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day_number = {self.original_filter.quantity}"
        else:
            # Rangos tradicionales (ultimas X semanas)
            if self.original_filter.unit == TemporalUnit.WEEKS:
                days = self.original_filter.quantity * 7
                return f"fecha >= DATE('now', '-{days} days')"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"fecha >= DATE('now', '-{self.original_filter.quantity} days')"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"fecha >= DATE('now', '-{self.original_filter.quantity} months')"
        
        return "1=1"



@dataclass
class SuperlativePattern:
    """Patrón superlativo detectado (sold the most, had the least, etc.)"""
    question_word: str          # "which", "who", "what"
    target_dimension: str       # "account", "store", "product"
    action_verb: str           # "sold", "had", "generated"
    superlative_type: str      # "most", "least", "highest", "lowest"
    direction: str             # "DESC" o "ASC"
    implied_metric: Optional[str] = None  # "sales", "revenue" (inferido)
    confidence: float = 0.0
    raw_tokens: List[str] = field(default_factory=list)
    
    
@dataclass
class MultiMetricPattern:
    """📊 PATRÓN PARA MÚLTIPLES MÉTRICAS"""
    metrics: List[str]  # Lista de nombres de métricas
    operations: List[str]  # Lista de operaciones
    has_dimension: bool
    dimension: Optional[str]
    has_filters: bool
    filters: List[Dict]
    confidence: float
    raw_tokens: List[str]


@dataclass
class ThisWeekPattern:
    """Patrón 'this week' detectado - última semana disponible"""
    indicator_text: str        # "this week"
    position_start: int        # Posición donde empieza0p
    position_end: int          # Posición donde termina
    confidence: float = 0.0
    raw_tokens: List[str] = field(default_factory=list)



@dataclass
class YNColumnPattern:  # Renombrar de StockOutPattern
    """Patrón para columnas Y/N detectado"""
    column_name: str           # 'Stock_Out' o 'Dead_Inventory'
    value: str                 # 'Y' o 'N'
    negation_detected: bool    # Si hay "not" o "aren't"
    indicator_text: str        # "in stock out", "not in stock out"
    position_start: int        # Posición donde empieza
    position_end: int          # Posición donde termina
    confidence: float = 0.0
    raw_tokens: List[str] = field(default_factory=list)



# ----- Cargador de diccionarios desde JSON -----

class JSONDictionaryLoader:
    """Carga diccionarios desde archivos JSON manteniendo la misma interfaz"""

    
# --------------------------------------------------------------
# ---------------- ENCONTRAR DICCIONARIOS ----------------------
# --------------------------------------------------------------    
    
# ----- Encontrar diccionario OPERACIONAL -----
    def __init__(self, json_path: str = "diccionarios/simples/"):
        self.json_path = Path(json_path)
        
        
# ----- Encontrar diccionario TEMPORAL -----
        self.temporal_path = Path("diccionarios/temporales/diccionario_temporal_actual.json")  
        self.temporal_dictionary = {}  
        self.load_all_dictionaries()
    
    
    def load_all_dictionaries(self):
        """Carga todos los diccionarios desde JSON"""
        try:
            
            # Cargar archivos core
            self.operaciones = self._load_and_convert_operations()
            self.dimensiones = set(self._load_json_file("core/dimensions.json", []))
            self.metricas = set(self._load_json_file("core/metrics.json", []))
            self.columnas_conocidas = self._load_json_file("core/known_columns.json", {})
            self.valores_comunes = set(self._load_json_file("core/common_values.json", []))
            
            
# ---------------- CARGAR DICCIONARIOS LINGUISTIC EN ESPAÑOL E INGLÉS ----------------------
            
            # Cargar archivos linguistic - AMBOS IDIOMAS
            self.synonym_groups = self._load_json_file("linguistic/synonym_groups.json", {})

            # ESPAÑOL
            self.conectores_es = set(self._load_json_file("linguistic/es/connectors.json", []))
            self.numeros_palabras_es = self._load_json_file("linguistic/es/word_numbers.json", {})
            self.correcciones_tipograficas_es = self._load_json_file("linguistic/es/typo_corrections.json", {})

            # INGLÉS  
            self.conectores_en = set(self._load_json_file("linguistic/en/connectors.json", []))
            self.numeros_palabras_en = self._load_json_file("linguistic/en/word_numbers.json", {})
            self.correcciones_tipograficas_en = self._load_json_file("linguistic/en/typo_corrections.json", {})

            # Variable para idioma detectado
            self.detected_language = 'es'  # ESPAÑOL ES EL IDIOMA DEFAULT
            
            # 🔧 AGREGAR ESTA LÍNEA AQUÍ:
            self._create_language_aliases()
                
            # Cargar archivos temporal
            self.indicadores_temporales = self._load_json_file("temporal/temporal_indicators.json", {})
            self.unidades_tiempo = self._load_and_convert_temporal_units()
            
            # Construir frases compuestas
            self.frases_compuestas = {}
            self._build_compound_phrases()
            self._load_temporal_dictionary()
            
            print("✅ Diccionarios JSON cargados exitosamente")
            
            # 🚀 Construir índices optimizados
            self._build_optimized_indices()
            
        except Exception as e:
            print(f"❌ Error cargando diccionarios JSON: {e}")
            print("📚 Usando diccionarios básicos de fallback")
            self._load_fallback_dictionaries()
            
    
    def _load_json_file(self, relative_path: str, default_value):
        """Carga un archivo JSON específico"""
        file_path = self.json_path / relative_path
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Error cargando {relative_path}: {e}")
        return default_value
    
        
    def _load_and_convert_operations(self):
        """🔧 CARGA OPERACIONES CON PALABRAS ANCLA - VERSIÓN ACTUALIZADA"""
        operations_data = self._load_json_file("core/operations.json", {})
        
        # Mapeo de operaciones a enums
        string_to_enum = {
            "máximo": OperationType.MAXIMUM,
            "mínimo": OperationType.MINIMUM,
            "suma": OperationType.SUM,
            "promedio": OperationType.AVERAGE,
            "conteo": OperationType.COUNT
        }
        
        # Crear diccionario plano: palabra_ancla -> tipo_operacion
        operations_dict = {}
        
        for operation_key, anchor_words in operations_data.items():
            if isinstance(anchor_words, list):
                # Nuevo formato: lista de palabras ancla
                operation_enum = string_to_enum.get(operation_key, operation_key)
                
                for anchor_word in anchor_words:
                    # Normalizar la palabra ancla
                    normalized_anchor = anchor_word.lower().strip()
                    operations_dict[normalized_anchor] = operation_enum
                    
                    print(f"   📎 '{normalized_anchor}' → {operation_key}")
            else:
                # Formato anterior (compatibilidad hacia atrás)
                operations_dict[operation_key] = string_to_enum.get(anchor_words, anchor_words)
        
        print(f"✅ Operaciones cargadas: {len(operations_dict)} palabras ancla")
        return operations_dict

    
    def _load_and_convert_temporal_units(self):
        """Carga unidades temporales y convierte strings a enums"""
        units_data = self._load_json_file("temporal/temporal_units.json", {})
        units_dict = {}
        
        string_to_enum = {
            "days": TemporalUnit.DAYS,
            "weeks": TemporalUnit.WEEKS,
            "months": TemporalUnit.MONTHS,
            "years": TemporalUnit.YEARS,
            "quarters": TemporalUnit.QUARTERS
        }
        
        for key, value in units_data.items():
            units_dict[key] = string_to_enum.get(value, value)
        
        return units_dict
    
    
    def _build_compound_phrases(self):
        """Construye frases compuestas desde synonym_groups"""
        for normalized_key, synonyms in self.synonym_groups.items():
            for synonym in synonyms:
                self.frases_compuestas[synonym.lower()] = normalized_key
    
    
    # Métodos para mantener compatibilidad con el código original
    
    def get_component_type(self, word: str) -> ComponentType:
        """🚀 VERSIÓN OPTIMIZADA - BÚSQUEDA O(1)"""
        # Regla absoluta para mayúsculas (más rápido)
        if len(word) == 1 and word.isupper() and word.isalpha():
            return ComponentType.VALUE
        
        word_lower = word.lower()
        
        # Búsqueda directa en índice principal
        direct_match = self.word_to_type_index.get(word_lower)
        if direct_match:
            return direct_match
        
        # Búsqueda con prefijo de idioma
        lang_key = f"{self.detected_language}_{word_lower}"
        lang_match = self.word_to_type_index.get(lang_key)
        if lang_match:
            return lang_match
        
        # Búsqueda temporal optimizada
        if hasattr(self, 'temporal_lookup'):
            temporal_result = self.temporal_lookup.get(word_lower)
            if temporal_result:
                return ComponentType.VALUE
        
        # Fallback a búsqueda tradicional si no está indexado
        if word_lower in self.indicadores_temporales or word_lower in self.unidades_tiempo:
            return ComponentType.TEMPORAL
        elif word.isdigit() or word_lower in self._get_numeros_palabras_by_language():
            return ComponentType.VALUE
        else:
            return ComponentType.UNKNOWN
    

    def _get_conectores_by_language(self):
        """Retorna conectores según idioma detectado"""
        if self.detected_language == 'en':
            return self.conectores_en
        else:
            return self.conectores_es


    def _get_numeros_palabras_by_language(self):
        """Retorna números en palabras según idioma detectado"""
        if self.detected_language == 'en':
            return self.numeros_palabras_en
        else:
            return self.numeros_palabras_es    
        
        
    def get_operation_type(self, word: str):
        """🔍 OBTIENE EL TIPO DE OPERACIÓN - VERSIÓN MEJORADA"""
        word_normalized = word.lower().strip()
        
        # Búsqueda directa en el diccionario plano
        operation_type = self.operaciones.get(word_normalized, None)
        
        if operation_type:
            print(f"   ✅ Operación encontrada: '{word}' → {operation_type}")
            return operation_type
        
        print(f"   ❌ Operación no encontrada: '{word}'")
        return None
        
    
    def search_operation_in_phrase(self, phrase: str):
        """🔍 BUSCA OPERACIONES EN FRASES COMPLETAS"""
        phrase_normalized = phrase.lower().strip()
        
        # Buscar frases exactas primero (más específicas)
        exact_matches = []
        partial_matches = []
        
        for anchor_word, operation_type in self.operaciones.items():
            if len(anchor_word.split()) > 1:  # Es una frase
                if anchor_word in phrase_normalized:
                    exact_matches.append((anchor_word, operation_type))
            else:  # Es una palabra individual
                if anchor_word in phrase_normalized.split():
                    partial_matches.append((anchor_word, operation_type))
        
        # Priorizar frases exactas sobre palabras individuales
        if exact_matches:
            # Ordenar por longitud (frases más largas = más específicas)
            exact_matches.sort(key=lambda x: len(x[0]), reverse=True)
            best_match = exact_matches[0]
            print(f"   🎯 Frase encontrada: '{best_match[0]}' → {best_match[1]}")
            return best_match[1]
        
        elif partial_matches:
            best_match = partial_matches[0]
            print(f"   🎯 Palabra encontrada: '{best_match[0]}' → {best_match[1]}")
            return best_match[1]
        
        print(f"   ❌ No se encontraron operaciones en: '{phrase}'")
        return None
      


    def get_operation_suggestions(self, word: str, max_suggestions: int = 3):
        """💡 SUGERENCIAS DE OPERACIONES SIMILARES"""
        from difflib import get_close_matches
        
        word_normalized = word.lower().strip()
        all_anchor_words = list(self.operaciones.keys())
        
        # Buscar palabras similares
        suggestions = get_close_matches(
            word_normalized, 
            all_anchor_words, 
            n=max_suggestions, 
            cutoff=0.6
        )
        
        suggestion_results = []
        for suggestion in suggestions:
            operation_type = self.operaciones[suggestion]
            suggestion_results.append({
                'word': suggestion,
                'operation': operation_type,
                'confidence': 0.8  # Puedes calcular esto basado en similarity
            })
        
        return suggestion_results
    
    
    def get_temporal_unit(self, word: str):
        """Obtiene la unidad temporal"""
        return self.unidades_tiempo.get(word.lower(), None)
    
    
    def normalize_compound_phrases(self, text: str) -> str:
        """Normaliza frases compuestas"""
        text_lower = text.lower()
        sorted_phrases = sorted(self.frases_compuestas.keys(), key=len, reverse=True)
        
        for phrase in sorted_phrases:
            if phrase in text_lower:
                normalized = self.frases_compuestas[phrase]
                text_lower = text_lower.replace(phrase, normalized)
        
        return text_lower
    
    
    def correct_typo(self, word: str) -> str:
        """Corrige errores tipográficos según idioma detectado"""
        if self.detected_language == 'en':
            return self.correcciones_tipograficas_en.get(word.lower(), word)
        else:  # español
            return self.correcciones_tipograficas_es.get(word.lower(), word)
        
    
    def get_statistics(self) -> dict:
        """Obtiene estadísticas de los diccionarios"""
        return {
            'total_dimensiones': len(self.dimensiones),
            'total_operaciones': len(self.operaciones),
            'total_metricas': len(self.metricas),
            'source': 'JSON files'
        }


    def _load_temporal_dictionary(self):
        """Carga el diccionario temporal con datos reales de la tabla"""
        try:
            if self.temporal_path.exists():
                with open(self.temporal_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.temporal_dictionary = data.get('temporal_dictionary', {})
                    print(f"✅ Diccionario temporal cargado: {len(self.temporal_dictionary)} entradas")
            else:
                print(f"⚠️ Diccionario temporal no encontrado: {self.temporal_path}")
                self.temporal_dictionary = {}
        except Exception as e:
            print(f"❌ Error cargando diccionario temporal: {e}")
            self.temporal_dictionary = {}


    def search_in_temporal_dictionary(self, word: str) -> Optional[Dict]:
        """🚀 BÚSQUEDA TEMPORAL OPTIMIZADA O(1)"""
        if hasattr(self, 'temporal_lookup'):
            return self.temporal_lookup.get(word.lower())
        
        # Fallback al método original si no hay índice
        return self._search_temporal_original(word)


    def _search_temporal_original(self, word: str) -> Optional[Dict]:
        """🔄 MÉTODO ORIGINAL COMO FALLBACK"""
        word_lower = word.lower()
        
        # Buscar coincidencia exacta por clave
        if word_lower in self.temporal_dictionary:
            return self.temporal_dictionary[word_lower]
        
        # Buscar en variants de todas las entradas
        for key, entry in self.temporal_dictionary.items():
            variants = entry.get('variants', [])
            for variant in variants:
                if variant.lower() == word_lower:
                    return entry
        
        return None


    def get_temporal_component_type(self, word: str) -> Optional[ComponentType]:
        """
        Versión avanzada con logging para debugging
        """
        temporal_entry = self.search_in_temporal_dictionary(word)
        
        if temporal_entry:
            original_column_type = temporal_entry.get('column_type', 'unknown')
            
            # 🔧 FORZAR COMO VALUE
            print(f"   🗄️ TEMPORAL: '{word}' original_type='{original_column_type}' → FORZANDO como VALUE")
            
            return ComponentType.VALUE
        
        return None


# ---------------- DETECTAR IDIOMA POR MEDIO DE LA DETECCION EN TOKENS ----------------------

    def detect_language_from_tokens(self, tokens: List[str]) -> str:
        """🔧 DETECTOR DE IDIOMA - REGLA ABSOLUTA PARA MAYÚSCULAS"""
        
        # 🔧 REGLA ABSOLUTA: Filtrar todas las letras mayúsculas individuales
        filtered_tokens = []
        excluded_tokens = []
        
        for token in tokens:
            if len(token) == 1 and token.isupper() and token.isalpha():
                excluded_tokens.append(token)
            else:
                filtered_tokens.append(token)
        
        print(f"🔍 TOKENS ORIGINALES: {tokens}")
        print(f"🔒 DATOS EXCLUIDOS: {excluded_tokens} (letras mayúsculas = DATOS)")
        print(f"🔍 TOKENS PARA ANÁLISIS: {filtered_tokens}")
        
        if not filtered_tokens:
            print(f"⚠️ No hay tokens para analizar idioma, defaulteando a inglés")
            return 'en'
        
        english_score = 0
        spanish_score = 0
        
        for token in filtered_tokens:
            token_lower = token.lower()
            
            # 🔧 PALABRAS CLARAMENTE INGLESAS (alta prioridad)
            clear_english = {
                'with', 'and', 'more', 'most', 'top', 'best', 'worst', 'bottom',
                'having', 'where', 'between', 'from', 'excluding', 'except', 
                'without', 'store', 'sales', 'account', 'product', 'customer'
            }
            
            if token_lower in clear_english:
                english_score += 15
                print(f"   🇺🇸 Palabra claramente inglesa: '{token}' (+15)")
                continue
            
            # 🔧 PALABRAS CLARAMENTE ESPAÑOLAS (alta prioridad)
            clear_spanish = {
                'con', 'mas', 'mayor', 'menor', 'mejor', 'peor', 'primeros',
                'ultimos', 'entre', 'desde', 'hasta', 'suma', 'promedio',
                'tienda', 'ventas', 'cuenta', 'producto', 'cliente'
            }
            
            if token_lower in clear_spanish:
                spanish_score += 15
                print(f"   🇪🇸 Palabra claramente española: '{token}' (+15)")
                continue
            
            # 🔧 CONECTORES INGLESES
            if hasattr(self, 'conectores_en') and token_lower in self.conectores_en:
                english_score += 10
                print(f"   🇺🇸 Conector inglés: '{token}' (+10)")
            
            # 🔧 CONECTORES ESPAÑOLES
            elif token_lower in self.conectores:
                spanish_score += 10
                print(f"   🇪🇸 Conector español: '{token}' (+10)")
        
        # 🔧 RESOLVER EMPATES A FAVOR DEL INGLÉS si hay "with"
        if english_score == spanish_score:
            has_with = any(t.lower() == 'with' for t in filtered_tokens)
            if has_with:
                print(f"   🔧 EMPATE: Resolviendo a favor del inglés por 'with'")
                return 'en'
            
            # Si no hay with, usar heurísticas adicionales
            has_snake_case = any('_' in token for token in filtered_tokens)
            if has_snake_case:
                print(f"   🔧 EMPATE: Resolviendo a favor del inglés por snake_case")
                return 'en'
            
            print(f"   🔧 EMPATE: Defaulteando a inglés")
            return 'en'
        
        result = 'en' if english_score > spanish_score else 'es'
        print(f"   🎯 RESULTADO: {'INGLÉS' if result == 'en' else 'ESPAÑOL'} (score: {english_score} vs {spanish_score})")

        # ✅ ACTUALIZAR IDIOMA DETECTADO Y RECREAR ALIASES
        self.detected_language = result
        self._create_language_aliases()

        return result


    def _create_language_aliases(self):
        """
        🔧 Crear aliases para mantener compatibilidad con código existente
        Actualiza los aliases según el idioma detectado
        """
        if self.detected_language == 'en':
            self.conectores = self.conectores_en
            self.numeros_palabras = self.numeros_palabras_en
            self.correcciones_tipograficas = self.correcciones_tipograficas_en
            print(f"   🇺🇸 Aliases configurados para INGLÉS")
        else:
            self.conectores = self.conectores_es
            self.numeros_palabras = self.numeros_palabras_es
            self.correcciones_tipograficas = self.correcciones_tipograficas_es
            print(f"   🇪🇸 Aliases configurados para ESPAÑOL")
        
        # 🔧 Verificar que los aliases se crearon correctamente
        print(f"   ✅ Conectores activos: {len(self.conectores)} palabras")
        print(f"   ✅ Números activos: {len(self.numeros_palabras)} palabras") 
        print(f"   ✅ Correcciones activas: {len(self.correcciones_tipograficas)} palabras")


    def _detect_compound_phrases_dictionary_based(self, query: str) -> str:
        """
        🔍 DETECCIÓN AUTOMÁTICA CON DEBUGGING ESPECÍFICO
        """
        print(f"🔍 DETECTANDO FRASES COMPUESTAS (Dictionary-Based): '{query}'")
        
# PASO 1: Preservar mayúsculas individuales
        query_with_placeholders, preserved_tokens = self._preserve_single_uppercase_letters(query)
        print(f"🔍 DEBUG: Query con placeholders: '{query_with_placeholders}'")
        
        text_lower = query_with_placeholders.lower()
        print(f"🔍 DEBUG: Text en minúsculas: '{text_lower}'")
        
# PASO 2: Generar frases compuestas
        compound_phrases = self._generate_all_compound_phrases()
        print(f"   🔍 Generadas {len(compound_phrases)} frases compuestas automáticamente")
        
        changes_made = []
        
        
# PASO 3: Aplicar reemplazos con debugging detallado
        for space_version, underscore_version in sorted(compound_phrases.items(), key=lambda x: len(x[0]), reverse=True):
            if space_version in text_lower:
                print(f"   🎯 MATCH ENCONTRADO: '{space_version}' → '{underscore_version}'")
                text_lower = text_lower.replace(space_version, underscore_version)
                changes_made.append(f"AUTO: '{space_version}' → '{underscore_version}'")

# PASO 4: Restaurar mayúsculas con debugging
        print(f"🔍 DEBUG: Antes de restaurar mayúsculas: '{text_lower}'")
        final_text = self._restore_preserved_tokens_fixed(text_lower, preserved_tokens)
        print(f"🔍 DEBUG: Después de restaurar mayúsculas: '{final_text}'")
        
        return final_text


    def _restore_preserved_tokens_fixed(self, text: str, preserved_tokens: Dict[str, str]) -> str:
        """🔓 RESTAURAR TOKENS PRESERVADOS - VERSIÓN CORREGIDA"""
        final_text = text
        
        print(f"🔓 RESTAURANDO TOKENS:")
        print(f"   📥 Input: '{text}'")
        print(f"   🔑 Tokens preservados: {preserved_tokens}")
        
        for placeholder_lower, original_letter in preserved_tokens.items():
            if placeholder_lower in final_text:
                final_text = final_text.replace(placeholder_lower, original_letter)
                print(f"   ✅ Restaurado: '{placeholder_lower}' → '{original_letter}'")
            else:
                print(f"   ❌ NO encontrado: '{placeholder_lower}' en '{final_text}'")
                
                # 🆕 BÚSQUEDA MÁS ROBUSTA
                # Buscar partes del placeholder que puedan estar fragmentadas
                placeholder_parts = placeholder_lower.split('_')
                for i, part in enumerate(placeholder_parts):
                    if part in final_text and len(part) > 3:  # Solo partes significativas
                        print(f"   🔍 Encontrada parte del placeholder: '{part}'")
        
        print(f"   📤 Output: '{final_text}'")
        return final_text


# ---------------- DETECTAR DATOS COMPUESTOS POR 2 PALABRAS ----------------------

    def _generate_all_compound_phrases(self) -> Dict[str, str]:
        """🚀 VERSIÓN OPTIMIZADA CON CACHE"""
        if hasattr(self, '_compound_phrases_cache'):
            return self._compound_phrases_cache
        
        # Si no existe cache, construirlo
        self._build_compound_phrases_cache()
        return self._compound_phrases_cache


    def _add_automatic_variations(self, compound_phrases: Dict[str, str]):
        """
        🔄 AGREGAR VARIACIONES AUTOMÁTICAS
        Genera variaciones comunes de las frases encontradas
        """
        # Crear copias para iterar sin modificar el diccionario original
        original_phrases = compound_phrases.copy()
        
        for space_phrase, underscore_phrase in original_phrases.items():
            
# VARIACIÓN 1: Agregar plurales automáticamente
            if not space_phrase.endswith('s'):
                plural_space = f"{space_phrase}s"
                plural_underscore = f"{underscore_phrase}s" 
                
                # Solo agregar si el plural existe en los diccionarios
                if (plural_underscore in self.dimensiones or 
                    plural_underscore in self.metricas):
                    compound_phrases[plural_space] = plural_underscore
            
# VARIACIÓN 2: Manejar casos con mayúsculas mezcladas
            # Esto permite detectar "DEAD INVENTORY", "Dead Inventory", etc.
            words = space_phrase.split()
            if len(words) >= 2:
                # Generar todas las combinaciones de mayúsculas/minúsculas comunes
                variations = [
                    ' '.join(word.upper() for word in words),      # DEAD INVENTORY
                    ' '.join(word.capitalize() for word in words), # Dead Inventory
                    ' '.join([words[0].upper()] + words[1:]),      # DEAD inventory
                ]
                
                for variation in variations:
                    if variation != space_phrase:  # No duplicar la versión original
                        compound_phrases[variation.lower()] = underscore_phrase


# ----- FUNCION ESPECIAL PARA LA DETECCION DE VALORES LETRAS INDIVIDUALES MAYUSCULA ----
    
    def _preserve_single_uppercase_letters(self, query: str) -> Tuple[str, Dict[str, str]]:
        """🔒 PRESERVAR SOLO LETRAS MAYÚSCULAS INDIVIDUALES"""
        preserved_tokens = {}
        words = query.split()
        processed_query = query
        
        for i, word in enumerate(words):
            clean_word = re.sub(r'[^\w]', '', word)
            if len(clean_word) == 1 and clean_word.isupper() and clean_word.isalpha():
                placeholder = f"__UPPERCASE_{i}_{clean_word}__"
                preserved_tokens[placeholder.lower()] = clean_word
                processed_query = processed_query.replace(word, placeholder)
                print(f"   🔒 Preservando: '{clean_word}' → '{placeholder}'")
        
        return processed_query, preserved_tokens


    def _restore_preserved_tokens(self, text: str, preserved_tokens: Dict[str, str]) -> str:
        """🔓 RESTAURAR TOKENS PRESERVADOS"""
        final_text = text
        
        for placeholder_lower, original_letter in preserved_tokens.items():
            if placeholder_lower in final_text:
                final_text = final_text.replace(placeholder_lower, original_letter)
                print(f"   🔓 Restaurando: '{placeholder_lower}' → '{original_letter}'")
        
        return final_text


    def _process_synonym_groups(self, text_lower: str) -> List[str]:
        """📚 PROCESAR SYNONYM GROUPS EXISTENTES"""
        changes_made = []
        
        sorted_phrases = sorted(self.synonym_groups.keys(), key=len, reverse=True)
        for phrase in sorted_phrases:
            if phrase in text_lower:
                normalized = self.synonym_groups[phrase]
                text_lower = text_lower.replace(phrase, normalized)
                changes_made.append(f"SYNONYM: '{phrase}' → '{normalized}'")
        
        return changes_made



    def _build_optimized_indices(self):
        """🚀 CONSTRUIR ÍNDICES PARA BÚSQUEDAS RÁPIDAS"""
        import time
        start_time = time.time()
        
        print("🔧 Construyendo índices de búsqueda optimizada...")
        
        # ÍNDICE 1: Palabra -> Tipo de componente
        self.word_to_type_index = {}
        
        # Indexar dimensiones
        for dim in self.dimensiones:
            self.word_to_type_index[dim.lower()] = ComponentType.DIMENSION
        
        # Indexar métricas
        for metric in self.metricas:
            self.word_to_type_index[metric.lower()] = ComponentType.METRIC
        
        # Indexar operaciones
        for op_word in self.operaciones.keys():
            self.word_to_type_index[op_word.lower()] = ComponentType.OPERATION
        
        # Indexar conectores español
        for connector in self.conectores_es:
            self.word_to_type_index[f"es_{connector.lower()}"] = ComponentType.CONNECTOR
        
        # Indexar conectores inglés
        for connector in self.conectores_en:
            self.word_to_type_index[f"en_{connector.lower()}"] = ComponentType.CONNECTOR
        
        # ÍNDICE 2: Diccionario temporal optimizado
        self._build_temporal_index()
        
        # ÍNDICE 3: Frases compuestas en cache
        self._build_compound_phrases_cache()
        
        end_time = time.time()
        print(f"✅ Índices construidos: {len(self.word_to_type_index)} palabras en {end_time - start_time:.3f}s")


    def _build_temporal_index(self):
        """🚀 OPTIMIZAR DICCIONARIO TEMPORAL"""
        self.temporal_lookup = {}
        
        if not hasattr(self, 'temporal_dictionary') or not self.temporal_dictionary:
            print("⚠️ No hay diccionario temporal para indexar")
            return
        
        for key, entry in self.temporal_dictionary.items():
            # Indexar clave principal
            self.temporal_lookup[key.lower()] = entry
            
            # Indexar todas las variantes
            variants = entry.get('variants', [])
            for variant in variants:
                self.temporal_lookup[variant.lower()] = entry
        
        print(f"✅ Índice temporal: {len(self.temporal_lookup)} entradas")


    def _build_compound_phrases_cache(self):
        """🚀 CACHE DE FRASES COMPUESTAS"""
        self._compound_phrases_cache = {}
        
        # Solo procesar palabras con guiones bajos
        underscore_items = []
        
        for dim in self.dimensiones:
            if '_' in dim:
                underscore_items.append(dim)
        
        for metric in self.metricas:
            if '_' in metric:
                underscore_items.append(metric)
        
        for item in underscore_items:
            space_version = item.replace('_', ' ')
            self._compound_phrases_cache[space_version] = item
        
        print(f"✅ Frases compuestas en cache: {len(self._compound_phrases_cache)} entradas")


# --------------------------
# ------ DETECCIONES -------
# --------------------------
    
# primer filtro antes de generar los procesos, identificamos cuales son las palabaras que 
# contienen los inputs del usuario, si se detecta alguan palabra desconocida se convertirá
# en desconocida y se agregará en un diccionario json


# detector de palabras desconocidas
@dataclass
class UnknownWord:
    """Información de palabra desconocida"""
    word: str
    position: int
    context_before: List[str]
    context_after: List[str]
    suggested_type: str
    confidence: float
    timestamp: str
    full_query: str


# si se detecta alguna palabra que no se conoce la consulta fallará y no se forazará el proceso
@dataclass
class QueryFailure:
    """Información de consulta fallida"""
    original_query: str
    unknown_words: List[UnknownWord]
    timestamp: str
    session_id: str
    user_feedback: Optional[str] = None
    resolved: bool = False


@dataclass
class AdvancedTemporalInfo:
    """Información temporal avanzada - complementa TemporalFilter existente"""
    original_filter: TemporalFilter
    is_range_from: bool = False    # "desde semana 8"
    is_range_between: bool = False # "de semana 8 a 4"  
    is_range_to: bool = False      # "hasta semana 5"
    start_value: Optional[int] = None
    end_value: Optional[int] = None
    raw_tokens: List[str] = field(default_factory=list)
    
    
    
    def to_sql_condition(self) -> str:
        """Convierte a condición SQL avanzada - VERSIÓN CORREGIDA"""
        if self.is_range_from:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week >= {self.start_value}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month >= {self.start_value}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day >= {self.start_value}"
                
        elif self.is_range_between:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day BETWEEN {min(self.start_value, self.end_value)} AND {max(self.start_value, self.end_value)}"
                
        elif self.is_range_to:
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week <= {self.end_value}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month <= {self.end_value}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day <= {self.end_value}"
        
        # Si no es ningún patrón avanzado, usar lógica original
        if self.original_filter.filter_type == "specific":
            if self.original_filter.unit == TemporalUnit.WEEKS:
                return f"week = {self.original_filter.quantity}"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"month = {self.original_filter.quantity}"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"day = {self.original_filter.quantity}"
        else:
            # Rangos tradicionales (ultimas X semanas)
            if self.original_filter.unit == TemporalUnit.WEEKS:
                days = self.original_filter.quantity * 7
                return f"fecha >= DATE('now', '-{days} days')"
            elif self.original_filter.unit == TemporalUnit.DAYS:
                return f"fecha >= DATE('now', '-{self.original_filter.quantity} days')"
            elif self.original_filter.unit == TemporalUnit.MONTHS:
                return f"fecha >= DATE('now', '-{self.original_filter.quantity} months')"
        
        return "1=1"

        
    
# ----------------------------------------------------------------
# ------ DETECCION DE COMPLEJIDAD (evauluador inteligente) -------
# ----------------------------------------------------------------

# asignamos una categoria de complejidad evaluando la dificultad computacional y logica para procesar la consulta del usuario
# por medio de un sistema de puntuaciones basado en el tipo de datos que contiene la consulta.
# el valor de estas categorias se determina por medio del costo computacional que requiere completar la tarea.


# ------ Definir complejidad de consultas -------

    def get_complexity_level(self) -> str:
        """Calcula nivel de complejidad Y detecta errores críticos"""
        
        # Detección temprana de errores
        if len(self.unknown_tokens) > 0:
            return self._handle_unknown_tokens()
        
        # Procesamiento normal si no hay errores
        complexity_score = 0
        complexity_score += len(self.column_conditions) * 2
        complexity_score += len(self.temporal_filters) * 3
        complexity_score += len(self.operations) * 1
        
        if self.is_compound_query:
            complexity_score += len(self.compound_criteria) * 2
        
        if self.query_pattern == QueryPattern.REFERENCED:
            complexity_score += 2
        elif self.query_pattern == QueryPattern.LIST_ALL:
            complexity_score += 1
        
        # Clasificación normal
        if complexity_score <= 0:
            return "simple"
        elif complexity_score <= 3:
            return "moderada"
        elif complexity_score <= 6:
            return "compleja"
        else:
            return "muy_compleja"


    # ------ Detectar tokens invalidos -------

    def handle_unknown_tokens(self) -> dict:
        """Maneja tokens desconocidos - falla si encuentra alguno"""
        
        # Si no hay tokens desconocidos, todo bien
        if not self.unknown_tokens:
            return {
                'valid': True,
                'should_fail': False
            }
        
        # Si hay tokens desconocidos, fallar
        unknown_words = [token.text for token in self.unknown_tokens]
        
        print(f"🚨 TOKENS DESCONOCIDOS DETECTADOS:")
        print(f"   ❌ Palabras no reconocidas: {unknown_words}")
        
        return {
            'valid': False,
            'should_fail': True,
            'error': f'Palabras no reconocidas: {", ".join(unknown_words)}',
            'unknown_tokens': unknown_words
        }



# =====================================================
# ===== PROCESADORES ESPECIALIZADOS POR IDIOMA ========
# =====================================================


class BaseLanguageProcessor:
    """Clase base para procesadores de idioma específicos"""
    
    
    def __init__(self, dictionaries):
        self.dictionaries = dictionaries
    
    
    def detect_temporal_patterns(self, tokens: List[str]) -> List[TemporalFilter]:
        """Método abstracto - debe ser implementado por cada idioma"""
        raise NotImplementedError
    
    
    def detect_column_value_patterns(self, tokens: List[str], temporal_filters: List[TemporalFilter]) -> List[ColumnValuePair]:
        """Método abstracto - debe ser implementado por cada idioma"""
        raise NotImplementedError
    
    
    def detect_ranking_patterns(self, tokens: List[str], classified_components: Dict) -> Optional[RankingCriteria]:
        """Método abstracto - debe ser implementado por cada idioma"""
        raise NotImplementedError




# -----------------------------------------------------------------------------------------

# ================================================================================
# =========== PIPELINE DE PROCESAMIENTO COMPLETO (INGLÉS Y ESPAÑOL) ==============
# ================================================================================

# Se dividen los pipelines para las consultas en inglés y en español, los PIPELINES
# comparten funcionalidades pero cada uno se encuentra adaptado para las reglas de 
# su respectivo idioma a tratar, las funcionalidades que se agreguen a uno no afectaran
# al otro por lo que hay que crear funciones para cada uno.  

# -----------------------------------------------------------------------------------------




# =========================================================        
# =========== PIPELINE PARA CONSULTAS EN INGLÉS ===========
# =========================================================     


class EnglishNLPParser:
    """🇺🇸 PARSER NLP ESPECÍFICO PARA CONSULTAS EN INGLÉS"""
    

# ---------------- CORDINADOR DE CLASES INVOLUCRADAS EN EL PIPELINE ---------------------
    
    def __init__(self, dictionaries):
        """Inicializar parser inglés con diccionarios compartidos"""
        self.dictionaries = dictionaries
        self.pre_mapping_analyzer = PreMappingSemanticAnalyzer()
        
        # ✅ INICIALIZAR SQL MAPPER CON MANEJO DE ERRORES
        try:
            self.sql_mapper = SQLSchemaMapper()
            print("🇺🇸 English NLP Parser initialized with SQL Schema Mapper")
        except Exception as e:
            print(f"⚠️ Warning: SQLSchemaMapper failed to initialize: {e}")
            print("📋 Continuing without schema mapping (using conceptual SQL)")


    def format_temporal_dimension(self, dimension_name: str) -> str:
        """Formatea dimensiones temporales SOLO para SELECT"""
        if not dimension_name:
            return dimension_name
            
        temporal_dims = {
            'week': 'Week',
            'month': 'Month', 
            'year': 'Year',
            'day': 'Day',
            'quarter': 'Quarter'
        }
        
        # Obtener nombre normalizado
        normalized_name = temporal_dims.get(dimension_name.lower(), dimension_name)
        
        if dimension_name.lower() in temporal_dims:
            return f"CAST({normalized_name} AS CHAR) as {normalized_name}"
        
        return dimension_name



# ---------------- PROCESOS DEL PIPELINE PARA CONSUTLAS EN INGLÉS ---------------------
            
    def process_query(self, query: str, pre_normalized_query: str, preliminary_tokens: List[str]) -> Dict:
        """🇺🇸 PIPELINE PRINCIPAL PARA INGLÉS - VERSIÓN COMPLETA"""
        
        print(f"\n🇺🇸 PROCESSING ENGLISH QUERY: '{query}'")
        
    # STEP 1: NORMALIZATION (English-specific)
        normalized_query = self.normalize_english_query(pre_normalized_query)
        tokens = normalized_query.split()
        
        print(f"🧪 DEBUGGING TEMPORAL DICTIONARY:")
        if hasattr(self.dictionaries, 'temporal_dictionary'):
            # Test directo
            test_cases = ["palacio de hierro", "palaciodehierro", "palacio_de_hierro", "liverpool"]
            for test in test_cases:
                result = self.dictionaries.search_in_temporal_dictionary(test)
                if result:
                    print(f"   ✅ '{test}' → {result.get('original_value')} (column: {result.get('column_name')})")
                else:
                    print(f"   ❌ '{test}' → NOT FOUND")
        else:
            print(f"   ❌ No temporal dictionary loaded")
        
        print(f"🔤 English tokens: {tokens}")   
        
    # STEP 2: SEMANTIC ANALYSIS (reuse existing)
        original_intent = self.pre_mapping_analyzer.analyze_original_intent(tokens)
        print(f"🧠 English semantic intent: {original_intent}")
            
    # STEP 3: ENGLISH-SPECIFIC PATTERN DETECTION
        temporal_filters = self.detect_temporal_patterns_english(tokens)
        
        # Usar la nueva función con detección implícita
        column_value_pairs = self.detect_column_value_patterns_english_with_implicit(tokens, temporal_filters)  
                
    # STEP 3.5: TEMPORAL CONDITIONAL PATTERN DETECTION
        temporal_conditional_pattern = self.detect_temporal_conditional_pattern_english(tokens)
        print(f"🕐 DEBUG: temporal_conditional_pattern = {temporal_conditional_pattern is not None}")
                
    # STEP 3.6: LIST ALL PATTERN DETECTION
        list_all_pattern = self.detect_list_all_pattern_english(tokens)
        print(f"📋 DEBUG: list_all_pattern = {list_all_pattern is not None}")
        
    # STEP 3.7: SHOW ROWS PATTERN DETECTION
        show_rows_pattern = self.detect_show_rows_pattern_english(tokens)
        print(f"📊 DEBUG: show_rows_pattern = {show_rows_pattern is not None}")
                                
    # STEP 4: COMPONENT CLASSIFICATION (reuse with adaptations)
        classified_components = self.classify_components_english(tokens, column_value_pairs)
        
    # STEP 5: STRUCTURE BUILDING (usar el método existente)
        query_structure = self.build_english_structure(classified_components, column_value_pairs, temporal_filters, tokens, original_intent)
        
        print(f"🔧 DEBUG: Llegando a validación...")
        
    # STEP 6: VALIDATION (reuse existing)
        validation_result = self.validate_english_structure(query_structure)
        print(f"🔧 DEBUG: Validación result = {validation_result}")
        
        if not validation_result['valid']:
            return {
                'success': False,
                'error': validation_result['error'],
                'original_input': query,
                'suggestions': validation_result['suggestions'],
                'language': 'english'
            }
        
        
    # STEP 7: SQL GENERATION
        print(f"🔧 DEBUG: Antes de generar SQL...")
        print(f"🔧 DEBUG: hasattr list_all_pattern = {hasattr(query_structure, 'list_all_pattern')}")
        
        if hasattr(query_structure, 'list_all_pattern'):
            print(f"🔧 DEBUG: list_all_pattern value = {query_structure.list_all_pattern}")
        
        conceptual_sql = self.generate_optimized_sql_english(query_structure)
        
        print(f"🔧 DEBUG: SQL conceptual generado = '{conceptual_sql}'")

    # STEP 8: SQL SCHEMA NORMALIZATION (con fallback)
        if self.sql_mapper:
            try:
                normalized_sql = self.sql_mapper.normalize_sql(conceptual_sql)
                print(f"✅ Schema mapping applied successfully")
            except Exception as e:
                print(f"⚠️ Schema mapping failed: {e}")
                print(f"📋 Using conceptual SQL as fallback")
                normalized_sql = conceptual_sql
        else:
            print(f"📋 No schema mapper available, using conceptual SQL")
            normalized_sql = conceptual_sql  # LÍNEA CRÍTICA
            
        print(f"🔧 DEBUG: SQL final = '{normalized_sql}'")
            
        # STEP 9: FINAL RESULT
        try:
            # 🔧 DEBUG: Validar que query_structure existe y tiene los atributos necesarios
            print(f"🔧 DEBUG: Creating final result...")
            print(f"🔧 DEBUG: query_structure type: {type(query_structure)}")
            print(f"🔧 DEBUG: Has get_complexity_level: {hasattr(query_structure, 'get_complexity_level')}")
            
            # Calcular complejidad de forma segura
            try:
                complexity_level = query_structure.get_complexity_level() if hasattr(query_structure, 'get_complexity_level') else 'unknown'
            except Exception as e:
                print(f"⚠️ Error getting complexity level: {e}")
                complexity_level = 'unknown'
            
            # Calcular confianza de forma segura
            try:
                confidence_score = self.calculate_overall_confidence_english(query_structure)
            except Exception as e:
                print(f"⚠️ Error calculating confidence: {e}")
                confidence_score = 0.8
            
            # Generar estructura de diccionario de forma segura
            try:
                structure_dict = self.structure_to_dict_english(query_structure)
            except Exception as e:
                print(f"⚠️ Error converting structure to dict: {e}")
                structure_dict = {}
            
            # Generar estructura jerárquica de forma segura
            try:
                hierarchical_structure = self.generate_hierarchical_structure_english(query_structure)
            except Exception as e:
                print(f"⚠️ Error generating hierarchical structure: {e}")
                print(f"⚠️ Error details: {str(e)}")
                import traceback
                traceback.print_exc()
                hierarchical_structure = "error_generating_structure"
            
            # Generar interpretación de forma segura
            try:
                interpretation = self.generate_natural_interpretation_english(query_structure)
            except Exception as e:
                print(f"⚠️ Error generating interpretation: {e}")
                interpretation = "Query processed"
            
            # Obtener estadísticas de mapeo de forma segura
            try:
                mapping_stats = self.sql_mapper.get_mapping_statistics() if self.sql_mapper else {}
            except Exception as e:
                print(f"⚠️ Error getting mapping stats: {e}")
                mapping_stats = {}
            
            final_result = {
                'success': True,
                'language': 'english',
                'original_input': query,
                'normalized_query': normalized_query,
                'tokens': tokens,
                'conceptual_sql': conceptual_sql,
                'sql_query': normalized_sql,
                'complexity_level': complexity_level,
                'processing_method': 'english_pipeline_with_schema_mapping',
                'note': '🇺🇸 Processed with English-specific patterns + Schema Mapping',
                'query_structure': structure_dict,
                'hierarchical_structure': hierarchical_structure,
                'interpretation': interpretation,
                'confidence': confidence_score,
                'schema_mapping_stats': mapping_stats
            }
            
            print(f"🔧 DEBUG: Final result created successfully")
            print(f"🔧 DEBUG: SQL in result = '{final_result['sql_query']}'")
            
            return final_result
            
        except Exception as e:
            print(f"❌ ERROR creating final result: {e}")
            import traceback
            print(f"❌ FULL TRACEBACK:")
            traceback.print_exc()
            
            # Retornar resultado mínimo funcional
            return {
                'success': True,  # Cambiar a True ya que el SQL se generó correctamente
                'language': 'english',
                'original_input': query,
                'normalized_query': normalized_query,
                'tokens': tokens,
                'sql_query': normalized_sql,  # Lo importante es que el SQL esté disponible
                'conceptual_sql': conceptual_sql,
                'error_in_metadata': str(e),
                'note': 'SQL generated successfully but metadata generation had errors'
            }


    # Métodos auxiliares simples:
    
    def _structure_to_dict(self, structure: QueryStructure) -> Dict:
        """Convertidor simple de estructura"""
        return {
            'main_dimension': structure.main_dimension.text if structure.main_dimension else None,
            'operations': [op.text for op in structure.operations],
            'metrics': [m.text for m in structure.metrics],
            'query_pattern': structure.query_pattern.value if hasattr(structure, 'query_pattern') else 'unknown'
        }


    def _calculate_confidence(self, structure: QueryStructure) -> float:
        """Calculador simple de confianza"""
        return getattr(structure, 'confidence_score', 0.85)
            
    
# ---------------- PROCESOS DEL PIPELINE PARA NORMALIZACION DE CONSUTLAS EN INGLÉS ---------------------

    def normalize_english_query(self, query: str) -> str:
        """🇺🇸 NORMALIZACIÓN ESPECÍFICA PARA INGLÉS"""
        
        print(f"🔧 Normalizing English query: '{query}'")
        
# STEP 1: Apply English typo corrections
        words = query.split()
        corrected_words = []
        
        for word in words:
            # Preserve single uppercase letters
            if len(word) == 1 and word.isupper() and word.isalpha():
                corrected_words.append(word)
                print(f"🔒 Preserving uppercase: '{word}'")
            else:
                corrected_word = self.dictionaries.correct_typo(word)
                corrected_words.append(corrected_word)
                if corrected_word != word:
                    print(f"🔧 English correction: '{word}' → '{corrected_word}'")
        
        query = ' '.join(corrected_words)
        
# STEP 2: Clean special characters
        query = re.sub(r'[^\w\s_/^a-zA-Z0-9\s\.\,\-\(\)\/]', '', query)
    
# STEP 3: Normalize spaces
        query = re.sub(r'\s+', ' ', query).strip()
        
        print(f"✅ English normalized: '{query}'")
        return query
    
        
        

# =========== PROCESAMIENTO DE PATRONES TEMPORALES ===========
                    
    def detect_temporal_patterns_english(self, tokens: List[str]) -> List[TemporalFilter]:
        """🇺🇸 DETECCIÓN DE PATRONES TEMPORALES EN INGLÉS - VERSIÓN GENÉRICA MEJORADA"""
        
        print(f"⏰ DETECTING ENGLISH TEMPORAL PATTERNS:")
        print(f"   🔤 Full tokens list: {tokens}")  # VER TODOS LOS TOKENS
        print(f"   📏 Total tokens: {len(tokens)}")
        
        # Buscar si existe "between" en los tokens
        between_positions = [i for i, t in enumerate(tokens) if t.lower() == 'between']
        print(f"   🔍 'between' found at positions: {between_positions}")
        
        temporal_filters = []
        advanced_temporal_info = []
        processed_positions = set()
        i = 0
        
        while i < len(tokens):
            # Saltar posiciones ya procesadas
            if i in processed_positions:
                print(f"   ⏭️ Position {i} already processed, skipping")  # AGREGAR
                i += 1
                continue
                    
            token_lower = tokens[i].lower()
                            
            # PATTERN 1: between weeks/months/days X and Y - VERSIÓN MEJORADA PARA "IN BETWEEN"
            if token_lower == 'between' or (token_lower == 'in' and i + 1 < len(tokens) and tokens[i + 1].lower() == 'between'):
                print(f"🔧 DEBUG: Found 'between' pattern at position {i}")
                
                # Ajustar el índice inicial si hay "in" antes
                start_idx = i
                if token_lower == 'in':
                    start_idx = i + 1  # Saltar "in" para empezar desde "between"
                
                # Buscar componentes de forma FLEXIBLE
                components = {
                    'unit': None,
                    'unit_pos': -1,
                    'numbers': [],
                    'and_pos': -1
                }
                
                # Buscar en las próximas 10 posiciones desde "between"
                search_range = min(start_idx + 10, len(tokens))
                
                for j in range(start_idx + 1, search_range):
                    if j in processed_positions:
                        continue
                        
                    current_token = tokens[j].lower()
                    print(f"      🔍 Checking position {j}: '{tokens[j]}' (lower: '{current_token}')")
                    
                    # Buscar unidad temporal
                    if not components['unit'] and current_token in ['week', 'weeks', 'month', 'months', 'day', 'days', 'year', 'years']:
                        components['unit'] = current_token
                        components['unit_pos'] = j
                        print(f"   ✅ Found unit '{current_token}' at position {j}")
                    
                    # Buscar números
                    elif tokens[j].isdigit():
                        components['numbers'].append((j, int(tokens[j])))
                        print(f"   ✅ Found number '{tokens[j]}' at position {j}")
                    
                    # Buscar 'and'
                    elif current_token == 'and' and len(components['numbers']) == 1:
                        components['and_pos'] = j
                        print(f"   ✅ Found 'and' at position {j}")
                
                # Debug de componentes encontrados
                print(f"   📊 Components found:")
                print(f"      unit: {components['unit']} at pos {components['unit_pos']}")
                print(f"      numbers: {components['numbers']}")
                print(f"      and_pos: {components['and_pos']}")
                
                # Validar que tenemos todos los componentes necesarios
                if (components['unit'] and 
                    len(components['numbers']) >= 2 and 
                    components['and_pos'] > -1):
                    
                    # Extraer valores
                    num1_pos, week_num1 = components['numbers'][0]
                    num2_pos, week_num2 = components['numbers'][1]
                    
                    # Verificar que 'and' está entre los números
                    if num1_pos < components['and_pos'] < num2_pos:
                        current_year = 2025
                        
                        # Mapear unidad
                        unit_map = {
                            'week': TemporalUnit.WEEKS, 'weeks': TemporalUnit.WEEKS,
                            'month': TemporalUnit.MONTHS, 'months': TemporalUnit.MONTHS,
                            'day': TemporalUnit.DAYS, 'days': TemporalUnit.DAYS,
                            'year': TemporalUnit.YEARS, 'years': TemporalUnit.YEARS
                        }
                        temporal_unit = unit_map.get(components['unit'], TemporalUnit.WEEKS)
                        
                        # Convertir valores según la unidad
                        if temporal_unit == TemporalUnit.WEEKS:
                            start_value = int(f"{current_year}{str(week_num1).zfill(2)}") if week_num1 < 100 else week_num1
                            end_value = int(f"{current_year}{str(week_num2).zfill(2)}") if week_num2 < 100 else week_num2
                        else:
                            start_value = week_num1
                            end_value = week_num2
                        
                        # Crear TemporalFilter con valores correctos
                        temporal_filter = TemporalFilter(
                            indicator='between',
                            quantity=None,
                            unit=temporal_unit,
                            confidence=0.95,
                            filter_type='range_between',
                            start_value=start_value,
                            end_value=end_value
                        )
                        
                        # Crear información avanzada
                        advanced_info = AdvancedTemporalInfo(
                            original_filter=temporal_filter,
                            is_range_between=True,
                            start_value=start_value,
                            end_value=end_value,
                            raw_tokens=tokens[i:num2_pos + 1]
                        )
                        
                        temporal_filters.append(temporal_filter)
                        advanced_temporal_info.append(advanced_info)
                        
                        print(f"   ✅ BETWEEN PATTERN COMPLETE: {components['unit']} {week_num1} and {week_num2}")
                        print(f"      start_value={start_value}, end_value={end_value}")
                        
                        # Marcar todas las posiciones como procesadas (incluyendo "in" si existe)
                        for pos in range(i, num2_pos + 1):
                            processed_positions.add(pos)
                        
                        i = num2_pos + 1
                        continue
                else:
                    print(f"   ❌ Missing components for between pattern")
                    print(f"      unit: {components['unit']}, numbers: {len(components['numbers'])}, and: {components['and_pos']}")
                        
            # PATTERN 2: "from week/month X to Y" - VERSIÓN GENÉRICA
            elif token_lower == 'from':
                print(f"🔧 DEBUG: Found 'from' at position {i}")
                
                components = {
                    'unit': None,
                    'first_number': None,
                    'to_pos': -1,
                    'second_number': None
                }
                
                # Buscar en las próximas 8 posiciones
                search_range = min(i + 8, len(tokens))
                
                for j in range(i + 1, search_range):
                    if j in processed_positions:
                        continue
                        
                    current_token = tokens[j].lower()
                    
                    # Buscar unidad
                    if not components['unit'] and current_token in ['week', 'weeks', 'month', 'months', 'day', 'days', 'year', 'years']:
                        components['unit'] = current_token
                    
                    # Buscar primer número
                    elif not components['first_number'] and tokens[j].isdigit():
                        components['first_number'] = (j, int(tokens[j]))
                    
                    # Buscar 'to'
                    elif current_token == 'to' and components['first_number']:
                        components['to_pos'] = j
                    
                    # Buscar segundo número
                    elif components['to_pos'] > -1 and not components['second_number'] and tokens[j].isdigit():
                        components['second_number'] = (j, int(tokens[j]))
                        break
                
                # Validar componentes
                if all([components['unit'], components['first_number'], components['second_number'], components['to_pos'] > -1]):
                    num1_pos, num1 = components['first_number']
                    num2_pos, num2 = components['second_number']
                    
                    # Mapear unidad
                    unit_map = {
                        'week': TemporalUnit.WEEKS, 'weeks': TemporalUnit.WEEKS,
                        'month': TemporalUnit.MONTHS, 'months': TemporalUnit.MONTHS,
                        'day': TemporalUnit.DAYS, 'days': TemporalUnit.DAYS,
                        'year': TemporalUnit.YEARS, 'years': TemporalUnit.YEARS
                    }
                    temporal_unit = unit_map.get(components['unit'], TemporalUnit.WEEKS)
                    
                    # Convertir valores
                    current_year = 2025
                    if temporal_unit == TemporalUnit.WEEKS:
                        start_value = int(f"{current_year}{str(num1).zfill(2)}") if num1 < 100 else num1
                        end_value = int(f"{current_year}{str(num2).zfill(2)}") if num2 < 100 else num2
                    else:
                        start_value = num1
                        end_value = num2
                    
                    temporal_filter = TemporalFilter(
                        indicator="from_to",
                        quantity=None,
                        unit=temporal_unit,
                        confidence=0.95,
                        filter_type="range_between",
                        start_value=start_value,
                        end_value=end_value
                    )
                    
                    temporal_filters.append(temporal_filter)
                    
                    print(f"   ✅ FROM-TO PATTERN: from {components['unit']} {num1} to {num2}")
                    
                    # Marcar posiciones procesadas
                    for pos in range(i, num2_pos + 1):
                        processed_positions.add(pos)
                    
                    i = num2_pos + 1
                    continue
            
            # PATTERN 3: "last X weeks/months" - GENÉRICO
            elif token_lower == 'last':
                # Buscar número y unidad en las próximas posiciones
                number_found = None
                unit_found = None
                
                for j in range(i + 1, min(i + 4, len(tokens))):
                    if j in processed_positions:
                        continue
                        
                    # Buscar número
                    if not number_found and tokens[j].isdigit():
                        number_found = int(tokens[j])
                    
                    # Buscar unidad
                    elif tokens[j].lower() in ['weeks', 'months', 'days', 'years', 'week', 'month', 'day', 'year']:
                        unit_found = tokens[j].lower()
                        
                        if number_found and unit_found:
                            unit_map = {
                                'weeks': TemporalUnit.WEEKS, 'week': TemporalUnit.WEEKS,
                                'months': TemporalUnit.MONTHS, 'month': TemporalUnit.MONTHS,
                                'days': TemporalUnit.DAYS, 'day': TemporalUnit.DAYS,
                                'years': TemporalUnit.YEARS, 'year': TemporalUnit.YEARS
                            }
                            
                            temporal_filter = TemporalFilter(
                                indicator="last",
                                quantity=number_found,
                                unit=unit_map[unit_found],
                                confidence=0.95,
                                filter_type="range"
                            )
                            
                            temporal_filters.append(temporal_filter)
                            
                            print(f"   ✅ LAST PATTERN: last {number_found} {unit_found}")
                            
                            # Marcar posiciones procesadas
                            for pos in range(i, j + 1):
                                processed_positions.add(pos)
                            
                            i = j + 1
                            break
            
            # PATTERN 4: "week/month X" (específico) - GENÉRICO
            elif token_lower in ['week', 'weeks', 'month', 'months', 'day', 'days', 'year', 'years']:
                # Buscar número en las próximas 3 posiciones
                number_found = None
                number_pos = -1
                
                for j in range(i + 1, min(i + 4, len(tokens))):
                    if j in processed_positions:
                        continue
                        
                    if tokens[j].isdigit():
                        number_found = int(tokens[j])
                        number_pos = j
                        break
                
                if number_found:
                    # Normalizar unidad
                    unit_map = {
                        'week': TemporalUnit.WEEKS, 'weeks': TemporalUnit.WEEKS,
                        'month': TemporalUnit.MONTHS, 'months': TemporalUnit.MONTHS,
                        'day': TemporalUnit.DAYS, 'days': TemporalUnit.DAYS,
                        'year': TemporalUnit.YEARS, 'years': TemporalUnit.YEARS
                    }
                    temporal_unit = unit_map.get(token_lower, TemporalUnit.WEEKS)
                    
                    # Para semanas, convertir a formato YYYYWW
                    if temporal_unit == TemporalUnit.WEEKS and number_found < 100:
                        current_year = 2025
                        quantity = int(f"{current_year}{str(number_found).zfill(2)}")
                    else:
                        quantity = number_found
                    
                    temporal_filter = TemporalFilter(
                        indicator="specific",
                        quantity=quantity,
                        unit=temporal_unit,
                        confidence=0.90,
                        filter_type="specific"
                    )
                    
                    temporal_filters.append(temporal_filter)
                    
                    print(f"   ✅ SPECIFIC PATTERN: {token_lower} {number_found} → {quantity}")
                    
                    # Marcar posiciones procesadas
                    processed_positions.add(i)
                    processed_positions.add(number_pos)
                    
                    i = number_pos + 1
                    continue
            
            # PATTERN 5: "this week/month"
            elif token_lower == 'this':
                if i + 1 < len(tokens):
                    next_token = tokens[i + 1].lower()
                    if next_token in ['week', 'month', 'day', 'year']:
                        unit_map = {
                            'week': TemporalUnit.WEEKS,
                            'month': TemporalUnit.MONTHS,
                            'day': TemporalUnit.DAYS,
                            'year': TemporalUnit.YEARS
                        }
                        
                        temporal_filter = TemporalFilter(
                            indicator="this",
                            quantity=1,
                            unit=unit_map[next_token],
                            confidence=0.95,
                            filter_type="current_week" if next_token == 'week' else "current"
                        )
                        
                        temporal_filters.append(temporal_filter)
                        
                        print(f"   ✅ THIS PATTERN: this {next_token}")
                        
                        processed_positions.add(i)
                        processed_positions.add(i + 1)
                        
                        i += 2
                        continue
            
            # PATTERN 6: "since week X"
            elif token_lower == 'since':
                components = {'unit': None, 'number': None}
                
                for j in range(i + 1, min(i + 4, len(tokens))):
                    if tokens[j].lower() in ['week', 'weeks', 'month', 'months']:
                        components['unit'] = tokens[j].lower()
                    elif tokens[j].isdigit():
                        components['number'] = int(tokens[j])
                        
                    if components['unit'] and components['number']:
                        unit_map = {
                            'week': TemporalUnit.WEEKS, 'weeks': TemporalUnit.WEEKS,
                            'month': TemporalUnit.MONTHS, 'months': TemporalUnit.MONTHS
                        }
                        temporal_unit = unit_map.get(components['unit'], TemporalUnit.WEEKS)
                        
                        if temporal_unit == TemporalUnit.WEEKS:
                            current_year = 2025
                            since_value = int(f"{current_year}{str(components['number']).zfill(2)}") if components['number'] < 100 else components['number']
                        else:
                            since_value = components['number']
                        
                        temporal_filter = TemporalFilter(
                            indicator='since',
                            quantity=None,
                            unit=temporal_unit,
                            confidence=0.95,
                            filter_type='since',
                            start_value=since_value
                        )
                        
                        temporal_filters.append(temporal_filter)
                        print(f"   ✅ SINCE PATTERN: since {components['unit']} {components['number']}")
                        
                        for pos in range(i, j + 1):
                            processed_positions.add(pos)
                        
                        i = j + 1
                        break
            
            i += 1
        
        # Guardar información para uso posterior
        self.advanced_temporal_info = advanced_temporal_info
        self.temporal_processed_positions = processed_positions
        
        print(f"⏰ TOTAL ENGLISH TEMPORAL FILTERS: {len(temporal_filters)}")
        for tf in temporal_filters:
            print(f"   📅 Filter: {tf.filter_type} - {tf.indicator}")
            if hasattr(tf, 'start_value'):
                print(f"      start_value: {tf.start_value}")
            if hasattr(tf, 'end_value'):
                print(f"      end_value: {tf.end_value}")
        
        return temporal_filters


# =========== PROCESAMIENTO DE PATRONES COLUMNA VALOR EN INGLÉS ===========
            
    def detect_column_value_patterns_english(self, tokens: List[str], temporal_filters: List[TemporalFilter]) -> List[ColumnValuePair]:
        """🇺🇸 DETECCIÓN CON CONTROL DE DUPLICADOS CORREGIDO"""
        
        print(f"🎯 DETECTING ENGLISH COLUMN-VALUE PATTERNS:")
        
        # VERIFICAR DICCIONARIO TEMPORAL
        if hasattr(self.dictionaries, 'temporal_dictionary'):
            temp_dict_size = len(self.dictionaries.temporal_dictionary)
            print(f"📚 Temporal dictionary loaded: {temp_dict_size} entries")
            test_search = self.dictionaries.search_in_temporal_dictionary("palacio de hierro")
            if test_search:
                print(f"✅ Test: 'palacio de hierro' → {test_search.get('original_value')}")
        
        column_value_pairs = []
        
        # Identificar columnas temporales
        temporal_columns = set()
        for tf in temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['week', 'weeks', 'semana', 'semanas'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['month', 'months', 'mes', 'meses'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['day', 'days', 'dia', 'dias'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['year', 'years', 'año', 'años'])
        
        print(f"⏰ Columnas temporales a excluir: {temporal_columns}")
        
    # CONTROL ESTRICTO DE DUPLICADOS
        processed_positions = set()
        created_filters = set()  # Para evitar filtros duplicados por contenido
        
    # PASO 1: DETECTAR PATRONES DIRECTOS CON DICCIONARIO TEMPORAL (PRIORIDAD MÁXIMA)
        for i in range(len(tokens) - 1):
            if i in processed_positions:
                continue
                
            current_token = tokens[i]
            
            # Verificar si es columna potencial
            column_info = self._identify_potential_column_english(current_token)
            if not column_info['is_column'] or column_info['normalized_name'] in temporal_columns:
                continue
            
            print(f"🔍 Testing TEMPORAL DICT pattern: '{current_token}' + [value from dictionary]")
            
            dict_result = self._extract_value_from_temporal_dict(tokens, i + 1, column_info['normalized_name'])
            
            if dict_result:
                # 🆕 CREAR CLAVE ÚNICA PARA EVITAR DUPLICADOS
                filter_key = f"{column_info['normalized_name']}={dict_result['normalized_value']}"
                
                if filter_key not in created_filters:
                    column_value_pairs.append(ColumnValuePair(
                        column_name=column_info['normalized_name'],
                        value=dict_result['normalized_value'],
                        confidence=dict_result['confidence'],
                        raw_text=f"{current_token} {dict_result['raw_text']}"
                    ))
                    
                    created_filters.add(filter_key)
                    print(f"✅ TEMPORAL DICT SUCCESS: {current_token} = '{dict_result['normalized_value']}'")
                    
                    # 🚨 MARCAR TODAS LAS POSICIONES COMO PROCESADAS
                    for pos in range(i, i + 1 + dict_result['tokens_consumed']):
                        processed_positions.add(pos)
                    
                    print(f"🔒 POSITIONS LOCKED: {list(range(i, i + 1 + dict_result['tokens_consumed']))}")
                else:
                    print(f"🔄 TEMPORAL DICT DUPLICATE AVOIDED: {filter_key}")
        
    # PASO 2: PATRONES CON PREPOSICIONES (respetando posiciones procesadas)
        i = 0
        while i < len(tokens) - 2:
            # 🚨 VERIFICAR SI LA POSICIÓN YA FUE PROCESADA
            if i in processed_positions:
                print(f"⏭️ SKIPPING position {i} (already processed)")
                i += 1
                continue
            
            # PATTERN ESPECIAL: "total [word] of [metric]" → Agregación global
            if (i < len(tokens) - 3 and
                tokens[i].lower() == 'total' and
                tokens[i + 2].lower() == 'of' and
                self._is_potential_metric_english(tokens[i + 3])):
                
                print(f"   🌐 ENGLISH AGGREGATION PATTERN: total {tokens[i + 1]} of {tokens[i + 3]} (no filter created)")
                i += 4
                continue
            
        # PATTERN 1: [preposition] [column] [value]
            if i < len(tokens) - 2:
                # 🚨 VERIFICAR QUE NINGUNA DE LAS 3 POSICIONES ESTÉ PROCESADA
                positions_needed = {i, i + 1, i + 2}
                if positions_needed.intersection(processed_positions):
                    print(f"⏭️ PREPOSITION PATTERN: positions {positions_needed} overlap with processed {processed_positions}")
                    i += 1
                    continue
                
                pattern_result = self._detect_preposition_column_value_pattern_english(
                    tokens, i, temporal_columns, processed_positions
                )
                
                if pattern_result:
                    # 🆕 VERIFICAR DUPLICADOS POR CONTENIDO
                    filter_key = f"{pattern_result['pair'].column_name}={pattern_result['pair'].value}"
                    
                    if filter_key not in created_filters:
                        column_value_pairs.append(pattern_result['pair'])
                        created_filters.add(filter_key)
                        print(f"✅ ENGLISH FILTER CREATED (preposition): {pattern_result['raw_text']}")
                        
                        # Marcar posiciones como procesadas
                        for pos in range(i, i + pattern_result['tokens_consumed']):
                            processed_positions.add(pos)
                    else:
                        print(f"🔄 PREPOSITION DUPLICATE AVOIDED: {filter_key}")
                    
                    i += pattern_result['tokens_consumed']
                    continue
            
            i += 1
        
        print(f"🎯 Total English filters detected: {len(column_value_pairs)}")
        print(f"🔄 Unique filters created: {created_filters}")
        print(f"🔒 Final processed positions: {sorted(processed_positions)}")
        
        return column_value_pairs
                        

    # =====================================================================
    # =========== DETECTOR DE PATRONES DE VALORES IMPLÍCITOS =============
    # =====================================================================

    def detect_implicit_value_patterns_english(self, tokens: List[str]) -> Tuple[List[ColumnValuePair], List[int]]:
        implicit_filters = []
        processed_positions = set()
        
        
        # STEP 1: Buscar combinaciones de múltiples tokens PRIMERO
        for start_idx in range(len(tokens)):
            if start_idx in processed_positions:
                continue
                
            # 🔧 CAMBIO CRÍTICO: Empezar desde las combinaciones más largas
            for length in range(min(15, len(tokens) - start_idx), 0, -1):  # De más largo a más corto
                end_idx = start_idx + length
                
                # Verificar que no haya posiciones ya procesadas en este rango
                if any(pos in processed_positions for pos in range(start_idx, end_idx)):
                    continue
                
                candidate_tokens = tokens[start_idx:end_idx]
                
                print(f"      🔍 Testing combination: {candidate_tokens} (positions {start_idx}-{end_idx-1})")
                
                # Buscar en diccionario temporal
                implicit_result = self._search_implicit_value_in_temporal_dict(candidate_tokens)
                
                if implicit_result:
                    # Determinar el contexto de la consulta para validar si tiene sentido
                    context_info = self._analyze_query_context_for_implicit_value(
                        tokens, start_idx, end_idx, implicit_result
                    )
                    
                    if context_info['is_valid_context']:
                        # Crear ColumnValuePair automáticamente
                        column_value_pair = ColumnValuePair(
                            column_name=implicit_result['column_name'].lower(),
                            value=implicit_result['original_value'],
                            confidence=implicit_result['confidence'] * context_info['context_confidence'],
                            raw_text=' '.join(candidate_tokens)
                        )
                        
                        implicit_filters.append(column_value_pair)
                        
                        # Marcar TODAS las posiciones como procesadas
                        for pos in range(start_idx, end_idx):
                            processed_positions.add(pos)
                        
                        print(f"      ✅ IMPLICIT VALUE DETECTED:")
                        print(f"         📍 Value: '{implicit_result['original_value']}'")
                        print(f"         📋 Column: {implicit_result['column_name']}")
                        print(f"         🎯 Context: {context_info['context_type']}")
                        print(f"         ⭐ Confidence: {column_value_pair.confidence:.2f}")
                        print(f"         🔒 Positions processed: {list(range(start_idx, end_idx))}")
                        
                        # 🔧 IMPORTANTE: Salir del loop de longitud para esta posición
                        break  # Procesar solo la combinación más larga encontrada
                    else:
                        print(f"      ❌ Invalid context for implicit value: {context_info['reason']}")
        
        # 🔧 CAMBIO: Ya NO buscar tokens individuales por separado
        # porque ya están incluidos en el loop anterior (cuando length=1)
        
        print(f"🔍 TOTAL IMPLICIT VALUES DETECTED: {len(implicit_filters)}")
        print(f"🔒 TOTAL POSITIONS PROCESSED: {sorted(processed_positions)}")
        
        return implicit_filters, list(processed_positions)


    def _search_implicit_value_in_temporal_dict(self, candidate_tokens: List[str]) -> Optional[Dict]:
        """
        🗄️ BÚSQUEDA DE VALOR EN DICCIONARIO TEMPORAL
        Prueba múltiples variantes de los tokens candidatos
        """
        
        # Generar variantes para buscar
        test_variants = self._generate_search_variants(candidate_tokens)
        
        for variant in test_variants:
            print(f"         🔍 Testing variant: '{variant}'")
            
            temporal_entry = self.dictionaries.search_in_temporal_dictionary(variant)
            
            if temporal_entry:
                print(f"         ✅ MATCH FOUND: '{variant}' → {temporal_entry}")
                return temporal_entry
        
        return None


    def _generate_search_variants(self, tokens: List[str]) -> List[str]:
        """
        🔧 GENERADOR DE VARIANTES DE BÚSQUEDA
        Crea todas las combinaciones posibles para buscar en el diccionario
        """
        
        base_text = ' '.join(tokens)
        
        variants = [
            base_text.lower(),                          # "palacio de hierro"
            base_text.upper(),                          # "PALACIO DE HIERRO"
            ''.join(tokens).lower(),                    # "palaciodehierro"
            '('.join(tokens).lower(),
            ''.join(tokens).upper(),                    # "PALACIODEHIERRO"
            '_'.join(tokens).lower(),                   # "palacio_de_hierro"
            '_'.join(tokens).upper(),                   # "PALACIO_DE_HIERRO"
            base_text.title(),                          # "Palacio De Hierro"
        ]
        
        # Para tokens individuales, agregar variantes adicionales
        if len(tokens) == 1:
            token = tokens[0]
            variants.extend([
                token,                                  # Original
                token.lower(),                          # lowercase
                token.upper(),                          # UPPERCASE
                token.capitalize()                      # Capitalized
            ])
        
        # Remover duplicados manteniendo orden
        seen = set()
        unique_variants = []
        for variant in variants:
            if variant not in seen:
                seen.add(variant)
                unique_variants.append(variant)
        
        return unique_variants


    def _analyze_query_context_for_implicit_value(self, tokens: List[str], start_idx: int, end_idx: int, implicit_result: Dict) -> Dict:
        """
        🧠 ANALIZADOR DE CONTEXTO PARA VALORES IMPLÍCITOS
        Determina si el valor encontrado tiene sentido en el contexto de la consulta
        """
        
        print(f"         🧠 Analyzing context for implicit value...")
        
        # Tokens antes y después del valor encontrado
        before_tokens = tokens[:start_idx]
        after_tokens = tokens[end_idx:]
        value_column = implicit_result['column_name'].lower()
        
        print(f"            📍 Before: {before_tokens}")
        print(f"            📍 After: {after_tokens}")
        print(f"            📋 Value column: {value_column}")
        
        context_patterns = []
        context_confidence = 0.7  # Base confidence
        
    # PATTERN 1: "how many X does [VALUE] have" → COUNT query
        if self._matches_count_pattern(before_tokens + after_tokens):
            context_patterns.append('COUNT_PATTERN')
            context_confidence += 0.2
            print(f"            ✅ COUNT pattern detected")
        
    # PATTERN 2: "top N X of [VALUE]" → RANKING query  
        if self._matches_ranking_pattern(before_tokens + after_tokens):
            context_patterns.append('RANKING_PATTERN')
            context_confidence += 0.2
            print(f"            ✅ RANKING pattern detected")
        
    # PATTERN 3: "which X of [VALUE]" → SELECTION query
        if self._matches_selection_pattern(before_tokens + after_tokens):
            context_patterns.append('SELECTION_PATTERN')
            context_confidence += 0.2
            print(f"            ✅ SELECTION pattern detected")
        
    # PATTERN 4: "[VALUE] sales/revenue/data" → METRIC query
        if self._matches_metric_pattern(before_tokens + after_tokens):
            context_patterns.append('METRIC_PATTERN')
            context_confidence += 0.15
            print(f"            ✅ METRIC pattern detected")
        
    # PATTERN 5: Contiene palabras interrogativas
        if self._contains_question_words(before_tokens + after_tokens):
            context_patterns.append('QUESTION_PATTERN')
            context_confidence += 0.1
            print(f"            ✅ QUESTION pattern detected")


    # VALIDATION: Debe tener al menos un patrón válido O ser "X of Y"
        if not context_patterns:
            # Verificar si es patrón "X of Y"
            if (len(before_tokens) >= 2 and 
                before_tokens[-1].lower() == 'of', 'in'):
                return {
                    'is_valid_context': True,
                    'context_type': 'X_OF_Y_PATTERN',
                    'context_confidence': 0.85,
                    'patterns_detected': ['X_OF_Y']
                }
            
            # Si no, rechazar
            return {
                'is_valid_context': False,
                'context_type': 'UNKNOWN',
                'context_confidence': 0.0,
                'reason': 'No recognizable query patterns found'
            }
                
    # VALIDATION: Verificar coherencia con tipo de columna
        column_type = implicit_result.get('column_type', 'unknown')
        if not self._is_coherent_with_column_type(context_patterns, column_type):
            return {
                'is_valid_context': False,
                'context_type': 'INCOHERENT',
                'context_confidence': 0.0,
                'reason': f'Context patterns {context_patterns} not coherent with column type {column_type}'
            }
        
        return {
            'is_valid_context': True,
            'context_type': '_'.join(context_patterns),
            'context_confidence': min(1.0, context_confidence),
            'patterns_detected': context_patterns
        }


    def _matches_count_pattern(self, surrounding_tokens: List[str]) -> bool:
        """🔢 DETECTOR DE PATRÓN DE CONTEO"""
        text = ' '.join(surrounding_tokens).lower()
        
        count_indicators = [
            'how many', 'how much', 'total number', 'number of', 'count of',
            'total', 'sum of', 'amount of', 'quantity of'
        ]
        
        for indicator in count_indicators:
            if indicator in text:
                return True
        
        # Buscar palabras individuales también
        count_words = {'many', 'much', 'total', 'count', 'number', 'quantity', 'amount'}
        return any(word in [t.lower() for t in surrounding_tokens] for word in count_words)


    def _matches_ranking_pattern(self, surrounding_tokens: List[str]) -> bool:
        """🏆 DETECTOR DE PATRÓN DE RANKING"""
        
        ranking_words = {
            'top', 'best', 'highest', 'maximum', 'first', 'greatest', 'most',
            'worst', 'lowest', 'minimum', 'last', 'least', 'bottom'
        }
        
        # Buscar indicadores de ranking
        for token in surrounding_tokens:
            if token.lower() in ranking_words:
                return True
        
        # Buscar números que indican ranking (top 5, best 10, etc.)
        for i, token in enumerate(surrounding_tokens):
            if token.lower() in ranking_words and i + 1 < len(surrounding_tokens):
                next_token = surrounding_tokens[i + 1]
                if next_token.isdigit() or next_token.endswith('%'):
                    return True
        
        return False


    def _matches_selection_pattern(self, surrounding_tokens: List[str]) -> bool:
        """🎯 DETECTOR DE PATRÓN DE SELECCIÓN"""
        
        selection_words = {
            'which', 'what', 'who', 'where', 'show', 'list', 'display', 
            'get', 'find', 'search', 'lookup', 'identify'
        }
        
        return any(token.lower() in selection_words for token in surrounding_tokens)


    def _matches_metric_pattern(self, surrounding_tokens: List[str]) -> bool:
        """📊 DETECTOR DE PATRÓN DE MÉTRICAS"""
        
        metric_words = {
            'sales', 'revenue', 'profit', 'margin', 'cost', 'price',
            'inventory', 'stock', 'volume', 'units', 'dollars', 'data'
        }
        
        return any(token.lower() in metric_words for token in surrounding_tokens)


    def _contains_question_words(self, surrounding_tokens: List[str]) -> bool:
        """❓ DETECTOR DE PALABRAS INTERROGATIVAS"""
        
        question_words = {
            'how', 'what', 'which', 'who', 'where', 'when', 'why',
            'does', 'do', 'is', 'are', 'can', 'will', 'would'
        }
        
        return any(token.lower() in question_words for token in surrounding_tokens)


    def _is_coherent_with_column_type(self, context_patterns: List[str], column_type: str) -> bool:
        """🔍 VERIFICADOR DE COHERENCIA CON TIPO DE COLUMNA"""
        
        # Por ahora, permitir todos los patrones para todos los tipos de columna
        # Se puede refinar más adelante con reglas específicas
        
        # Ejemplo de reglas futuras:
        # if column_type == 'dimension' and 'METRIC_PATTERN' in context_patterns:
        #     return False  # Una dimensión no debería ser tratada como métrica
        
        return True


    # =====================================================================
    # =========== INTEGRACIÓN CON PIPELINE EXISTENTE ====================
    # =====================================================================

    def detect_column_value_patterns_english_with_implicit(self, tokens: List[str], temporal_filters: List[TemporalFilter]) -> List[ColumnValuePair]:
        """
        🎯 VERSIÓN MEJORADA QUE INCLUYE: implícitos + especiales (this week, enhanced stock out)
        MANTIENE EL NOMBRE ORIGINAL DEL MÉTODO
        """
        
        print(f"🎯 DETECTING ENGLISH COLUMN-VALUE PATTERNS (WITH ENHANCED STOCK OUT - ORIGINAL METHOD):")
                
        all_column_value_pairs = []
        all_processed_positions = set()

        # 🆕 PASO 0.1: Identificar posiciones que son GROUP BY (no filtros)
        groupby_positions = set()
        
        for i, token in enumerate(tokens):
            if token.lower() == 'by' and i + 1 < len(tokens):
                next_token = tokens[i + 1]
                # Usar método existente que consulta diccionarios
                column_info = self._identify_potential_column_english(next_token)
                
                if column_info['is_column'] and column_info['type'] == 'dimension':
                    groupby_positions.add(i + 1)  # Marcar posición de la dimensión
                    print(f"   📍 Excluding position {i + 1} ('{next_token}') from implicit values - it's GROUP BY")

        # STEP 1: DETECTAR PATRONES ESPECIALES PRIMERO (mayor prioridad)

        # 1.1: Detectar patrón THIS WEEK
        this_week_pattern = self.detect_this_week_pattern_english(tokens)
        if this_week_pattern:
            # Agregar como filtro temporal especial
            special_temporal_filter = TemporalFilter(
                indicator="this_week",
                quantity=1,
                unit=TemporalUnit.WEEKS,
                confidence=this_week_pattern.confidence,
                filter_type="current_week"
            )
            temporal_filters.append(special_temporal_filter)
            
            # Marcar posiciones como procesadas
            for pos in range(this_week_pattern.position_start, this_week_pattern.position_end + 1):
                all_processed_positions.add(pos)
            
            print(f"   📅 THIS WEEK pattern processed - added to temporal filters")

        # 1.2: Detectar patrón ENHANCED YN PATTERNS
        print(f"   🔧 DEBUG: Llamando a detect_enhanced_stock_out_pattern_english...")
        enhanced_yn_pattern = self.detect_enhanced_yn_column_pattern_english(tokens)

        if enhanced_yn_pattern:
            # Crear filtro de columna genérico
            yn_pair = ColumnValuePair(
                column_name=enhanced_yn_pattern.column_name,  # Usa la columna detectada
                value=enhanced_yn_pattern.value,               # Usa el valor Y/N
                confidence=enhanced_yn_pattern.confidence,
                raw_text=enhanced_yn_pattern.indicator_text
            )
            
            all_column_value_pairs.append(yn_pair)
            
            print(f"   📦 ENHANCED Y/N pattern processed: {enhanced_yn_pattern.column_name} = '{enhanced_yn_pattern.value}'")

        
        # STEP 1.5: DETECTAR VALORES IMPLÍCITOS (lógica existente)
        implicit_filters, implicit_positions = self.detect_implicit_value_patterns_english(tokens)
        
        # 🆕 Filtrar implicit_filters que estén en groupby_positions
        filtered_implicit_filters = []
        for filter_item in implicit_filters:
            # Usar raw_text para buscar la posición original
            filter_text = filter_item.raw_text.lower()
            filter_conflicts_groupby = False
            
            # Verificar si este filtro conflictúa con alguna posición de GROUP BY
            for gb_pos in groupby_positions:
                if gb_pos < len(tokens) and tokens[gb_pos].lower() in filter_text:
                    filter_conflicts_groupby = True
                    print(f"   🚫 Excluding implicit filter: {filter_item.column_name} = {filter_item.value} (GROUP BY conflict with position {gb_pos})")
                    break
            
            if not filter_conflicts_groupby:
                filtered_implicit_filters.append(filter_item)
        
        implicit_filters = filtered_implicit_filters
        
        if implicit_filters:
            all_column_value_pairs.extend(implicit_filters)
            all_processed_positions.update(implicit_positions)
            
            print(f"   ✅ Implicit filters found: {len(implicit_filters)}")
            for filter in implicit_filters:
                print(f"      🔍 {filter.column_name} = '{filter.value}' (confidence: {filter.confidence:.2f})")
            
        # STEP 2: DETECTAR PATRONES EXPLÍCITOS EN POSICIONES NO PROCESADAS
        # (Reutilizar la lógica existente pero evitando posiciones ya procesadas)
        
        # Identificar columnas temporales
        temporal_columns = set()
        for tf in temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['week', 'weeks', 'semana', 'semanas'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['month', 'months', 'mes', 'meses'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['day', 'days', 'dia', 'dias'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['year', 'years', 'año', 'años'])
        
        print(f"⏰ Temporal columns to exclude: {temporal_columns}")
        print(f"🔒 Positions already processed: {sorted(all_processed_positions)}")
        
        # CONTROL ESTRICTO DE DUPLICADOS
        processed_positions = set(all_processed_positions)  # Copiar posiciones ya procesadas
        created_filters = set()  # Para evitar filtros duplicados por contenido
        
        # PASO 1: DETECTAR PATRONES DIRECTOS CON DICCIONARIO TEMPORAL (PRIORIDAD MÁXIMA)
        for i in range(len(tokens) - 1):
            if i in processed_positions:
                continue
                
            current_token = tokens[i]
            
            # Verificar si es columna potencial
            column_info = self._identify_potential_column_english(current_token)
            if not column_info['is_column'] or column_info['normalized_name'] in temporal_columns:
                continue
            
            print(f"🔍 Testing TEMPORAL DICT pattern: '{current_token}' + [value from dictionary]")
            
            dict_result = self._extract_value_from_temporal_dict(tokens, i + 1, column_info['normalized_name'])
            
            if dict_result:
                # 🆕 CREAR CLAVE ÚNICA PARA EVITAR DUPLICADOS
                filter_key = f"{column_info['normalized_name']}={dict_result['normalized_value']}"
                
                if filter_key not in created_filters:
                    column_value_pairs = ColumnValuePair(
                        column_name=column_info['normalized_name'],
                        value=dict_result['normalized_value'],
                        confidence=dict_result['confidence'],
                        raw_text=f"{current_token} {dict_result['raw_text']}"
                    )
                    
                    all_column_value_pairs.append(column_value_pairs)
                    created_filters.add(filter_key)
                    print(f"✅ TEMPORAL DICT SUCCESS: {current_token} = '{dict_result['normalized_value']}'")
                    
                    # 🔧 FIX: Manejar None en tokens_consumed
                    tokens_consumed = dict_result.get('tokens_consumed')
                    if tokens_consumed is None or not isinstance(tokens_consumed, int):
                        # Calcular tokens_consumed basado en raw_text
                        raw_text = dict_result.get('raw_text', '')
                        if raw_text:
                            tokens_consumed = len(raw_text.split())
                        else:
                            tokens_consumed = 1
                        print(f"⚠️ WARNING: tokens_consumed was None, calculated as {tokens_consumed}")
                    
                    # 🚨 MARCAR TODAS LAS POSICIONES COMO PROCESADAS
                    end_position = min(i + 1 + tokens_consumed, len(tokens))
                    for pos in range(i, end_position):
                        processed_positions.add(pos)
                    
                    print(f"🔒 POSITIONS LOCKED: {list(range(i, end_position))}")
                else:
                    print(f"🔄 TEMPORAL DICT DUPLICATE AVOIDED: {filter_key}")
        
        # PASO 3: PATRONES EXPLÍCITOS CON PREPOSICIONES (respetando posiciones procesadas)
        i = 0
        while i < len(tokens) - 2:
            # Verificar si la posición ya fue procesada
            if i in processed_positions:
                print(f"⭕ SKIPPING position {i} (processed by special patterns)")
                i += 1
                continue
            
            # PATTERN: [preposition] [column] [value]
            positions_needed = {i, i + 1, i + 2}
            if positions_needed.intersection(processed_positions):
                print(f"⭕ PREPOSITION PATTERN: positions {positions_needed} overlap with processed {processed_positions}")
                i += 1
                continue
            
            pattern_result = self._detect_preposition_column_value_pattern_english(
                tokens, i, temporal_columns, processed_positions
            )
            
            if pattern_result:
                # Verificar duplicados por contenido
                filter_key = f"{pattern_result['pair'].column_name}={pattern_result['pair'].value}"
                existing_filters = {f"{cvp.column_name}={cvp.value}" for cvp in all_column_value_pairs}
                
                if filter_key not in existing_filters:
                    all_column_value_pairs.append(pattern_result['pair'])
                    print(f"✅ EXPLICIT FILTER CREATED (preposition): {pattern_result['raw_text']}")
                    
                    # Marcar posiciones como procesadas
                    tokens_consumed = pattern_result.get('tokens_consumed', 3)  # 🔧 FIX: Default value
                    for pos in range(i, i + tokens_consumed):
                        processed_positions.add(pos)
                else:
                    print(f"🔄 EXPLICIT DUPLICATE AVOIDED: {filter_key}")
                
                i += pattern_result.get('tokens_consumed', 3)  # 🔧 FIX: Default value
                continue
            
            i += 1
        
        print(f"🎯 Total filters detected: {len(all_column_value_pairs)}")
        print(f"   📦 Enhanced stock out: {len([cvp for cvp in all_column_value_pairs if cvp.column_name == 'Stock_Out'])}")
        print(f"   🔍 Implicit: {len(implicit_filters)}")
        print(f"   🔍 Explicit: {len(all_column_value_pairs) - len(implicit_filters) - len([cvp for cvp in all_column_value_pairs if cvp.column_name == 'Stock_Out'])}")
        print(f"🔒 Final processed positions: {sorted(processed_positions)}")
            
        # 🆕 ELIMINAR FILTROS DUPLICADOS
        print(f"🔧 REMOVING DUPLICATE FILTERS:")
        seen_filters = {}
        unique_filters = []
        
        for filter_item in all_column_value_pairs:
            filter_key = f"{filter_item.column_name}={filter_item.value}"
            if filter_key not in seen_filters:
                seen_filters[filter_key] = True
                unique_filters.append(filter_item)
            else:
                print(f"   🔄 Removing duplicate filter: {filter_key}")
        
        all_column_value_pairs = unique_filters
        print(f"🎯 Final unique filters: {len(all_column_value_pairs)}")

        return all_column_value_pairs


    # =====================================================================
    # =========== ACTUALIZACIÓN DEL PIPELINE PRINCIPAL ==================
    # =====================================================================

    def process_query_with_implicit_values(self, query: str, pre_normalized_query: str, preliminary_tokens: List[str]) -> Dict:
        """
        🇺🇸 PIPELINE PRINCIPAL ACTUALIZADO CON SOPORTE PARA VALORES IMPLÍCITOS
        Esta función reemplaza o extiende el process_query original
        """
        
        print(f"\n🇺🇸 PROCESSING ENGLISH QUERY WITH IMPLICIT VALUES: '{query}'")
        
    # STEP 1: NORMALIZATION (English-specific)
        normalized_query = self.normalize_english_query(pre_normalized_query)
        tokens = normalized_query.split()
        
        print(f"🔤 English tokens: {tokens}")   
        
    # STEP 2: SEMANTIC ANALYSIS (reuse existing)
        original_intent = self.pre_mapping_analyzer.analyze_original_intent(tokens)
        print(f"🧠 English semantic intent: {original_intent}")
           
    # STEP 2.5: DETECTAR MULTI-MÉTRICA TEMPRANO
        multi_metric_pattern = self.detect_multi_metric_pattern_english(tokens)
        if multi_metric_pattern and multi_metric_pattern.confidence >= 0.8:
            print(f"📊 MULTI-METRIC pattern detected early - using optimized path")
            
            # Generar SQL directamente para multi-métrica
            multi_metric_sql = self.generate_multi_metric_sql_direct(multi_metric_pattern, normalized_query, query)
            if multi_metric_sql:
                return multi_metric_sql      
            
    # STEP 3: ENGLISH-SPECIFIC PATTERN DETECTION CON VALORES IMPLÍCITOS
        temporal_filters = self.detect_temporal_patterns_english(tokens)
        
    # STEP 3.2: USAR LA NUEVA FUNCIÓN QUE INCLUYE VALORES IMPLÍCITOS
        column_value_pairs = self.detect_column_value_patterns_english_with_implicit(tokens, temporal_filters)  
        
    # STEP 3.5: Otros patrones (temporal conditional, list all, show rows)
        temporal_conditional_pattern = self.detect_temporal_conditional_pattern_english(tokens)
        list_all_pattern = self.detect_list_all_pattern_english(tokens)
        show_rows_pattern = self.detect_show_rows_pattern_english(tokens)
                                
    # STEP 4: COMPONENT CLASSIFICATION (reuse with adaptations)
        classified_components = self.classify_components_english(tokens, column_value_pairs)
        
    # STEP 5: STRUCTURE BUILDING (usar el método existente)
        query_structure = self.build_english_structure(classified_components, column_value_pairs, temporal_filters, tokens, original_intent)
        
        # Agregar patrones especiales detectados
        if temporal_conditional_pattern:
            query_structure.temporal_conditional_pattern = temporal_conditional_pattern
        
        if list_all_pattern:
            query_structure.list_all_pattern = list_all_pattern
            
        if show_rows_pattern:
            query_structure.show_rows_pattern = show_rows_pattern
        
    # STEP 6: VALIDATION (reuse existing)
        validation_result = self.validate_english_structure(query_structure)
        
        if not validation_result['valid']:
            return {
                'success': False,
                'error': validation_result['error'],
                'original_input': query,
                'suggestions': validation_result['suggestions'],
                'language': 'english'
            }
        
    # STEP 7: SQL GENERATION
        conceptual_sql = self.generate_optimized_sql_english(query_structure)

    # STEP 8: SQL SCHEMA NORMALIZATION (con fallback)
        if self.sql_mapper:
            try:
                normalized_sql = self.sql_mapper.normalize_sql(conceptual_sql)
            except Exception as e:
                print(f"⚠️ Schema mapping failed: {e}")
                normalized_sql = conceptual_sql
        else:
            normalized_sql = conceptual_sql
            
    # STEP 9: FINAL RESULT
        return {
            'success': True,
            'language': 'english',
            'original_input': query,
            'normalized_query': normalized_query,
            'tokens': tokens,
            'conceptual_sql': conceptual_sql,
            'sql_query': normalized_sql,
            'complexity_level': query_structure.get_complexity_level(),
            'processing_method': 'english_pipeline_with_implicit_values',
            'note': '🇺🇸 Processed with English patterns + Implicit Value Detection',
            'query_structure': self.structure_to_dict_english(query_structure),
            'hierarchical_structure': self.generate_hierarchical_structure_english(query_structure),
            'interpretation': self.generate_natural_interpretation_english(query_structure),
            'confidence': self.calculate_overall_confidence_english(query_structure),
            'schema_mapping_stats': self.sql_mapper.get_mapping_statistics() if self.sql_mapper else {},
            'implicit_values_detected': len([cvp for cvp in column_value_pairs if 'implicit' in cvp.raw_text.lower()]) # Estadística adicional
        }
        
        
    def _extract_value_from_temporal_dict(self, tokens: List[str], start_idx: int, column_name: str) -> Optional[Dict]:
        """
        🗄️ EXTRACTOR ESPECÍFICO PARA DICCIONARIO TEMPORAL
        Prueba combinaciones de tokens contra el diccionario temporal
        """
        if (not hasattr(self.dictionaries, 'temporal_dictionary') or 
            start_idx >= len(tokens)):
            return None
        
        print(f"      🗄️ Searching temporal dictionary starting at position {start_idx}")
        
        # Probar combinaciones desde la más larga (6 tokens) hasta 1 token
        max_tokens = min(6, len(tokens) - start_idx)
        
        for length in range(max_tokens, 0, -1):
            if start_idx + length > len(tokens):
                continue
            
            candidate_tokens = tokens[start_idx:start_idx + length]
            
            print(f"         🔍 Testing {length} tokens: {candidate_tokens}")
            
            # Generar variantes para buscar en el diccionario
            test_variants = [
                ' '.join(candidate_tokens).lower(),           # "palacio de hierro"
                ''.join(candidate_tokens).lower(),            # "palaciodehierro"  
                '_'.join(candidate_tokens).lower(),           # "palacio_de_hierro"
                ' '.join(candidate_tokens).upper(),           # "PALACIO DE HIERRO"
                ''.join(candidate_tokens).upper(),            # "PALACIODEHIERRO"
                '_'.join(candidate_tokens).upper(),           # "PALACIO_DE_HIERRO"
            ]
            
            for variant in test_variants:
                print(f"            🔍 Testing variant: '{variant}'")
                
                temporal_entry = self.dictionaries.search_in_temporal_dictionary(variant)
                
                if temporal_entry:
                    # Verificar que la columna coincida
                    entry_column = temporal_entry.get('column_name', '').lower()
                    if entry_column == column_name.lower():
                        
                        original_value = temporal_entry.get('original_value', variant.upper())
                        confidence = temporal_entry.get('confidence', 0.95)
                        
                        print(f"            ✅ PERFECT MATCH: '{variant}' → '{original_value}' (column: {entry_column})")
                        
                        return {
                            'normalized_value': original_value,
                            'raw_text': ' '.join(candidate_tokens),
                            'tokens_consumed': length if length else len(candidate_tokens),  # FIX
                            'confidence': confidence
                        }
                    else:
                        print(f"            ❌ Column mismatch: found '{entry_column}', expected '{column_name.lower()}'")
        
        print(f"         ❌ No matches found in temporal dictionary")
        return None      
          
                
    def detect_temporal_conditional_pattern_english(self, tokens: List[str]) -> Optional[Dict]:
        """🕐 DETECTOR MEJORADO DE PATRÓN TEMPORAL CONDICIONAL EN INGLÉS"""
        print(f"🕐 DETECTING TEMPORAL CONDITIONAL PATTERN:")
        print(f"   📤 Tokens: {tokens}")
        
        if len(tokens) < 5:  # Mínimo: week where store had sales
            return None
        
        # STEP 1: Verificar que empiece con dimensión temporal (SINGULAR O PLURAL)
        first_token = tokens[0].lower()
        
        # 🔧 MAPEO DE PLURALES A SINGULARES
        temporal_plural_map = {
            'weeks': 'week',
            'months': 'month', 
            'days': 'day',
            'years': 'year',
            'quarters': 'quarter'
        }
        
        # Normalizar plural a singular si es necesario
        normalized_first = temporal_plural_map.get(first_token, first_token)
        
        temporal_dimensions = {'week', 'month', 'day', 'year', 'quarter'}
        
        if normalized_first not in temporal_dimensions:
            print(f"   ❌ No temporal dimension at start: '{first_token}'")
            return None
        
        # STEP 2: Buscar "where" (debe estar en posición 1 o 2)
        where_pos = -1
        for i in range(1, min(3, len(tokens))):
            if tokens[i].lower() == 'where':
                where_pos = i
                break
        
        if where_pos == -1:
            print(f"   ❌ No 'where' found after temporal dimension")
            return None
        
        print(f"   ✅ Temporal dimension: '{normalized_first}' (from '{first_token}')")
        print(f"   ✅ 'where' found at position {where_pos}")
        
        # STEP 3: Extraer componentes después del where
        remaining_tokens = tokens[where_pos + 1:]
        components = self._extract_enhanced_conditional_components_english(remaining_tokens)
        
        if not components:
            print(f"   ❌ Could not extract conditional components")
            return None
        
        # STEP 4: Construir resultado mejorado (usar la forma normalizada)
        pattern_result = {
            'pattern_type': 'TEMPORAL_CONDITIONAL',
            'temporal_dimension': normalized_first,  # Usar singular normalizado
            'entity_column': components['entity_column'],
            'entity_value': components['entity_value'],
            'condition_verb': components['condition_verb'],
            'comparative': components['comparative'],
            'target_metric': components['target_metric'],
            'order_direction': components['order_direction'],
            'confidence': components['confidence'],
            'raw_tokens': tokens
        }
        
        print(f"🕐 TEMPORAL CONDITIONAL PATTERN DETECTED:")
        print(f"   ⏰ Temporal: {pattern_result['temporal_dimension']}")
        print(f"   🎯 Entity: {pattern_result['entity_column']} = '{pattern_result['entity_value']}'")
        print(f"   🔄 Verb: {pattern_result['condition_verb']}")
        print(f"   📊 Metric: {pattern_result['target_metric']}")
        print(f"   🔼 Direction: {pattern_result['order_direction']}")
        
        return pattern_result
            
                            
                    
    def _extract_enhanced_conditional_components_english(self, tokens: List[str]) -> Optional[Dict]:
        """🔍 EXTRACTOR MEJORADO DE COMPONENTES CONDICIONALES"""
        print(f"      🔍 Extracting from: {tokens}")
        
        if len(tokens) < 3:  # Mínimo: sams had sales
            return None
        
        # Inicializar valores
        entity_column = None
        entity_value = None
        condition_verb = None
        target_metric = None
        comparative = 'more'  # Default
        order_direction = 'DESC'  # Default
        verb_start_pos = -1
        
        # STEP 1: Buscar entidad (columna + valor)
        for i in range(len(tokens) - 2):
            current_token = tokens[i]
            next_token = tokens[i + 1] if i + 1 < len(tokens) else None
            
            # Verificar si es columna potencial (store, account, item, etc.)
            if self._is_potential_column_english(current_token):
                # Verificar si el siguiente es un valor
                if next_token and self._is_potential_value_english(next_token):
                    entity_column = current_token.lower()
                    entity_value = next_token.upper()
                    verb_start_pos = i + 2
                    print(f"         ✅ Entity found: {entity_column} = '{entity_value}'")
                    break
            
            # Verificar si es un valor conocido del diccionario temporal
            if hasattr(self.dictionaries, 'search_in_temporal_dictionary'):
                temp_result = self.dictionaries.search_in_temporal_dictionary(current_token)
                if temp_result:
                    entity_column = temp_result.get('column_name', 'account').lower()
                    entity_value = temp_result.get('original_value', current_token.upper())
                    verb_start_pos = i + 1
                    print(f"         ✅ Entity from dictionary: {entity_column} = '{entity_value}'")
                    break
        
        # Si no encontramos entidad, empezar desde el principio
        if verb_start_pos == -1:
            verb_start_pos = 0
        
        # STEP 2: Buscar verbo temporal
        condition_verbs = {
            'had', 'has', 'got', 'achieved', 'reached', 'obtained',
            'generated', 'produced', 'made', 'recorded', 'showed', 'with'
        }
        
        for i in range(verb_start_pos, min(verb_start_pos + 2, len(tokens))):
            if i < len(tokens) and tokens[i].lower() in condition_verbs:
                condition_verb = tokens[i].lower()
                comparative_start_pos = i + 1
                print(f"         ✅ Verb found: '{condition_verb}'")
                break
        
        # Si no encontramos verbo, buscar desde el inicio
        if not condition_verb:
            for token in tokens:
                if token.lower() in condition_verbs:
                    condition_verb = token.lower()
                    print(f"         ✅ Verb found: '{condition_verb}'")
                    break
        
        # STEP 3: Buscar comparativo + métrica
        comparative_map = {
            # Positivos (ORDER BY DESC)
            'more': 'DESC', 'most': 'DESC', 'highest': 'DESC',
            'best': 'DESC', 'maximum': 'DESC', 'greater': 'DESC',
            # Negativos (ORDER BY ASC)
            'less': 'ASC', 'least': 'ASC', 'lowest': 'ASC',
            'worst': 'ASC', 'minimum': 'ASC', 'smaller': 'ASC'
        }
        
        for token in tokens:
            token_lower = token.lower()
            
            # Si encontramos un comparativo
            if token_lower in comparative_map:
                comparative = token_lower
                order_direction = comparative_map[token_lower]
                print(f"         ✅ Comparative: '{comparative}' → {order_direction}")
                break
        
        # STEP 4: Buscar métrica objetivo
        metric_keywords = {
            'sales', 'revenue', 'inventory', 'profit', 'margin',
            'cost', 'units', 'sell_out', 'stock', 'amount', 'quantity'
        }
        
        for token in tokens:
            if token.lower() in metric_keywords:
                target_metric = token.lower()
                print(f"         ✅ Metric: '{target_metric}'")
                break
        
        # Si no encontramos métrica, usar 'sales' como default
        if not target_metric:
            # Mapear métricas comunes según el contexto
            if 'sales' in ' '.join(tokens).lower() or 'sell' in ' '.join(tokens).lower():
                target_metric = 'Sell_Out'
            else:
                target_metric = 'Sell_Out'  # Default más común
            print(f"         ℹ️ Using default metric: '{target_metric}'")
        
        # Si no encontramos verbo, usar 'had' como default
        if not condition_verb:
            condition_verb = 'had'
            print(f"         ℹ️ Using default verb: 'had'")
        
        # STEP 5: Calcular confianza
        confidence = 0.6  # Base
        if entity_column and entity_value:
            confidence += 0.2
        if condition_verb:
            confidence += 0.1
        if comparative != 'more':  # Comparativo explícito
            confidence += 0.1
        
        return {
            'entity_column': entity_column,
            'entity_value': entity_value,
            'condition_verb': condition_verb,
            'comparative': comparative,
            'target_metric': target_metric,
            'order_direction': order_direction,
            'confidence': min(1.0, confidence)
        }
                    


    def _detect_preposition_column_value_pattern_english(self, tokens: List[str], start_idx: int, temporal_columns: set, processed_positions: set = None) -> Optional[Dict]:
        """
        Detecta patrones: [preposition] [column] [value] - CON VERIFICACIÓN DE POSICIONES
        """
        if processed_positions is None:
            processed_positions = set()
        
        # 🆕 VERIFICAR SI YA ESTÁN PROCESADAS LAS POSICIONES
        positions_to_check = {start_idx, start_idx + 1, start_idx + 2}
        if positions_to_check.intersection(processed_positions):
            print(f"   🔄 Positions {positions_to_check} already processed, skipping")
            return None
        
        if start_idx + 2 >= len(tokens):
            return None
        
        preposition_token = tokens[start_idx]
        column_token = tokens[start_idx + 1] 
        value_token = tokens[start_idx + 2]
        
        english_prepositions = {'with', 'from', 'for', 'by', 'in', 'on', 'at', 'of'}
        
        if preposition_token.lower() not in english_prepositions:
            return None
        
        print(f"🔍 Analyzing English preposition pattern: '{preposition_token}' + '{column_token}' + '{value_token}'")
        
        column_info = self._identify_potential_column_english(column_token)
        print(f"     Column? {column_info}")
        
        if not column_info['is_column']:
            return None
        
        if column_info['normalized_name'] in temporal_columns:
            print(f"⏰ Skipping '{column_token}' - already processed as temporal")
            return None
        
        value_info = self._identify_potential_value_english(value_token, start_idx + 2, tokens)
        print(f"     Value? {value_info}")
        
        if not value_info['is_value']:
            return None
        
        confidence_adjustment = 0.95
        final_confidence = min(column_info['confidence'], value_info['confidence']) * confidence_adjustment
        
        pair = ColumnValuePair(
            column_name=column_info['normalized_name'],
            value=value_info['normalized_value'],
            confidence=final_confidence,
            raw_text=f"{preposition_token} {column_token} {value_token}"
        )
        
        return {
            'pair': pair,
            'tokens_consumed': 3,
            'raw_text': f"{preposition_token} {column_token} = '{value_token}'"
        }


    def _identify_potential_column_english(self, token: str) -> Dict:
        """Identificador de Columnas Potenciales para inglés - VERSIÓN MEJORADA"""
        token_lower = token.lower()
        
        # 🔧 DESCARTAR: Modificadores de agregación que NO son columnas
        aggregate_modifiers = {'total', 'sum', 'average', 'avg', 'max', 'min', 'count'}
        if token_lower in aggregate_modifiers:
            return {
                'is_column': False,
                'normalized_name': None,
                'type': 'aggregate_modifier',
                'confidence': 0.0
            }
        
        # 🔧 DESCARTAR: Conectores y palabras de enlace
        link_words = {'of', 'for', 'in', 'on', 'at', 'by', 'from', 'to', 'with'}
        if token_lower in link_words:
            return {
                'is_column': False,
                'normalized_name': None,
                'type': 'link_word',
                'confidence': 0.0
            }
        
        # 🔧 DESCARTAR: Errores tipográficos comunes que no son columnas
        common_typos = {'ammount', 'amout', 'summ', 'totall', 'avrage'}
        if token_lower in common_typos:
            return {
                'is_column': False,
                'normalized_name': None,
                'type': 'typo',
                'confidence': 0.0
            }
        
        # LÓGICA ORIGINAL (mantener)
        if token_lower in self.dictionaries.dimensiones:
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'dimension',
                'confidence': 0.95
            }
        
        if token_lower in self.dictionaries.metricas:
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'metric',
                'confidence': 0.90
            }
        
        # Buscar en frases compuestas (ej: stock_out)
        if token_lower in self.dictionaries.frases_compuestas:
            normalized = self.dictionaries.frases_compuestas[token_lower]
            return {
                'is_column': True,
                'normalized_name': normalized,
                'type': 'compound',
                'confidence': 0.95
            }
        
        # Detectar nombres de columnas con snake_case
        if self._looks_like_column_name_english(token):
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'inferred',
                'confidence': 0.70
            }
        
        return {
            'is_column': False,
            'normalized_name': None,
            'type': None,
            'confidence': 0.0
        }
        

    def _identify_potential_value_english(self, token: str, position: int, tokens: List[str]) -> Dict:
        """Identificador de Valores Específicos para inglés"""
        
        # PRIORIDAD MÁXIMA: Letras individuales mayúsculas
        if len(token) == 1 and token.isupper() and token.isalpha():
            return {
                'is_value': True,
                'normalized_value': token,
                'confidence': 0.98
            }
        
        token_lower = token.lower()
        token_upper = token.upper()
        
        
        # DESCARTAR: Palabras del lenguaje natural en inglés
        english_language_words = self.dictionaries.conectores.union({
            'between', 'from', 'to', 'with', 'and', 'or', 'but'
        })
        
        if token_lower in english_language_words and token != 'Y':
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        # DESCARTAR: Usar diccionarios para operaciones y métricas
        if token_lower in self.dictionaries.operaciones:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        if token_lower in self.dictionaries.metricas:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        if token_lower in self.dictionaries.dimensiones:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}

        # REGLA GENÉRICA: Códigos alfanuméricos
        if self._is_generic_code_value_english(token):
            context_confidence = self._calculate_generic_context_confidence_english(token, position, tokens)
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': context_confidence
            }

        # REGLAS BÁSICAS
        if len(token) == 1 and token.isalpha():
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': 0.90
            }
        
        if token.isdigit():
            return {
                'is_value': True,
                'normalized_value': token,
                'confidence': 0.95
            }
        
        # Estados comunes en inglés
        common_english_states = {
            'active', 'inactive', 'pending', 'completed', 'cancelled',
            'yes', 'no', 'true', 'false', 'on', 'off',
            'high', 'medium', 'low', 'premium', 'basic', 'vip'
        }
        if token_lower in common_english_states:
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': 0.85
            }
        
        return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}

    def _looks_like_column_name_english(self, token: str) -> bool:
        """Detecta si parece nombre de columna en inglés"""
        token_lower = token.lower()
        
        # Snake case (ej: stock_out, dead_inventory)
        if '_' in token_lower:
            return True
        
        # Nombres comunes de columnas en inglés
        common_column_patterns = {
            'account', 'product', 'customer', 'store', 'item', 'order',
            'status', 'type', 'code', 'id', 'name', 'date', 'amount'
        }
        
        if token_lower in common_column_patterns:
            return True
        
        # Termina en sufijos comunes
        if token_lower.endswith(('_id', '_code', '_status', '_type', '_date')):
            return True
        
        return False

    def _is_generic_code_value_english(self, token: str) -> bool:
        """Detecta códigos genéricos en inglés"""
        if not re.match(r'^[A-Za-z0-9\-/\.]+$', token):
            return False
        
        if len(token) < 3:
            return False
        
        has_letter = any(c.isalpha() for c in token)
        has_number = any(c.isdigit() for c in token)
        
        if has_letter and has_number:
            return True
        
        if has_letter and not has_number and len(token) >= 2:
            return True
        
        if has_number and not has_letter and len(token) >= 4:
            return True
        
        return False

    def _calculate_generic_context_confidence_english(self, token: str, position: int, tokens: List[str]) -> float:
        """Calcula confianza basada en contexto para inglés"""
        base_confidence = 0.75
        
        # Si está después de preposición, aumentar confianza
        if position > 0:
            prev_token = tokens[position - 1].lower()
            if prev_token in {'with', 'for', 'by', 'of'}:
                base_confidence += 0.1
        
        # Si es largo y alfanumérico, probablemente es código
        if len(token) > 8 and any(c.isalpha() for c in token) and any(c.isdigit() for c in token):
            base_confidence += 0.05
        
        return min(0.95, base_confidence)

    def _is_potential_column_english(self, token: str) -> bool:
        """🇺🇸 VERIFICADOR DE COLUMNAS - NUNCA MAYÚSCULAS INDIVIDUALES"""
        
        # 🔧 REGLA ABSOLUTA: Letras mayúsculas individuales NUNCA son columnas
        if len(token) == 1 and token.isupper() and token.isalpha():
            return False
        
        token_lower = token.lower()
        return (token_lower in self.dictionaries.dimensiones or 
                token_lower in self.dictionaries.metricas or
                '_' in token or
                token_lower in ['account', 'product', 'customer', 'partner', 'region', 'status', 'type', 'store', 'stock_out'])

            
    
    def classify_components_english(self, tokens: List[str], column_value_pairs: List[ColumnValuePair]) -> Dict[str, QueryComponent]:
        """🇺🇸 CLASIFICACIÓN DE COMPONENTES EN INGLÉS"""
        
        print(f"🔍 CLASSIFYING ENGLISH COMPONENTS:")
        
        classified = {}
        processed_tokens = set()
        
        # Obtener posiciones procesadas por patrones temporales
        temporal_positions = getattr(self, 'temporal_processed_positions', set())
        if temporal_positions:
            print(f"   ⏰ Temporal positions to skip: {sorted(temporal_positions)}")
        
        # 🔧 NUEVO: Obtener posiciones procesadas por valores implícitos
        implicit_positions = set()
        
        # Mark tokens used in column-value pairs (incluye implícitos)
        for cvp in column_value_pairs:
            pair_tokens = cvp.raw_text.split()
            processed_tokens.update(pair_tokens)
            
            # 🔧 NUEVO: Si es un valor multi-palabra, marcar las posiciones
            # de todos los tokens individuales
            for i, token in enumerate(tokens):
                # Buscar si este token es parte del raw_text del filtro
                if token.lower() in cvp.raw_text.lower():
                    # Verificar si es parte del valor compuesto
                    raw_lower = cvp.raw_text.lower()
                    token_lower = token.lower()
                    
                    # Si el raw_text contiene múltiples palabras y este token es una de ellas
                    if ' ' in raw_lower and token_lower in raw_lower.split():
                        implicit_positions.add(i)
                        print(f"   🔒 Token '{token}' at position {i} is part of implicit value '{cvp.raw_text}'")
            
            print(f"🔗 English filter detected: {cvp.column_name} = '{cvp.value}'")
        
        # Classify individual tokens
        for i, token in enumerate(tokens):
            # Saltar tokens en posiciones temporales
            if i in temporal_positions:
                print(f"   ⏭️ Skipping position {i} ('{token}') - used in temporal pattern")
                continue
            
            # 🔧 NUEVO: Saltar tokens que son parte de valores implícitos multi-palabra
            if i in implicit_positions:
                print(f"   ⏭️ Skipping position {i} ('{token}') - part of implicit multi-word value")
                continue
            
            classified[token] = self.classify_single_component_english(token)
            
            if token in processed_tokens:
                classified[token].linguistic_info['used_in_filter'] = True
                print(f"🎯 English token '{token}' classified as {classified[token].type.value} (used in filter)")
            else:
                print(f"🔍 English token '{token}' classified as {classified[token].type.value}")
        
        return classified

                     
    def classify_single_component_english(self, token: str) -> QueryComponent:
        # PRIORIDAD 0: Buscar PRIMERO en diccionario temporal
        if hasattr(self.dictionaries, 'temporal_dictionary'):
            # Buscar el token en el diccionario temporal
            temp_result = self.dictionaries.search_in_temporal_dictionary(token.lower())
            if not temp_result:
                temp_result = self.dictionaries.search_in_temporal_dictionary(token.upper())
            
            if temp_result:
                return QueryComponent(
                    text=token,
                    type=ComponentType.VALUE,
                    confidence=0.95,  # Alta confianza porque está en el diccionario
                    subtype='dictionary_value',
                    value=temp_result.get('original_value'),
                    column_name=temp_result.get('column_name'),
                    linguistic_info={
                        'source': 'temporal_dictionary',
                        'column': temp_result.get('column_name'),
                        'original_value': temp_result.get('original_value')
                    }
                )
    
        # 🆕 REGLA ESPECIAL: Números en contexto (para SHOW_ROWS y otros)
        if token.isdigit():
            return QueryComponent(
                text=token,
                type=ComponentType.VALUE,
                confidence=0.95,
                subtype='numeric_value',
                value=int(token),
                linguistic_info={'source': 'numeric_literal'}
            )
        
        # 🆕 REGLA PRIORITARIA: Dimensiones temporales (SINGULAR Y PLURAL)
        temporal_dimensions = {'week', 'weeks', 'month', 'months', 'day', 'days', 'year', 'years', 'quarter', 'quarters'}
        if token.lower() in temporal_dimensions:
            # Normalizar plural a singular para el valor
            temporal_singular_map = {
                'weeks': 'week',
                'months': 'month',
                'days': 'day', 
                'years': 'year',
                'quarters': 'quarter'
            }
            normalized_value = temporal_singular_map.get(token.lower(), token.lower())
            
            return QueryComponent(
                text=token,
                type=ComponentType.DIMENSION,
                confidence=0.98,
                subtype='temporal_dimension',
                value=normalized_value,  # Guardar la forma singular
                linguistic_info={
                    'source': 'temporal_dimension_priority', 
                    'is_plural': token.lower() != normalized_value,
                    'original_form': token
                }
            )
        
        # Para 'with' específicamente:
        if token.lower() == 'with':
            print(f"🔍 DEBUG CRÍTICO para 'with':")
            print(f"   ¿En self.dictionaries.conectores? {'with' in self.dictionaries.conectores}")
            print(f"   Conectores disponibles: {list(self.dictionaries.conectores)[:10]}...")  # Primeros 10
            print(f"   get_component_type('with') = {self.dictionaries.get_component_type('with')}")
        
        # 🔧 REGLA ABSOLUTA #1: Letras mayúsculas individuales SIEMPRE son datos
        if len(token) == 1 and token.isupper() and token.isalpha():
            return QueryComponent(
                text=token,
                type=ComponentType.VALUE,
                confidence=1.0,  # Confianza máxima
                subtype='table_data_absolute',
                value=token,
                linguistic_info={
                    'source': 'uppercase_letter_absolute_rule',
                    'is_table_data': True,
                    'never_connector': True,
                    'absolute_rule_applied': True
                }
            )
        
        # 🔧 REGLA ABSOLUTA #2: Códigos cortos en mayúsculas también son datos
        if token.isupper() and 2 <= len(token) <= 4 and any(c.isalpha() for c in token):
            return QueryComponent(
                text=token,
                type=ComponentType.VALUE,
                confidence=0.98,
                subtype='code_data_absolute',
                value=token,
                linguistic_info={
                    'source': 'uppercase_code_absolute_rule',
                    'is_table_data': True
                }
            )
        
        # English ranking indicators
        ranking_indicators = {
            'top', 'best', 'highest', 'maximum', 'first', 'greatest', 'most',
            'worst', 'lowest', 'minimum', 'last', 'least', 'bottom'
        }
        
        if token.lower() in ranking_indicators:
            return QueryComponent(
                text=token,
                type=ComponentType.OPERATION,
                confidence=0.90,
                subtype='ranking_indicator',
                value=token.lower(),
                linguistic_info={'source': 'english_ranking_indicator'}
            )
        
        # Continuar con el resto de la clasificación existente...
        corrected_token = self.dictionaries.correct_typo(token)
        if corrected_token != token:
            corrected_component = self.classify_single_component_english(corrected_token)
            if corrected_component.type != ComponentType.UNKNOWN:
                corrected_component.linguistic_info = {
                    'source': 'typo_correction',
                    'original': token,
                    'corrected': corrected_token
                }
                corrected_component.confidence *= 0.85
                return corrected_component
        
        component_type = self.dictionaries.get_component_type(token)
        
        if component_type == ComponentType.DIMENSION:
            return QueryComponent(
                text=token,
                type=ComponentType.DIMENSION,
                confidence=0.95,
                linguistic_info={'source': 'dimension_dictionary'}
            )
        elif component_type == ComponentType.OPERATION:
            english_operations = {
                'max': 'máximo', 'maximum': 'máximo', 'highest': 'máximo', 
                'more': 'suma',
                'most': 'suma',
                'min': 'mínimo', 'minimum': 'mínimo', 'lowest': 'mínimo', 'less': 'mínimo',
                'sum': 'suma', 'total': 'suma',
                'avg': 'promedio', 'average': 'promedio',
                'count': 'conteo'
            }
            
            mapped_value = english_operations.get(token.lower(), token.lower())
            
            return QueryComponent(
                text=token,
                type=ComponentType.OPERATION,
                confidence=0.95,
                value=mapped_value,
                linguistic_info={'source': 'english_operation_dictionary'}
            )
        elif component_type == ComponentType.METRIC:
            return QueryComponent(
                text=token,
                type=ComponentType.METRIC,
                confidence=0.95,
                linguistic_info={'source': 'metric_dictionary'}
            )
        elif component_type == ComponentType.CONNECTOR:
            return QueryComponent(
                text=token,
                type=ComponentType.CONNECTOR,
                confidence=0.8,
                linguistic_info={'source': 'english_connector_dictionary'}
            )
        
        # Default: unknown
        return QueryComponent(
            text=token,
            type=ComponentType.UNKNOWN,
            confidence=0.3,
            linguistic_info={'source': 'unknown_english'}
        )

    

    def _is_potential_value_english(self, token: str) -> bool:
        """🇺🇸 VERIFICADOR DE VALORES - REGLA ABSOLUTA PARA MAYÚSCULAS"""
        
        # 🔧 REGLA ABSOLUTA #1: Letras mayúsculas individuales SIEMPRE son valores
        if len(token) == 1 and token.isupper() and token.isalpha():
            return True
        
        # 🔧 REGLA ABSOLUTA #2: Códigos en mayúsculas SIEMPRE son valores
        if token.isupper() and 2 <= len(token) <= 6 and any(c.isalpha() for c in token):
            return True
        
        # RESTO DE VALIDACIONES...
        if token.isdigit():
            return True
        
        if token.endswith('%'):
            return True
        
        # Estados comunes en mayúsculas
        common_states = {'ACTIVE', 'INACTIVE', 'PENDING', 'COMPLETE', 'CANCELLED', 'YES', 'NO', 'TRUE', 'FALSE'}
        if token.upper() in common_states:
            return True
        
        return False
        
        
    def generate_english_sql(self, structure: QueryStructure) -> str:
        """🇺🇸 GENERACIÓN SQL COMPLETA PARA INGLÉS - VERSIÓN FINAL"""
        
        select_parts = []
        from_clause = "FROM datos"
        where_conditions = []
        group_by_parts = []
        order_by_parts = []
        
        # Identificar columnas temporales para evitar duplicación
        temporal_columns = set()
        
        for tf in structure.temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['week', 'weeks'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['month', 'months'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['day', 'days'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['year', 'years'])
        
        print(f"🗄️ Generating COMPLETE English SQL:")
        print(f"   ⏰ Temporal columns detected: {temporal_columns}")
        print(f"   🎯 Query pattern: {structure.query_pattern.value}")
        print(f"   🔗 Is compound: {structure.is_compound_query}")
        print(f"   🏆 Is ranking: {structure.is_ranking_query}")
        print(f"   🔗 Is multi-dimensional: {structure.is_multi_dimension_query}")
        
        # 🔧 NUEVA LÓGICA: Manejar rankings multi-dimensionales
        if (structure.is_ranking_query and 
            structure.is_multi_dimension_query and 
            len(structure.main_dimensions) >= 2):
            print(f"🏆🔗 DETECTED: English multi-dimensional ranking → using specialized generator")
            return self.generate_multi_dimension_english_sql(structure, temporal_columns)
        
        # LÓGICA: Manejar consultas multi-dimensionales sin ranking
        if (structure.is_multi_dimension_query and 
            structure.query_pattern == QueryPattern.MULTI_DIMENSION):
            print(f"🔗 DETECTED: English multi-dimensional without ranking → using specialized generator")
            return self.generate_multi_dimension_english_sql(structure, temporal_columns)
        
        # LÓGICA: Manejar rankings simples
        if (structure.is_ranking_query and 
            structure.ranking_criteria and 
            not structure.is_multi_dimension_query):
            print(f"🏆 DETECTED: English simple ranking → using ranking generator")
            return self.generate_ranking_sql_english(structure, temporal_columns)
        
        # Verificar si es agregación global
        is_global_aggregation = not structure.main_dimension and structure.operations and structure.metrics
        
        if is_global_aggregation:
            print(f"🌐 Generating English SQL for global aggregation")
            
            if structure.operations and structure.metrics:
                operation = structure.operations[0]
                metric = structure.metrics[0]

                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                            
        else:
            # Lógica para consultas con dimensión principal
            if structure.main_dimension:
                dim_name = structure.main_dimension.text
                formatted_dim = self.format_temporal_dimension(dim_name)
                select_parts.append(formatted_dim)
                group_by_parts.append(dim_name)  
            
            # CONSULTAS COMPUESTAS
            if structure.is_compound_query and structure.compound_criteria:
                print(f"🔗 Processing English compound query with {len(structure.compound_criteria)} criteria:")
                
                for i, criteria in enumerate(structure.compound_criteria):
                    operation_value = criteria.operation.value
                    metric_text = criteria.metric.text
                    
                    if operation_value == 'máximo':
                        agg_function = self._get_contextual_aggregation_english(structure, metric_text, operation_value)
                    else:
                        sql_operations = {
                            'mínimo': f'MIN({metric_text})',
                            'suma': f'SUM({metric_text})',
                            'promedio': f'AVG({metric_text})',
                            'conteo': f'COUNT({metric_text})'
                        }
                        agg_function = sql_operations.get(operation_value, f'SUM({metric_text})')
                    
                    if agg_function:
                        select_parts.append(agg_function)
                        
                        if operation_value in ['máximo', 'mayor']:
                            order_direction = "DESC"
                        elif operation_value in ['mínimo', 'menor']:
                            order_direction = "ASC"
                        else:
                            order_direction = "DESC"
                        
                        order_by_parts.append(f"{agg_function} {order_direction}")
                        
                        print(f"   🔗 English Criteria {i+1}: {operation_value} {metric_text} → {agg_function} {order_direction}")
                    else:
                        select_parts.append(metric_text)
                        order_by_parts.append(f"{metric_text} DESC")
                        print(f"   🔗 English Criteria {i+1}: {metric_text} → {metric_text} DESC")
                
                
                
            # LÓGICA TRADICIONAL
            elif structure.operations and structure.metrics:
                operation = structure.operations[0]
                metric = structure.metrics[0]
                
                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                    
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        if operation.value in ['máximo', 'mayor']:
                            order_by_parts.append(f"{agg_function} DESC")
                        elif operation.value in ['mínimo', 'menor']:
                            order_by_parts.append(f"{agg_function} ASC")
                        else:
                            order_by_parts.append(f"{agg_function} DESC")
                    else:
                        order_by_parts.append(f"{agg_function} DESC")
                else:
                    select_parts.append(metric.text)
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        order_by_parts.append(f"{metric.text} DESC")
        
        # WHERE para condiciones de columna (excluyendo temporales duplicadas)
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
                print(f"   ✅ English WHERE condition: {condition.column_name} = '{condition.value}'")
            else:
                print(f"   ⏰ English excluding duplicate temporal condition: {condition.column_name} = '{condition.value}'")
        
        # FILTROS DE EXCLUSIÓN
        if hasattr(structure, 'exclusion_filters'):
            for exclusion in structure.exclusion_filters:
                if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                    where_conditions.append(f"{exclusion.column_name} != '{exclusion.value}'")
                    print(f"   🚫 English exclusion condition: {exclusion.column_name} != '{exclusion.value}'")
        
        # FILTROS TEMPORALES
        advanced_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
        if advanced_conditions:
            where_conditions.extend(advanced_conditions)
            print(f"   ✅ English using temporal filters: {advanced_conditions}")
        
        # CONSTRUCCIÓN DEL SQL FINAL
        sql_parts = []
        
        if select_parts:
            sql_parts.append(f"SELECT {', '.join(select_parts)}")
        else:
            sql_parts.append("SELECT *")
        
        sql_parts.append(from_clause)
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # LIMITAR LA DATA SEGÚN EL PATRÓN
        if structure.query_pattern == QueryPattern.REFERENCED:
            sql_parts.append("LIMIT 1")
            print(f"   🎯 English adding LIMIT 1 for REFERENCED pattern")
            
        elif structure.query_pattern == QueryPattern.TOP_N and structure.limit_value:
            sql_parts.append(f"LIMIT {structure.limit_value}")
            print(f"   🏆 English adding LIMIT {structure.limit_value} for TOP_N pattern")
        
        elif structure.is_ranking_query and structure.ranking_criteria and structure.ranking_criteria.value:
            limit_value = int(structure.ranking_criteria.value)
            sql_parts.append(f"LIMIT {limit_value}")
            print(f"   🏆 English FORCING LIMIT {limit_value} for ranking (pattern: {structure.query_pattern.value})")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 Final COMPLETE English SQL: {final_sql}")
        
        return final_sql


    def generate_ranking_sql_english(self, structure: QueryStructure, temporal_columns: set) -> str:
        """🇺🇸 GENERADOR DE SQL PARA RANKINGS EN INGLÉS - VERSIÓN CORREGIDA CON MULTI-MÉTRICAS"""
        print(f"🏆 GENERATING ENGLISH SQL FOR RANKING:")
        
        ranking = structure.ranking_criteria
        if not ranking:
            print(f"❌ Error: No English ranking criteria")
            return "SELECT * FROM datos;"
        
        # CONSTRUIR SELECT
        select_parts = []
        if structure.main_dimension:
            dim_name = structure.main_dimension.text
            formatted_dim = self.format_temporal_dimension(dim_name)
            select_parts.append(formatted_dim)
        
        order_by_parts = []
        
        # 🆕 RECOPILAR TODAS LAS MÉTRICAS (principal + adicionales)
        all_metrics = []
        all_operations = []
        
        # Métrica principal del ranking
        if ranking.metric:
            all_metrics.append(ranking.metric)
            if ranking.operation:
                all_operations.append(ranking.operation)
        
        # 🆕 IMPORTANTE: Agregar TODAS las métricas de la estructura
        print(f"   📊 Metrics in structure: {[m.text for m in structure.metrics]}")
        for metric in structure.metrics:
            # Evitar duplicados
            if not any(m.text == metric.text for m in all_metrics):
                all_metrics.append(metric)
                print(f"   📊 Adding additional metric: {metric.text}")
        
        # Agregar operaciones correspondientes
        print(f"   ⚡ Operations in structure: {[op.text for op in structure.operations]}")
        for op in structure.operations:
            if op.text.lower() not in ['top', 'bottom', 'best', 'worst']:  # Filtrar indicadores de ranking
                all_operations.append(op)
        
        # Asegurar que tenemos operaciones para todas las métricas
        while len(all_operations) < len(all_metrics):
            # Usar 'suma' como operación por defecto
            default_op = QueryComponent(
                text='total',
                type=ComponentType.OPERATION,
                value='suma',
                confidence=0.85
            )
            all_operations.append(default_op)
            print(f"   ⚡ Added default operation for metric")
        
        print(f"   📊 TOTAL METRICS TO PROCESS: {len(all_metrics)}")
        print(f"   📊 Metrics: {[m.text for m in all_metrics]}")
        print(f"   ⚡ Operations: {[op.value if hasattr(op, 'value') else op.text for op in all_operations]}")
        
        # GENERAR FUNCIONES SQL PARA CADA MÉTRICA
        for i, metric in enumerate(all_metrics):
            if i < len(all_operations):
                op = all_operations[i]
                operation_value = op.value if hasattr(op, 'value') else 'suma'
            else:
                operation_value = 'suma'
            
            # Para la métrica principal del ranking
            if i == 0:
                # Si es "more" o "most", interpretar como SUM
                if ranking.operation and ranking.operation.text.lower() in ['more', 'most', 'highest']:
                    agg_function = f'SUM({metric.text})'
                    print(f"   🏆 Primary ranking metric: 'more/most' → SUM")
                elif operation_value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, metric.text, operation_value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation_value, f'SUM({metric.text})')
            else:
                # Métricas adicionales - usar la operación correspondiente
                sql_operations = {
                    'mínimo': f'MIN({metric.text})',
                    'suma': f'SUM({metric.text})',
                    'promedio': f'AVG({metric.text})',
                    'conteo': f'COUNT({metric.text})',
                    'máximo': f'MAX({metric.text})'
                }
                agg_function = sql_operations.get(operation_value, f'SUM({metric.text})')
            
            select_parts.append(agg_function)
            
            # Solo la primera métrica define el ORDER BY
            if i == 0:
                if ranking.direction == RankingDirection.TOP:
                    order_direction = "DESC"
                else:
                    order_direction = "ASC"
                
                order_by_parts.append(f"{agg_function} {order_direction}")
                print(f"   ✅ Primary metric: {metric.text} → {agg_function} (ORDER BY {order_direction})")
            else:
                print(f"   ✅ Additional metric {i}: {metric.text} → {agg_function}")
        
        # CONSTRUIR WHERE
        where_conditions = []
        
        # Condiciones regulares
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
                print(f"   ✅ WHERE: {condition.column_name} = '{condition.value}'")
        
        # Exclusiones
        if hasattr(structure, 'exclusion_filters'):
            for exclusion in structure.exclusion_filters:
                if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                    where_conditions.append(f"{exclusion.column_name} != '{exclusion.value}'")
                    print(f"   🚫 Exclusion: {exclusion.column_name} != '{exclusion.value}'")
        
        # Filtros temporales
        print(f"🔧 DEBUG: Checking temporal filters...")
        print(f"🔧 DEBUG: structure.temporal_filters = {len(structure.temporal_filters)}")
        
        advanced_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
        print(f"🔧 DEBUG: advanced_conditions = {advanced_conditions}")
        
        if advanced_conditions:
            where_conditions.extend(advanced_conditions)
            print(f"   📅 English temporal conditions added: {advanced_conditions}")
        else:
            print(f"   ⏰ No temporal conditions found")
        
        # CONSTRUIR SQL FINAL
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            "FROM datos"
        ]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if structure.main_dimension:
            sql_parts.append(f"GROUP BY {structure.main_dimension.text}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # AGREGAR LIMIT BASADO EN EL VALOR DEL RANKING
        if ranking and hasattr(ranking, 'value') and ranking.value:
            if ranking.unit == RankingUnit.COUNT:
                limit_value = int(ranking.value)
                sql_parts.append(f"LIMIT {limit_value}")
                print(f"   🏆 Adding LIMIT {limit_value} for TOP {limit_value} ranking")
            elif ranking.unit == RankingUnit.PERCENTAGE:
                print(f"   🏆 Percentage ranking detected: {ranking.value}% - using default LIMIT 100")
                sql_parts.append("LIMIT 100")
        else:
            print(f"   ⚠️ No ranking value found, using default LIMIT 10")
            sql_parts.append("LIMIT 10")
        
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 English ranking SQL: {final_sql}")
        
        return final_sql


    def generate_temporal_conditional_sql_english(self, pattern_data: Dict) -> str:
        """🕐 GENERADOR SQL MEJORADO PARA PATRONES TEMPORALES CONDICIONALES"""
        print(f"🕐 GENERATING TEMPORAL CONDITIONAL SQL:")
        
        temporal_dim = pattern_data['temporal_dimension']
        entity_col = pattern_data.get('entity_column')
        entity_val = pattern_data.get('entity_value')
        target_metric = pattern_data.get('target_metric', 'Sell_Out')  # Default a Sell_Out
        order_direction = pattern_data.get('order_direction', 'DESC')
        
        print(f"   ⏰ Temporal: {temporal_dim}")
        if entity_col and entity_val:
            print(f"   🎯 Filter: {entity_col} = '{entity_val}'")
        print(f"   📊 Metric: {target_metric}")
        print(f"   🔄 Order: {order_direction}")
        
        # Mapear dimensión temporal a nombre de columna real
        temporal_column_map = {
            'week': 'Week',
            'month': 'Month',
            'day': 'Day',
            'year': 'Year',
            'quarter': 'Quarter'
        }
        
        temporal_column = temporal_column_map.get(temporal_dim, temporal_dim)
        
        # Mapear métricas comunes a nombres reales de columnas
        metric_column_map = {
            'sales': 'Sell_Out',
            'revenue': 'Sell_Out',
            'inventory': 'Inventory',
            'profit': 'profit',
            'margin': 'margin',
            'sell_out': 'Sell_Out',
            'stock': 'Inventory'
        }
        
        metric_column = metric_column_map.get(target_metric.lower(), target_metric)
        
        # Construir SQL con formato temporal si es necesario
        formatted_dim = self.format_temporal_dimension(temporal_column)
        
        # Si no hay entity_value específico, listar todas las semanas con el total
        if not entity_val:
            sql = f"""SELECT {formatted_dim}, SUM({metric_column}) as total_{target_metric}
    FROM datos
    GROUP BY {temporal_column}
    ORDER BY total_{target_metric} {order_direction}
    LIMIT 10;"""
        else:
            # Con filtro específico
            sql = f"""SELECT {formatted_dim}, SUM({metric_column}) as total_{target_metric}
    FROM datos
    WHERE {entity_col} = '{entity_val}'
    GROUP BY {temporal_column}
    ORDER BY total_{target_metric} {order_direction}
    LIMIT 10;"""
        
        # Limpiar el SQL (quitar saltos de línea extras)
        sql = ' '.join(sql.split())
        
        print(f"   🎯 Generated SQL: {sql}")
        return sql


    def validate_english_structure(self, structure: QueryStructure) -> Dict:
        """🇺🇸 VALIDACIÓN COMPLETA DE ESTRUCTURA INGLÉS - MEJORADA PARA LIST_ALL"""
        
        print(f"🔍 VALIDATING ENGLISH STRUCTURE:")
        print(f"   📋 Has list_all_pattern: {hasattr(structure, 'list_all_pattern')}")
        
        errors = []
        suggestions = []
        
        # 🆕 VALIDACIÓN ESPECIAL PARA LIST_ALL
        if hasattr(structure, 'list_all_pattern') and structure.list_all_pattern:
            print(f"   📋 LIST_ALL pattern detected - using special validation")
            
            target_dimension = structure.list_all_pattern.get('target_dimension')
            has_aggregation = structure.list_all_pattern.get('has_aggregation', False)
            
            if not target_dimension:
                errors.append("LIST_ALL pattern missing target dimension")
                suggestions.append("Specify what to list (e.g., 'list all accounts')")
            else:
                # Si tiene agregación, verificar que haya métricas
                if has_aggregation and not structure.metrics:
                    print(f"   ⚠️ LIST_ALL has aggregation indicator but no metrics found")
                    suggestions.append("Metrics detected in query but not properly identified")
                else:
                    print(f"   ✅ LIST_ALL validation passed - target: {target_dimension}")
                    return {
                        'valid': True,
                        'error': None,
                        'suggestions': []
                    }
                
#  VALIDACIÓN ESPECIAL PARA SHOW_ROWS
        if hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern:
            print(f"   📊 SHOW_ROWS pattern detected - using special validation")
            
            # Para SHOW_ROWS solo verificamos que tenga row_count válido
            row_count = structure.show_rows_pattern.get('row_count')
            
            if not row_count or row_count <= 0:
                errors.append("SHOW_ROWS pattern missing valid row count")
                suggestions.append("Specify number of rows (e.g., 'show first 100 rows')")
            elif row_count > 10000:  # Límite de seguridad
                errors.append("SHOW_ROWS pattern: row count too large (max 10000)")
                suggestions.append("Use a smaller number of rows")
            else:
                print(f"   ✅ SHOW_ROWS validation passed - count: {row_count}")
                return {
                    'valid': True,
                    'error': None,
                    'suggestions': []
                }                
                
#  VALIDACIÓN ESPECIAL PARA TEMPORAL CONDITIONAL
        if hasattr(structure, 'temporal_conditional_pattern') and structure.temporal_conditional_pattern:
            print(f"   🕐 TEMPORAL_CONDITIONAL pattern detected - using special validation")
            
            pattern = structure.temporal_conditional_pattern
            if (pattern.get('entity_column') and 
                pattern.get('entity_value') and 
                pattern.get('target_metric')):
                print(f"   ✅ TEMPORAL_CONDITIONAL validation passed")
                return {
                    'valid': True,
                    'error': None,
                    'suggestions': []
                }
            else:
                errors.append("TEMPORAL_CONDITIONAL pattern incomplete")
                suggestions.append("Include entity and metric (e.g., 'week where store X had most sales')")
                
        # VALIDACIÓN TRADICIONAL PARA OTROS PATRONES
        
        # NUEVA VALIDACIÓN: Permitir agregaciones globales
        if not structure.main_dimension:
            # Verificar si es una agregación global válida
            has_operations_and_metrics = structure.operations and structure.metrics
            
            if has_operations_and_metrics:
                print(f"   ✅ English global aggregation valid - no main dimension required")
            else:
                # Solo es error si NO es agregación global
                if structure.column_conditions:
                    available_columns = [cvp.column_name for cvp in structure.column_conditions]
                    suggestions.append(f"English columns detected: {', '.join(available_columns)}")
                errors.append("Missing main dimension")
                suggestions.append("Add an entity like: store, account, product, customer")
        
        # Validación para contenido significativo
        has_meaningful_content = (
            structure.metrics or 
            structure.operations or 
            structure.column_conditions or
            structure.temporal_filters
        )
        
        if not has_meaningful_content:
            errors.append("Missing metric, operation or condition")
            suggestions.append("Add a metric like: sales, revenue, inventory")
        
        # Advertencias para tokens desconocidos (pero NO como errores para LIST_ALL)
        if structure.unknown_tokens:
            unknown_words = [token.text for token in structure.unknown_tokens]
            # Para LIST_ALL, solo es sugerencia, no error
            if hasattr(structure, 'list_all_pattern') and structure.list_all_pattern:
                suggestions.append(f"English unrecognized words (non-critical): {', '.join(unknown_words)}")
            else:
                suggestions.append(f"English unrecognized words: {', '.join(unknown_words)}")
        
        final_result = {
            'valid': len(errors) == 0,
            'error': '; '.join(errors) if errors else None,
            'suggestions': suggestions
        }
        
        print(f"   🎯 Validation result: {final_result}")
        return final_result


    # ========================================================================
    # MÉTODOS DE SOPORTE ADICIONALES
    # ========================================================================

    def generate_natural_interpretation_english(self, structure: QueryStructure) -> str:
        """🇺🇸 GENERADOR DE INTERPRETACIÓN NATURAL EN INGLÉS - CON SOPORTE SHOW_ROWS"""
        
        # 🆕 CASO ESPECIAL: SHOW_ROWS
        if hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern:
            pattern = structure.show_rows_pattern
            position = pattern.get('position_type', '')
            count = pattern.get('row_count', 0)
            object_type = pattern.get('object_type', 'rows')
            
            if position:
                return f"Show the {position} {count} {object_type} from the table"
            else:
                return f"Show {count} {object_type} from the table"
        
        # CASO ESPECIAL: Rankings
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking = structure.ranking_criteria
            parts = []
            
            direction_text = "the best" if ranking.direction == RankingDirection.TOP else "the worst"
            
            # Verificar si hay dimensión principal
            dimension_text = structure.main_dimension.text if structure.main_dimension else "records"
            
            if ranking.unit == RankingUnit.COUNT:
                parts.append(f"Find {direction_text} {int(ranking.value)} {dimension_text}")
            else:  # PERCENTAGE
                parts.append(f"Find {direction_text} {ranking.value}% of {dimension_text}")
            
            if ranking.metric:
                if ranking.operation and ranking.operation.text.lower() in ['more', 'most', 'highest']:
                    parts.append(f"with highest total {ranking.metric.text}")
                elif ranking.operation and ranking.operation.text.lower() in ['less', 'least', 'lowest']:
                    parts.append(f"with lowest total {ranking.metric.text}")
                else:
                    parts.append(f"based on {ranking.metric.text}")
            
            # 🔧 FIX: Agregar filtros temporales con validación
            if structure.temporal_filters:
                for tf in structure.temporal_filters:
                    if tf.filter_type == "range_between":
                        # Usar start_value y end_value en lugar de quantity
                        start_val = getattr(tf, 'start_value', None)
                        end_val = getattr(tf, 'end_value', None)
                        
                        if start_val is not None and end_val is not None:
                            if tf.unit == TemporalUnit.WEEKS:
                                # Extraer solo el número de semana si es formato YYYYWW
                                if start_val > 1000:  # Es formato YYYYWW
                                    start_week = start_val % 100
                                    end_week = end_val % 100
                                else:
                                    start_week = start_val
                                    end_week = end_val
                                parts.append(f"between weeks {start_week} and {end_week}")
                            elif tf.unit == TemporalUnit.MONTHS:
                                parts.append(f"between months {start_val} and {end_val}")
                            elif tf.unit == TemporalUnit.DAYS:
                                parts.append(f"between days {start_val} and {end_val}")
                    elif tf.filter_type == "specific":
                        if tf.quantity is not None:  # Validar que quantity no sea None
                            if tf.unit == TemporalUnit.WEEKS:
                                # Extraer solo el número de semana si es formato YYYYWW
                                if tf.quantity > 1000:  # Es formato YYYYWW
                                    week_num = tf.quantity % 100
                                else:
                                    week_num = tf.quantity
                                parts.append(f"in week number {week_num}")
                            elif tf.unit == TemporalUnit.MONTHS:
                                parts.append(f"in month number {tf.quantity}")
                            elif tf.unit == TemporalUnit.DAYS:
                                parts.append(f"in day number {tf.quantity}")
                    elif tf.filter_type == "range":
                        if tf.quantity is not None:  # Validar que quantity no sea None
                            parts.append(f"in the {tf.indicator} {tf.quantity} {tf.unit.value}")
                    elif tf.filter_type == "current_week":
                        parts.append("in this week")
            
            # Agregar otros filtros
            if structure.column_conditions:
                conditions = []
                for condition in structure.column_conditions:
                    conditions.append(f"where {condition.column_name} = '{condition.value}'")
                parts.extend(conditions)
            
            interpretation = ", ".join(parts)
            return interpretation.capitalize() if interpretation else "English ranking query without clear interpretation"
        
        # LÓGICA PARA CONSULTAS NO-RANKING
        parts = []
        
        # Parte principal
        if structure.main_dimension:
            parts.append(f"Find {structure.main_dimension.text}")
        
        # Condiciones de columna
        if structure.column_conditions:
            conditions = []
            for condition in structure.column_conditions:
                conditions.append(f"where {condition.column_name} = '{condition.value}'")
            parts.append(", ".join(conditions))
        
        # Operación y métrica
        if structure.operations and structure.metrics:
            operation = structure.operations[0]
            metric = structure.metrics[0]
            
            if operation.value == 'máximo':
                parts.append(f"with the highest value in {metric.text}")
            elif operation.value == 'mínimo':
                parts.append(f"with the lowest value in {metric.text}")
            else:
                parts.append(f"calculating {operation.value} of {metric.text}")
        elif structure.operations:
            operation = structure.operations[0]
            parts.append(f"with {operation.value}")
        elif structure.metrics:
            metric = structure.metrics[0]
            parts.append(f"related to {metric.text}")
        
        # 🔧 FIX: Filtros temporales con validación
        if structure.temporal_filters:
            for tf in structure.temporal_filters:
                if tf.filter_type == "specific":
                    if tf.quantity is not None:  # Validar quantity
                        if tf.unit == TemporalUnit.WEEKS:
                            week_num = tf.quantity % 100 if tf.quantity > 1000 else tf.quantity
                            parts.append(f"in week number {week_num}")
                        elif tf.unit == TemporalUnit.MONTHS:
                            parts.append(f"in month number {tf.quantity}")
                elif tf.filter_type == "range":
                    if tf.quantity is not None:  # Validar quantity
                        parts.append(f"in the {tf.indicator} {tf.quantity} {tf.unit.value}")
                elif tf.filter_type == "range_between":
                    # Usar start_value y end_value
                    start_val = getattr(tf, 'start_value', None)
                    end_val = getattr(tf, 'end_value', None)
                    if start_val is not None and end_val is not None:
                        if tf.unit == TemporalUnit.WEEKS:
                            start_week = start_val % 100 if start_val > 1000 else start_val
                            end_week = end_val % 100 if end_val > 1000 else end_val
                            parts.append(f"between weeks {start_week} and {end_week}")
        
        interpretation = ", ".join(parts)
        return interpretation.capitalize() if interpretation else "English query without clear interpretation"

    # ========================================
    # MÉTODOS DE SOPORTE - VERSIONES LIMPIAS
    # ========================================

    def get_advanced_temporal_sql_conditions_english(self, structure: QueryStructure) -> List[str]:
        """🔧 VERSIÓN COMPLETA CON TODOS LOS CASOS TEMPORALES"""
        
        print(f"🔧 DEBUG EXTREMO: Método get_advanced_temporal_sql_conditions_english INICIADO")
        
        try:
            sql_conditions = []
            
            print(f"🔧 DEBUG EXTREMO: Inicializando sql_conditions = {sql_conditions}")
            
            print(f"⏰ GENERATING ADVANCED TEMPORAL CONDITIONS (With Special Patterns):")
            
            print(f"🔧 DEBUG EXTREMO: Verificando structure.temporal_filters...")
            print(f"🔧 DEBUG EXTREMO: hasattr(structure, 'temporal_filters') = {hasattr(structure, 'temporal_filters')}")
            
            if not hasattr(structure, 'temporal_filters'):
                print(f"❌ ERROR: structure no tiene temporal_filters")
                return []
            
            print(f"🔧 DEBUG EXTREMO: structure.temporal_filters = {structure.temporal_filters}")
            print(f"🔧 DEBUG EXTREMO: len(structure.temporal_filters) = {len(structure.temporal_filters)}")
            
            # 🔧 DEBUG: Mostrar todos los filtros temporales
            print(f"   📋 Total temporal filters in structure: {len(structure.temporal_filters)}")
            
            for i, tf in enumerate(structure.temporal_filters):
                print(f"🔧 DEBUG EXTREMO: Procesando filtro {i}: {tf}")
                print(f"🔧 DEBUG EXTREMO: tf.indicator = {tf.indicator}")
                print(f"🔧 DEBUG EXTREMO: tf.filter_type = {tf.filter_type}")
                print(f"🔧 DEBUG EXTREMO: tf.confidence = {tf.confidence}")
                print(f"   {i+1}. {tf.indicator} | {tf.filter_type} | {tf.confidence}")
            
    # STEP 1: PROCESAR FILTROS ESPECIALES EN structure.temporal_filters
            for i, tf in enumerate(structure.temporal_filters):
                print(f"🔧 DEBUG EXTREMO: Entrando al loop para filtro {i}")
                print(f"   📅 Procesando filtro: {tf.indicator} {tf.quantity} {tf.unit.value} (type: {tf.filter_type})")
                
                
        # CASO 1: THIS WEEK
                if tf.filter_type == "current_week":
                    print(f"🔧 DEBUG EXTREMO: Detectado current_week")
                    
                    if hasattr(self, 'generate_this_week_sql_condition'):
                        condition = self.generate_this_week_sql_condition()
                        sql_conditions.append(condition)
                        print(f"   📅 THIS WEEK condition: {condition}")
                    else:
                        # Crear condición manualmente
                        condition = "Week = (SELECT MAX(Week) FROM datos)"
                        sql_conditions.append(condition)
                        print(f"   📅 THIS WEEK condition (manual): {condition}")
               
                
        # CASO 2: SPECIFIC (week 5, month 3, etc.)
                elif tf.filter_type == "specific":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro specific")
                    if tf.unit == TemporalUnit.WEEKS:
                        condition = f"Week = {tf.quantity}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    elif tf.unit == TemporalUnit.MONTHS:
                        condition = f"Month = {tf.quantity}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    elif tf.unit == TemporalUnit.DAYS:
                        condition = f"Day = {tf.quantity}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                
                
        # CASO 3: RANGE (last X weeks/months/days)
                elif tf.filter_type == "range":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro range")
                    if tf.unit == TemporalUnit.WEEKS and tf.quantity:
                        # Calcular semana actual y restar
                        current_week = 202531  # O usar self.get_current_week()
                        start_week = current_week - tf.quantity + 1
                        condition = f"Week >= {start_week} AND Week <= {current_week}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition} (last {tf.quantity} weeks)")
                    elif tf.unit == TemporalUnit.MONTHS and tf.quantity:
                        condition = f"fecha >= DATE('now', '-{tf.quantity} months')"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    elif tf.unit == TemporalUnit.DAYS and tf.quantity:
                        condition = f"fecha >= DATE('now', '-{tf.quantity} days')"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                
                
        # CASO 4: RANGE BETWEEN (between weeks X and Y)
                elif tf.filter_type == "range_between":
                    print(f"🔧 DEBUG: Procesando filtro range_between")
                    print(f"🔧 DEBUG: Todos los atributos de tf: {vars(tf)}")
                    
                    # Verificar que los valores existen Y no son None
                    start_val = getattr(tf, 'start_value', None)
                    end_val = getattr(tf, 'end_value', None)
                    
                    print(f"🔧 DEBUG: start_value = {start_val}, end_value = {end_val}")
                    
                    if start_val is not None and end_val is not None:
                        if tf.unit == TemporalUnit.WEEKS:
                            # 🔧 CAMBIO: Usar >= y <= en lugar de BETWEEN
                            condition = f"Week >= {start_val} AND Week <= {end_val}"
                            sql_conditions.append(condition)
                            print(f"      ✅ SQL: {condition}")
                        elif tf.unit == TemporalUnit.MONTHS:
                            # 🔧 CAMBIO: Usar >= y <= en lugar de BETWEEN
                            condition = f"Month >= {start_val} AND Month <= {end_val}"
                            sql_conditions.append(condition)
                            print(f"      ✅ SQL: {condition}")
                        elif tf.unit == TemporalUnit.DAYS:
                            # 🔧 CAMBIO: Usar >= y <= en lugar de BETWEEN
                            condition = f"Day >= {start_val} AND Day <= {end_val}"
                            sql_conditions.append(condition)
                            print(f"      ✅ SQL: {condition}")
                    else:
                        print(f"      ❌ ERROR: start_value o end_value son None")
                                            
        # 🔧 CASO 5: SINCE (since week X)
                elif tf.filter_type == "since":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro since")
                    
                    if hasattr(tf, 'start_value') and tf.unit == TemporalUnit.WEEKS:
                        condition = f"Week >= {tf.start_value}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    elif hasattr(tf, 'start_value') and tf.unit == TemporalUnit.MONTHS:
                        condition = f"Month >= {tf.start_value}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    else:
                        print(f"      ❌ ERROR: since filter missing start_value")
                
        # 🔧 CASO 6: SINCE AGO (since X weeks ago)
                elif tf.filter_type == "since_ago":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro since_ago")
                    
                    if tf.unit == TemporalUnit.WEEKS and tf.quantity:
                        current_week = 202510  # usar self.get_current_week()
                        since_week = current_week - tf.quantity
                        condition = f"Week >= {since_week}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition} (since {tf.quantity} weeks ago)")
                    elif tf.unit == TemporalUnit.MONTHS and tf.quantity:
                        condition = f"fecha >= DATE('now', '-{tf.quantity} months')"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: {condition}")
                    else:
                        print(f"      ❌ ERROR: since_ago filter missing quantity")
                
        # 🔧 CASO 7: WEEK REFERENCE (para "week 5" = 202505)
                elif tf.filter_type == "week_reference":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro week_reference")
                    
                    if tf.unit == TemporalUnit.WEEKS and hasattr(tf, 'week_number'):
                        current_year = 2025
                        week_value = int(f"{current_year}{str(tf.week_number).zfill(2)}")
                        condition = f"Week = {week_value}"
                        sql_conditions.append(condition)
                        print(f"      ✅ SQL: Week = {week_value} (from 'week {tf.week_number}')")
                
        # 🔧 CASO 8: FROM TO (from week X to Y)
                elif tf.filter_type == "from_to":
                    print(f"🔧 DEBUG EXTREMO: Procesando filtro from_to")
                    
                    # Similar a range_between
                    if hasattr(tf, 'start_value') and hasattr(tf, 'end_value'):
                        if tf.unit == TemporalUnit.WEEKS:
                            # 🔧 CAMBIO: Usar >= y <= en lugar de BETWEEN
                            condition = f"Week >= {tf.start_value} AND Week <= {tf.end_value}"
                            sql_conditions.append(condition)
                            print(f"      ✅ SQL: {condition}")

        # STEP 2: PROCESAR advanced_temporal_info (RESPALDO)
        #     if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
        #         print(f"   🔍 Processing advanced_temporal_info: {len(self.advanced_temporal_info)} items")
                
        #         for ati in self.advanced_temporal_info:
        #             if ati.is_range_between and hasattr(ati, 'start_value') and hasattr(ati, 'end_value'):
        #                 if ati.original_filter.unit == TemporalUnit.WEEKS:
        #                     condition = f"Week BETWEEN {ati.start_value} AND {ati.end_value}"
        #                     if condition not in sql_conditions:
        #                         sql_conditions.append(condition)
        #                         print(f"      ✅ SQL from advanced_temporal_info: {condition}")
            else:
                print(f"   ⚠️  No advanced_temporal_info available")
            
            print(f"🔧 DEBUG EXTREMO: sql_conditions final = {sql_conditions}")
            print(f"⏰ TOTAL TEMPORAL CONDITIONS: {len(sql_conditions)}")
            
            if sql_conditions:
                print(f"   📝 Conditions generated: {sql_conditions}")
            else:
                print(f"   ❌ NO CONDITIONS GENERATED!")
            
            print(f"🔧 DEBUG EXTREMO: Retornando sql_conditions = {sql_conditions}")
            return sql_conditions
            
        except Exception as e:
            print(f"❌ ERROR EXTREMO en get_advanced_temporal_sql_conditions_english: {e}")
            import traceback
            print(f"❌ TRACEBACK: {traceback.format_exc()}")
            return []

    def _get_contextual_aggregation_english(self, structure: QueryStructure, metric_text: str, operation: str) -> str:
        """🇺🇸 Determina agregación contextual usando intent semántico para inglés"""
        
        if operation == 'máximo':
            original_intent = getattr(structure, 'original_semantic_intent', 'DEFAULT')
            
            print(f"   🎯 ENGLISH CONTEXTUAL AGGREGATION:")
            print(f"      📊 Metric: {metric_text}")
            print(f"      ⚡ Operation: {operation}")
            print(f"      🧠 Original Intent: {original_intent}")
            
            if original_intent == 'MAX':
                print(f"      ✅ INTENT → MAX({metric_text}) [singular context]")
                return f'MAX({metric_text})'
            elif original_intent == 'SUM':
                print(f"      ✅ INTENT → SUM({metric_text}) [plural context]")
                return f'SUM({metric_text})'
            else:
                print(f"      ✅ INTENT → SUM({metric_text}) [default for English]")
                return f'SUM({metric_text})'
        
        # Para otras operaciones
        sql_operations = {
            'mínimo': f'MIN({metric_text})',
            'suma': f'SUM({metric_text})',
            'promedio': f'AVG({metric_text})',
            'conteo': f'COUNT({metric_text})'
        }
        
        result = sql_operations.get(operation, f'SUM({metric_text})')
        print(f"   🎯 ENGLISH DIRECT MAPPING: {operation} → {result}")
        return result


    def build_english_structure(self, classified_components: Dict, column_value_pairs: List[ColumnValuePair], 
                                temporal_filters: List[TemporalFilter], tokens: List[str], original_intent: str) -> QueryStructure:
        """🇺🇸 CONSTRUCCIÓN DE ESTRUCTURA COMPLETA PARA INGLÉS - VERSIÓN CORREGIDA PARA PATRONES ESPECIALES"""
        
        print(f"🏗️ BUILDING COMPLETE ENGLISH QUERY STRUCTURE")
                
        # 🆕 PASO 0: DETECTAR PATRÓN GROUP BY PRIMERO
        groupby_dimension = self.detect_groupby_pattern_english(tokens)

        if groupby_dimension:
            print(f"   📍 GROUP BY dimension detected: {groupby_dimension.text}")
            # Remover filtros que coincidan con la dimensión de agrupación
            filtered_column_value_pairs = []
            for cvp in column_value_pairs:
                # Si el filtro es sobre la misma dimensión que el GROUP BY, no agregarlo
                if cvp.column_name.lower() != groupby_dimension.text.lower():
                    filtered_column_value_pairs.append(cvp)
                else:
                    print(f"   🔄 Removing conflicting filter: {cvp.column_name} = {cvp.value} (conflicts with GROUP BY {groupby_dimension.text})")
            
            column_value_pairs = filtered_column_value_pairs
            
        # 0.1: Detectar SHOW ROWS pattern
        show_rows_pattern = self.detect_show_rows_pattern_english(tokens)
        has_show_rows = show_rows_pattern is not None
        print(f"   📊 Show rows pattern detected: {has_show_rows}")
        
        # 0.2: Detectar LIST ALL pattern  
        list_all_pattern = self.detect_list_all_pattern_english(tokens)
        has_list_all = list_all_pattern is not None
        print(f"   📋 List all pattern detected: {has_list_all}")
        
        # 0.3: Detectar TEMPORAL CONDITIONAL pattern
        temporal_conditional_pattern = self.detect_temporal_conditional_pattern_english(tokens)
        has_temporal_conditional = temporal_conditional_pattern is not None
        print(f"   🕐 Temporal conditional pattern detected: {has_temporal_conditional}")
        
        # 🆕 PASO 1: Solo detectar ranking si NO hay patrones especiales prioritarios
        ranking_criteria = None
        exclusion_filters = []
        is_ranking = False
        
        if not (has_show_rows or has_list_all or has_temporal_conditional):
            # Solo procesar ranking si no hay patrones especiales
            ranking_criteria = self.detect_ranking_criteria_english(tokens, classified_components)
            exclusion_filters = self.detect_exclusion_filters_english(tokens, classified_components)
            is_ranking = self.is_ranking_query_english(ranking_criteria, exclusion_filters)
            print(f"   🏆 Ranking detected (no special patterns): {is_ranking}")
        else:
            print(f"   🏆 Skipping ranking detection due to special patterns")
            exclusion_filters = self.detect_exclusion_filters_english(tokens, classified_components)
        
        # PASO 1.5: Si hay ranking y no hay métrica, buscar o asignar métrica implícita
        if is_ranking and ranking_criteria:
            if not ranking_criteria.metric:
                # Buscar métricas en los componentes clasificados
                metric_found = False
                for token, component in classified_components.items():
                    if component.type == ComponentType.METRIC:
                        ranking_criteria.metric = component
                        print(f"   📊 Métrica encontrada para ranking: {component.text}")
                        metric_found = True
                        break
                
                # Si no hay métrica, usar default basado en contexto
                if not metric_found:
                    # Determinar métrica default basada en palabras clave
                    default_metric_name = 'Sell_Out'  # Default más común
                    
                    # Buscar pistas en los tokens
                    tokens_lower = [t.lower() for t in tokens]
                    if 'inventory' in tokens_lower:
                        default_metric_name = 'Inventory'
                    elif 'profit' in tokens_lower:
                        default_metric_name = 'profit'
                    elif 'margin' in tokens_lower:
                        default_metric_name = 'margin'
                    elif 'sales' in tokens_lower or 'sell' in tokens_lower:
                        default_metric_name = 'Sell_Out'
                    
                    default_metric = QueryComponent(
                        text=default_metric_name,
                        type=ComponentType.METRIC,
                        confidence=0.7,
                        subtype='default_ranking_metric',
                        linguistic_info={'source': 'default_for_ranking', 'reason': 'no_metric_specified'}
                    )
                    ranking_criteria.metric = default_metric
                    print(f"   📊 Usando métrica default para ranking: {default_metric_name}")
        
        # PASO 2: Detectar múltiples dimensiones
        multi_dimensions = self.detect_multi_dimensions_english(tokens, classified_components)
        is_multi_dimension = len(multi_dimensions) >= 2
        
        # PASO 3: Solo SI NO es ranking, procesar otros patrones
        if not is_ranking:
            compound_criteria = self.detect_compound_criteria_english(tokens, classified_components)
            is_compound = self.is_compound_query_english(compound_criteria)
        else:
            compound_criteria = []
            is_compound = False
        
        # PASO 4: Construir componentes básicos
        main_dimension = None
        operations = []
        metrics = []
        values = []
        connectors = []
        unknown_tokens = []
        
        # PASO 4.1: Si es ranking, buscar dimensión objetivo primero
        if is_ranking and ranking_criteria:
            # Buscar dimensiones típicas de ranking
            ranking_dimensions = ['account', 'accounts', 'store', 'stores', 'item', 'items', 
                                'product', 'products', 'customer', 'customers', 'brand', 'brands']
            
            for token, component in classified_components.items():
                token_lower = token.lower()
                if token_lower in ranking_dimensions or component.type == ComponentType.DIMENSION:
                    # Normalizar plural a singular
                    dimension_map = {
                        'accounts': 'account', 'stores': 'store', 'items': 'item',
                        'products': 'product', 'customers': 'customer', 'brands': 'brand'
                    }
                    
                    normalized_dim = dimension_map.get(token_lower, token_lower)
                    
                    main_dimension = QueryComponent(
                        text=normalized_dim,
                        type=ComponentType.DIMENSION,
                        confidence=0.95,
                        subtype='ranking_target',
                        linguistic_info={'source': 'ranking_dimension', 'original': token}
                    )
                    print(f"   📍 English ranking dimension: {normalized_dim} (from {token})")
                    break
        
        # PASO 4.1.5: Si es ranking, buscar métricas adicionales después de "and"
        if is_ranking and ranking_criteria:
            # Buscar si hay "and" en los tokens
            and_positions = [i for i, t in enumerate(tokens) if t.lower() == 'and']
            
            for and_pos in and_positions:
                # Buscar métricas después del "and"
                for i in range(and_pos + 1, len(tokens)):
                    token = tokens[i]
                    if token.lower() in ['inventory', 'profit', 'cost', 'margin', 'stock']:
                        # Crear componente métrica adicional
                        additional_metric = QueryComponent(
                            text=token.lower(),
                            type=ComponentType.METRIC,
                            confidence=0.9,
                            subtype='additional_ranking_metric',
                            linguistic_info={'source': 'ranking_additional_metric'}
                        )
                        metrics.append(additional_metric)
                        print(f"   📊 Additional ranking metric found: {token}")
                        
                        # Si hay "total" antes, agregar operación
                        if i > 0 and tokens[i-1].lower() == 'total':
                            total_op = QueryComponent(
                                text='total',
                                type=ComponentType.OPERATION,
                                confidence=0.9,
                                value='suma',
                                subtype='additional_operation',
                                linguistic_info={'source': 'ranking_additional_operation'}
                            )
                            operations.append(total_op)
                            print(f"   ⚡ Additional operation found: total")
        
        # PASO 4.2: Procesar componentes normalmente
        for token, component in classified_components.items():
            # Si ya tenemos main_dimension del ranking, skip dimensiones
            if is_ranking and main_dimension and component.type == ComponentType.DIMENSION:
                continue
                
            if component.type == ComponentType.DIMENSION and not main_dimension:
                main_dimension = component
                print(f"   📍 English main dimension: {component.text}")

            # 🆕 Si tenemos GROUP BY dimension, usarla como main_dimension
            if groupby_dimension and not main_dimension:
                main_dimension = groupby_dimension
                print(f"   📍 Using GROUP BY as main dimension: {main_dimension.text}")
            elif component.type == ComponentType.OPERATION:
                # No agregar "top" como operación si es ranking
                if not (is_ranking and component.text.lower() in ['top', 'bottom', 'best', 'worst', 'first', 'last']):
                    operations.append(component)
                    print(f"   ⚡ English operation: {component.text}")
            elif component.type == ComponentType.METRIC:
                metrics.append(component)
                print(f"   📊 English metric: {component.text}")
            elif component.type == ComponentType.VALUE:
                values.append(component)
            elif component.type == ComponentType.CONNECTOR:
                connectors.append(component)
            elif component.type == ComponentType.UNKNOWN:
                # No marcar números como unknown si son parte del ranking o show_rows
                if not ((is_ranking or has_show_rows) and token.isdigit()):
                    unknown_tokens.append(component)
        
        # PASO 5: Construir estructura completa
        # Preparar métricas finales
        final_metrics = []
        if is_ranking:
            # Si es ranking, incluir la métrica principal del ranking
            if ranking_criteria and ranking_criteria.metric:
                final_metrics.append(ranking_criteria.metric)
            # Y también incluir TODAS las métricas adicionales detectadas
            final_metrics.extend(metrics)
        else:
            # Si no es ranking, usar las métricas normales
            final_metrics = metrics

        structure = QueryStructure(
            main_dimension=main_dimension,
            main_dimensions=multi_dimensions if is_multi_dimension else ([main_dimension] if main_dimension else []),
            is_multi_dimension_query=is_multi_dimension,
            operations=operations,
            metrics=final_metrics,
            column_conditions=column_value_pairs,
            temporal_filters=temporal_filters,
            values=values,
            connectors=connectors,
            unknown_tokens=unknown_tokens,
            compound_criteria=compound_criteria,
            is_compound_query=is_compound,
            ranking_criteria=ranking_criteria,
            exclusion_filters=exclusion_filters,
            is_ranking_query=is_ranking,
            original_semantic_intent=original_intent
        )
        
        # 🆕 PASO 5.5: AGREGAR PATRONES ESPECIALES SI EXISTEN
        if show_rows_pattern:
            structure.show_rows_pattern = show_rows_pattern
            structure.is_ranking_query = False  # Forzar que NO sea ranking
            print(f"   📊 Structure marked as SHOW_ROWS (overriding ranking)")
        
        if list_all_pattern:
            structure.list_all_pattern = list_all_pattern
            if not has_show_rows:  # Solo si no hay show_rows
                structure.is_ranking_query = False
            print(f"   📋 Structure marked as LIST_ALL")
        
        if temporal_conditional_pattern:
            structure.temporal_conditional_pattern = temporal_conditional_pattern
            if not (has_show_rows or has_list_all):  # Solo si no hay otros
                structure.is_ranking_query = False
            print(f"   🕐 Structure marked as TEMPORAL_CONDITIONAL")
        
        # PASO 6: Detectar patrón de consulta
        query_pattern = self.detect_query_pattern_english(structure)
        structure.query_pattern = query_pattern
        
        # PASO 7: Configurar límites según el tipo de consulta
        if query_pattern == QueryPattern.TOP_N and structure.ranking_criteria:
            if structure.ranking_criteria.unit == RankingUnit.COUNT:
                structure.limit_value = int(structure.ranking_criteria.value)
            elif structure.ranking_criteria.unit == RankingUnit.PERCENTAGE:
                # Para porcentajes, necesitaremos calcularlo después
                structure.limit_value = None
            structure.is_single_result = False
            
            print(f"🏆 ENGLISH RANKING CONFIGURATION:")
            print(f"   📍 Target dimension: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
            print(f"   📊 Ranking metric: {structure.ranking_criteria.metric.text if structure.ranking_criteria.metric else 'N/A'}")
            print(f"   🎯 Direction: {structure.ranking_criteria.direction.value}")
            print(f"   📈 Unit: {structure.ranking_criteria.unit.value}")
            print(f"   🔢 Value: {structure.ranking_criteria.value}")
            
        elif query_pattern == QueryPattern.REFERENCED:
            structure.reference_metric = metrics[0] if metrics else None
            structure.is_single_result = True
            structure.limit_value = 1
            
            print(f"🎯 ENGLISH REFERENCED CONFIGURATION:")
            print(f"   📍 Target dimension: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
            print(f"   📊 Reference metric: {structure.reference_metric.text if structure.reference_metric else 'N/A'}")
        
        elif query_pattern == QueryPattern.TEMPORAL_CONDITIONAL:
            # Para temporal conditional, no necesitamos configuración especial aquí
            print(f"🕐 ENGLISH TEMPORAL CONDITIONAL CONFIGURATION:")
            print(f"   ⏰ Pattern will be handled by specialized generator")
        
        # PASO 8: Detectar patrón superlativo
        superlative_pattern = None
        # Solo si no hay otros patrones especiales
        if not (is_ranking or has_temporal_conditional or has_show_rows or has_list_all):
            superlative_pattern = self.detect_superlative_pattern_english(tokens)
            if superlative_pattern:
                # Configurar estructura para superlativo
                if not structure.main_dimension:
                    target_dim_component = QueryComponent(
                        text=superlative_pattern.target_dimension,
                        type=ComponentType.DIMENSION,
                        confidence=0.95,
                        subtype='superlative_target'
                    )
                    structure.main_dimension = target_dim_component
                
                # Agregar métrica implícita
                if superlative_pattern.implied_metric and not structure.metrics:
                    implied_metric_component = QueryComponent(
                        text=superlative_pattern.implied_metric,
                        type=ComponentType.METRIC,
                        confidence=0.85,
                        subtype='implied_from_verb'
                    )
                    structure.metrics.append(implied_metric_component)
                
                # Marcar como superlativo
                structure.superlative_pattern = superlative_pattern
                structure.is_superlative_query = True
                structure.query_pattern = QueryPattern.REFERENCED
                structure.is_single_result = True
                structure.limit_value = 1
        
        # PASO 9: Detectar y aplicar patrón COUNT
        structure = self.detect_and_apply_count_pattern(structure, tokens)
        
        print(f"🏗️ English structure built successfully:")
        print(f"   📊 Operations: {len(operations)}")
        print(f"   📈 Metrics: {len(structure.metrics)}")
        print(f"   🎯 Query pattern: {query_pattern.value}")
        print(f"   🏆 Is ranking: {is_ranking}")
        print(f"   🔗 Is compound: {is_compound}")
        print(f"   🔗 Is multi-dimensional: {is_multi_dimension}")
        print(f"   🕐 Has temporal conditional: {has_temporal_conditional}")
        print(f"   📋 Has list all: {has_list_all}")
        print(f"   📊 Has show rows: {has_show_rows}")
        
        return structure



    def detect_ranking_criteria_english(self, tokens: List[str], classified_components: Dict) -> Optional[RankingCriteria]:
        """🇺🇸 DETECTOR DE CRITERIOS DE RANKING EN INGLÉS - VERSIÓN SIMPLIFICADA QUE FUNCIONA"""
        print(f"🏆 DETECTING ENGLISH RANKING CRITERIA:")
        print(f"   🔤 Tokens: {tokens}")
        
        # Validación contextual: Verificar si es parte de SHOW_ROWS primero
        show_rows_indicators = {'rows', 'row', 'records', 'record', 'entries', 'lines'}
        
        for i in range(len(tokens) - 1):
            if tokens[i].lower() in ['first', 'last', 'top', 'bottom']:
                if i + 2 < len(tokens):
                    next_token = tokens[i + 1]
                    after_next = tokens[i + 2].lower() if i + 2 < len(tokens) else None
                    
                    if (next_token.isdigit() or next_token.lower() in self.dictionaries.numeros_palabras_en) and \
                    after_next in show_rows_indicators:
                        
                        # Verificar contexto adicional
                        has_metric_context = False
                        for j in range(i + 3, len(tokens)):
                            if tokens[j].lower() in ['with', 'having', 'by', 'sales', 'revenue', 'more', 'most', 'best']:
                                has_metric_context = True
                                break
                        
                        if not has_metric_context:
                            print(f"   ❌ Detected SHOW_ROWS pattern, not ranking: {tokens[i]} {next_token} {after_next}")
                            return None
        
        # English ranking indicators
        top_indicators = {'top', 'best', 'highest', 'maximum', 'first', 'greatest', 'most'}
        bottom_indicators = {'worst', 'lowest', 'minimum', 'last', 'least', 'bottom'}
        
        ranking_direction = None
        ranking_start_idx = -1
        
        for i, token in enumerate(tokens):
            token_lower = token.lower()
            
            if token_lower in top_indicators:
                ranking_direction = RankingDirection.TOP
                ranking_start_idx = i
                print(f"   🔝 TOP indicator found: '{token}' at position {i}")
                break
            elif token_lower in bottom_indicators:
                ranking_direction = RankingDirection.BOTTOM
                ranking_start_idx = i
                print(f"   📉 BOTTOM indicator found: '{token}' at position {i}")
                break
        
        if not ranking_direction:
            print(f"   ❌ No ranking indicators found")
            return None
        
        # Find ranking value
        ranking_value = None
        ranking_unit = None
        value_tokens = []
        
        search_end = min(ranking_start_idx + 4, len(tokens))
        
        for i in range(ranking_start_idx + 1, search_end):
            if i >= len(tokens):
                break
            
            token = tokens[i]
            
            # Percentage: "25%", "10.5%"
            if token.endswith('%'):
                try:
                    percent_value = float(token[:-1])
                    ranking_value = percent_value
                    ranking_unit = RankingUnit.PERCENTAGE
                    value_tokens.append(token)
                    print(f"   📊 Percentage detected: {percent_value}%")
                    break
                except ValueError:
                    continue
            
            # Number: "5", "10"
            elif token.isdigit():
                ranking_value = int(token)
                ranking_unit = RankingUnit.COUNT
                value_tokens.append(token)
                print(f"   🔢 Number detected: {ranking_value}")
                break
            
            # English number words: "five", "ten"
            elif token.lower() in self.dictionaries.numeros_palabras_en:
                ranking_value = self.dictionaries.numeros_palabras_en[token.lower()]
                ranking_unit = RankingUnit.COUNT
                value_tokens.append(token)
                print(f"   🔤 English number word detected: {token} = {ranking_value}")
                break
        
        # 🔧 FIX: ELIMINAR EL DEFAULT INTELIGENTE - SI NO HAY NÚMERO, NO ES RANKING VÁLIDO
        if ranking_value is None:
            print(f"   ❌ No numeric value found after indicator - not a valid ranking")
            return None
        
        # Find ranking metric and operation
        ranking_metric = None
        ranking_operation = None
        
        for token, component in classified_components.items():
            if component.type == ComponentType.METRIC and not ranking_metric:
                ranking_metric = component
                print(f"   📊 Ranking metric: {component.text}")
            elif component.type == ComponentType.OPERATION and not ranking_operation:
                # Filtrar indicadores de ranking que no son operaciones reales
                if component.text.lower() not in ['top', 'bottom', 'best', 'worst', 'first', 'last']:
                    ranking_operation = component
                    print(f"   ⚡ Ranking operation: {component.text}")
        
        # Si no hay métrica explícita, buscar en tokens restantes
        if not ranking_metric:
            metric_keywords = ['sales', 'revenue', 'profit', 'inventory', 'margin', 'cost']
            for i, token in enumerate(tokens):
                if token.lower() in metric_keywords:
                    implied_metric = QueryComponent(
                        text=token.lower(),
                        type=ComponentType.METRIC,
                        confidence=0.85,
                        subtype='implied_ranking_metric',
                        linguistic_info={'source': 'ranking_context_detection'}
                    )
                    ranking_metric = implied_metric
                    print(f"   📊 Implied ranking metric: {token}")
                    break
        
        # Calculate confidence
        confidence_factors = []
        base_confidence = 0.5
        
        base_confidence += 0.3  # Has ranking indicator
        confidence_factors.append("ranking_indicator")
        
        base_confidence += 0.2  # Has numeric value
        confidence_factors.append("numeric_value")
        
        if ranking_metric:
            base_confidence += 0.1
            confidence_factors.append("metric_found")
        
        if ranking_operation:
            base_confidence += 0.1
            confidence_factors.append("operation_found")
        
        final_confidence = min(1.0, base_confidence)
        
        raw_tokens = tokens[ranking_start_idx:ranking_start_idx + len(value_tokens) + 1] if value_tokens else [tokens[ranking_start_idx]]
        
        ranking_criteria = RankingCriteria(
            direction=ranking_direction,
            unit=ranking_unit,
            value=ranking_value,
            metric=ranking_metric,
            operation=ranking_operation,
            confidence=final_confidence,
            raw_tokens=raw_tokens
        )
        
        print(f"🏆 ENGLISH RANKING CRITERIA DETECTED:")
        print(f"   🎯 Direction: {ranking_direction.value}")
        print(f"   📊 Unit: {ranking_unit.value}")
        print(f"   🔢 Value: {ranking_value}")
        print(f"   📈 Metric: {ranking_metric.text if ranking_metric else 'N/A'}")
        print(f"   ⚡ Operation: {ranking_operation.text if ranking_operation else 'N/A'}")
        print(f"   ⭐ Confidence: {final_confidence:.2f}")
        
        return ranking_criteria  # 🔧 FIX: RETURN QUE FALTABA


    def detect_compound_criteria_english(self, tokens: List[str], classified_components: Dict) -> List[CompoundCriteria]:
        """🇺🇸 DETECTOR DE CONSULTAS COMPUESTAS EN INGLÉS"""
        print(f"🔗 DETECTING ENGLISH COMPOUND CRITERIA:")
        print(f"   🔤 Tokens: {tokens}")
        
        compound_criteria = []
        
        # Split by English connectors
        segments = self.split_by_connector_english(tokens, 'and')
        
        print(f"   📊 Segments detected: {segments}")
        
        for i, segment in enumerate(segments):
            print(f"\n   🎯 Processing English segment {i+1}: {segment}")
            
            criteria = self.extract_criteria_from_segment_english(segment, classified_components)
            if criteria:
                compound_criteria.append(criteria)
                print(f"      ✅ English criteria extracted: {criteria.operation.text} {criteria.metric.text}")
            else:
                print(f"      ❌ Could not extract criteria from segment")
        
        print(f"\n🔗 TOTAL ENGLISH COMPOUND CRITERIA: {len(compound_criteria)}")
        for i, criteria in enumerate(compound_criteria):
            print(f"   {i+1}. {criteria.operation.text} {criteria.metric.text} (confidence: {criteria.confidence:.2f})")
        
        return compound_criteria


    def split_by_connector_english(self, tokens: List[str], connector: str) -> List[List[str]]:
        """🇺🇸 DIVISOR POR CONECTORES EN INGLÉS"""
        segments = []
        current_segment = []
        
        for token in tokens:
            if token.lower() == connector.lower():
                if current_segment:
                    segments.append(current_segment)
                    current_segment = []
            else:
                current_segment.append(token)
        
        if current_segment:
            segments.append(current_segment)
        
        return segments


    def extract_criteria_from_segment_english(self, segment: List[str], classified_components: Dict) -> Optional[CompoundCriteria]:
        """🇺🇸 EXTRACTOR DE CRITERIOS DE SEGMENTOS EN INGLÉS"""
        operation_found = None
        metric_found = None
        dimension_candidate = None
        confidence_sum = 0.0
        count = 0
        
        print(f"      🔍 Analyzing English segment: {segment}")
        
        # PRIMERA PASADA: Buscar operaciones y métricas REALES
        for token in segment:
            if token in classified_components:
                component = classified_components[token]
                
                # Buscar operación
                if component.type == ComponentType.OPERATION and not operation_found:
                    operation_found = component
                    confidence_sum += component.confidence
                    count += 1
                    print(f"         ⚡ English operation found: {token}")
                
                # Priorizar métricas reales
                elif component.type == ComponentType.METRIC and not metric_found:
                    metric_found = component
                    confidence_sum += component.confidence
                    count += 1
                    print(f"         📊 English REAL metric found: {token}")
                
                # Guardar dimensión como candidato
                elif component.type == ComponentType.DIMENSION and not dimension_candidate:
                    dimension_candidate = component
                    print(f"         📍 English dimension candidate: {token}")
        
        # SEGUNDA PASADA: Solo si NO hay métrica real, usar dimensión
        if not metric_found and dimension_candidate:
            metric_component = QueryComponent(
                text=dimension_candidate.text,
                type=ComponentType.METRIC,
                confidence=dimension_candidate.confidence * 0.85,
                subtype='converted_from_dimension',
                value=dimension_candidate.value,
                column_name=dimension_candidate.column_name,
                linguistic_info={'converted_from': 'dimension'}
            )
            metric_found = metric_component
            confidence_sum += metric_component.confidence
            count += 1
            print(f"         🔄 English dimension converted to metric: {dimension_candidate.text}")
        
        # VALIDACIÓN FINAL
        if operation_found and metric_found:
            avg_confidence = confidence_sum / count if count > 0 else 0.0
            
            print(f"         ✅ English criteria complete: {operation_found.text} + {metric_found.text}")
            
            return CompoundCriteria(
                operation=operation_found,
                metric=metric_found,
                confidence=avg_confidence,
                raw_tokens=segment
            )
        
        print(f"         ❌ English criteria incomplete:")
        print(f"             Operation: {operation_found.text if operation_found else 'NOT FOUND'}")
        print(f"             Metric: {metric_found.text if metric_found else 'NOT FOUND'}")
        
        return None


    def detect_multi_dimensions_english(self, tokens: List[str], classified_components: Dict) -> List[QueryComponent]:
        """🇺🇸 DETECTOR DE MÚLTIPLES DIMENSIONES EN INGLÉS"""
        
        print(f"🔗 DETECTING ENGLISH MULTIPLE DIMENSIONS:")
        
        # PASO 1: Identificar dimensiones y conectores
        dimension_candidates = []
        connector_positions = []
        
        for i, token in enumerate(tokens):
            if token in classified_components:
                component = classified_components[token]
                if component.type == ComponentType.DIMENSION:
                    dimension_candidates.append((i, component))
                elif (component.type == ComponentType.CONNECTOR and 
                    token.lower() in ['and', 'with', ',']):
                    connector_positions.append(i)
        
        print(f"   📍 English dimensions found: {[(i, comp.text) for i, comp in dimension_candidates]}")
        print(f"   🔗 English connectors at positions: {connector_positions}")
        
        # PASO 2: Validar patrón secuencial
        if len(dimension_candidates) >= 2 and len(connector_positions) >= 1:
            valid_dimensions = self._validate_dimension_sequence_english(
                dimension_candidates, connector_positions, tokens
            )
            
            if len(valid_dimensions) >= 2:
                print(f"   ✅ ENGLISH MULTIPLE DIMENSIONS valid: {[d.text for d in valid_dimensions]}")
                return valid_dimensions
        
        print(f"   ❌ No valid English multi-dimensional pattern detected")
        return []


    def _validate_dimension_sequence_english(self, dimension_candidates: List, connector_positions: List, tokens: List[str]) -> List[QueryComponent]:
        """🇺🇸 VALIDADOR DE SECUENCIA DIMENSIONAL EN INGLÉS"""
        valid_dimensions = []
        
        for i, (pos, component) in enumerate(dimension_candidates):
            if i == 0:
                # Primera dimensión siempre válida
                valid_dimensions.append(component)
            else:
                # Verificar que hay conector antes de esta dimensión
                prev_dim_pos = dimension_candidates[i-1][0]
                has_connector_between = any(
                    prev_dim_pos < conn_pos < pos 
                    for conn_pos in connector_positions
                )
                
                if has_connector_between:
                    valid_dimensions.append(component)
                    print(f"      ✅ English '{component.text}' valid (connector found)")
                else:
                    print(f"      ❌ English '{component.text}' invalid (no connector)")
                    break
        
        return valid_dimensions


    def detect_exclusion_filters_english(self, tokens: List[str], classified_components: Dict) -> List[ExclusionFilter]:
        """🇺🇸 DETECTOR DE FILTROS DE EXCLUSIÓN EN INGLÉS"""
        print(f"🚫 DETECTING ENGLISH EXCLUSION FILTERS:")
        
        exclusion_filters = []
        
        # English exclusion indicators
        exclusion_indicators = {
            'excluding', 'except', 'without', 'not', 'minus', 'omitting', 'excluding'
        }
        
        # Buscar indicadores de exclusión
        for i, token in enumerate(tokens):
            token_lower = token.lower()
            
            if token_lower in exclusion_indicators:
                print(f"   🚫 English exclusion indicator found: '{token}' at position {i}")
                
                # Buscar patrón [COLUMNA] [VALOR] después del indicador
                exclusion_filter = self.extract_exclusion_from_position_english(tokens, i + 1, classified_components)
                
                if exclusion_filter:
                    exclusion_filters.append(exclusion_filter)
                    print(f"   ✅ English exclusion filter extracted: {exclusion_filter.column_name} != '{exclusion_filter.value}'")
        
        print(f"🚫 TOTAL ENGLISH EXCLUSION FILTERS: {len(exclusion_filters)}")
        return exclusion_filters


    def extract_exclusion_from_position_english(self, tokens: List[str], start_pos: int, classified_components: Dict) -> Optional[ExclusionFilter]:
        """🇺🇸 EXTRACTOR DE EXCLUSIONES POSICIONALES EN INGLÉS"""
        if start_pos >= len(tokens) - 1:
            return None
        
        # Buscar patrón [COLUMNA] [VALOR] en las siguientes posiciones
        search_end = min(start_pos + 3, len(tokens))
        
        for i in range(start_pos, search_end - 1):
            if i + 1 >= len(tokens):
                break
            
            current_token = tokens[i]
            next_token = tokens[i + 1]
            
            print(f"      🔍 Analyzing English exclusion: '{current_token}' + '{next_token}'")
            
            # Verificar si current_token es una columna potencial
            if self._is_potential_column_english(current_token):
                # Verificar si next_token es un valor
                if self._is_potential_value_english(next_token):
                    # Construir filtro de exclusión
                    confidence = 0.8  # Base confidence for exclusions
                    
                    return ExclusionFilter(
                        exclusion_type=ExclusionType.NOT_EQUALS,
                        column_name=current_token.lower(),
                        value=next_token.upper(),
                        confidence=confidence,
                        raw_tokens=tokens[start_pos-1:i+2]
                    )
        
        return None


    def detect_query_pattern_english(self, structure: QueryStructure) -> QueryPattern:
        """🇺🇸 DETECTOR DE PATRÓN DE CONSULTA EN INGLÉS - PRIORIDAD CORREGIDA"""
        print(f"🔍 DETECTING ENGLISH QUERY PATTERN:")
        print(f"   📍 Dimension: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
        print(f"   🔗 Multiple dimensions: {len(structure.main_dimensions) if structure.main_dimensions else 0}")
        print(f"   ⚡ Operations: {[op.text for op in structure.operations]}")
        print(f"   📊 Metrics: {[m.text for m in structure.metrics]}")
        print(f"   🏆 Is ranking: {structure.is_ranking_query}")
        print(f"   🔗 Is compound: {structure.is_compound_query}")
        print(f"   📐 Is multi-dimensional: {structure.is_multi_dimension_query}")

        # 🆕 PRIORIDAD 0: PATRONES ESPECIALES (máxima prioridad)
        
        # SHOW ROWS tiene la máxima prioridad
        if hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern:
            print(f"   📊 ENGLISH PATTERN: SHOW_ROWS (special pattern priority)")
            structure.confidence_score = structure.show_rows_pattern.get('confidence', 0.9)
            return QueryPattern.SHOW_ROWS
        
        # LIST ALL tiene segunda prioridad
        if hasattr(structure, 'list_all_pattern') and structure.list_all_pattern:
            print(f"   📋 ENGLISH PATTERN: LIST_ALL (special pattern priority)")
            structure.confidence_score = structure.list_all_pattern.get('confidence', 0.8)
            return QueryPattern.LIST_ALL
        
        # TEMPORAL CONDITIONAL tiene tercera prioridad
        if hasattr(structure, 'temporal_conditional_pattern') and structure.temporal_conditional_pattern:
            print(f"   🕒 ENGLISH PATTERN: TEMPORAL_CONDITIONAL (special pattern priority)")
            structure.confidence_score = structure.temporal_conditional_pattern.get('confidence', 0.8)
            return QueryPattern.TEMPORAL_CONDITIONAL
        
        # PATRÓN 1: RANKING (incluyendo multi-dimensionales)
        if structure.is_ranking_query and structure.ranking_criteria:
            confidence = self.calculate_ranking_confidence_english(structure)
            if confidence >= 0.7:
                print(f"   🏆 ENGLISH PATTERN: TOP_N (ranking, confidence: {confidence:.2f})")
                structure.confidence_score = confidence
                return QueryPattern.TOP_N
        
        # PATRÓN 2: MÚLTIPLES DIMENSIONES SIN RANKING
        if (structure.is_multi_dimension_query and 
            len(structure.main_dimensions) >= 2 and 
            not structure.is_ranking_query):
            confidence = self.calculate_multi_dimension_confidence_english(structure)
            if confidence >= 0.7:
                print(f"   🔗 ENGLISH PATTERN: MULTI_DIMENSION (confidence: {confidence:.2f})")
                structure.confidence_score = confidence
                return QueryPattern.MULTI_DIMENSION
        
        # PATRÓN 3: CONSULTAS COMPUESTAS REFERENCIADAS
        if (structure.is_compound_query and 
            structure.main_dimension and 
            len(structure.compound_criteria) >= 2):
            
            all_reference_operations = True
            reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
            
            for criteria in structure.compound_criteria:
                if criteria.operation.value not in reference_operations:
                    all_reference_operations = False
                    break
            
            if all_reference_operations:
                confidence = self.calculate_compound_reference_confidence_english(structure)
                if confidence >= 0.7:
                    print(f"   🎯 ENGLISH PATTERN: REFERENCED (compound, confidence: {confidence:.2f})")
                    structure.confidence_score = confidence
                    return QueryPattern.REFERENCED
        
        # PATRÓN 4: DATOS REFERENCIADOS SIMPLES
        if (structure.main_dimension and 
            len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1 and 
            len(structure.column_conditions) == 0 and
            not structure.is_ranking_query):
            
            operation = structure.operations[0]
            reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
            
            if operation.value in reference_operations:
                confidence = self.calculate_reference_confidence_english(structure)
                if confidence >= 0.7:
                    print(f"   🎯 ENGLISH PATTERN: REFERENCED (simple, confidence: {confidence:.2f})")
                    structure.confidence_score = confidence
                    return QueryPattern.REFERENCED
        
        # PATRÓN 5: AGREGACIÓN COMPLETA
        if (len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1 and 
            not structure.main_dimension):
            
            print(f"   📊 ENGLISH PATTERN: AGGREGATION (global)")
            structure.confidence_score = 0.90
            return QueryPattern.AGGREGATION
        
        # PATRÓN 6: AGREGACIÓN CON DIMENSIÓN
        if (structure.main_dimension and 
            len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1):
            
            print(f"   📊 ENGLISH PATTERN: AGGREGATION (with grouping)")
            structure.confidence_score = 0.85
            return QueryPattern.AGGREGATION
        
        # PATRÓN 7: LISTAR TODOS
        if (structure.main_dimension and 
            len(structure.operations) == 0):
            
            print(f"   📋 ENGLISH PATTERN: LIST_ALL")
            structure.confidence_score = 0.80
            return QueryPattern.LIST_ALL
        
        # PATRÓN: Metrica de Valor → Dimensión implícita
        if (not structure.main_dimension and 
            structure.metrics and 
            len(structure.column_conditions) > 0):
            
            # Si tenemos métrica + filtros pero no dimensión
            # La dimensión implícita es la columna del primer filtro
            first_filter = structure.column_conditions[0]
            
            # Crear dimensión implícita basada en el filtro
            if first_filter.column_name.lower() in ['item', 'store', 'account', 'brand']:
                implicit_dimension = QueryComponent(
                    text=first_filter.column_name.lower(),
                    type=ComponentType.DIMENSION,
                    confidence=0.85,
                    subtype='implicit_from_filter',
                    linguistic_info={'source': 'implicit_dimension_from_filter'}
                )
                
                structure.main_dimension = implicit_dimension
                
                print(f"   🎯 IMPLICIT DIMENSION from filter: {first_filter.column_name}")
                print(f"   📊 ENGLISH PATTERN: AGGREGATION (metric of value)")
                structure.confidence_score = 0.8
                return QueryPattern.AGGREGATION
        
        print(f"   ❓ ENGLISH PATTERN: UNKNOWN")
        structure.confidence_score = 0.4
        return QueryPattern.UNKNOWN


    def is_ranking_query_english(self, ranking_criteria: Optional[RankingCriteria], exclusion_filters: List[ExclusionFilter]) -> bool:
        """🇺🇸 VERIFICADOR DE CONSULTA DE RANKING EN INGLÉS"""
        has_valid_ranking = ranking_criteria and ranking_criteria.confidence >= 0.6
        is_ranking = bool(has_valid_ranking)
        
        print(f"🏆 EVALUATING ENGLISH RANKING QUERY:")
        print(f"   📊 Has valid criteria: {has_valid_ranking}")
        print(f"   🚫 Exclusion filters: {len(exclusion_filters)}")
        print(f"   🎯 Is ranking: {is_ranking}")
        
        return is_ranking


    def is_compound_query_english(self, compound_criteria: List[CompoundCriteria]) -> bool:
        """🇺🇸 VERIFICADOR DE CONSULTA COMPUESTA EN INGLÉS"""
        valid_criteria = [c for c in compound_criteria if c.confidence >= 0.6]
        is_compound = len(valid_criteria) >= 2
        
        print(f"🔗 EVALUATING ENGLISH COMPOUND QUERY:")
        print(f"   📊 Valid criteria: {len(valid_criteria)}")
        print(f"   🎯 Is compound: {is_compound}")
        
        return is_compound


    def calculate_ranking_confidence_english(self, structure: QueryStructure) -> float:
        """🇺🇸 CALCULADOR DE CONFIANZA DE RANKING EN INGLÉS"""
        print(f"   🔍 CALCULATING ENGLISH RANKING CONFIDENCE:")
        
        if not structure.ranking_criteria:
            return 0.0
        
        base_confidence = structure.ranking_criteria.confidence
        factors = ['base_criteria']
        
        # Factor 1: Tiene dimensión principal
        if structure.main_dimension:
            base_confidence += 0.1
            factors.append("has_dimension")
        
        # Factor 2: Tipo de unidad
        if structure.ranking_criteria.unit == RankingUnit.PERCENTAGE:
            base_confidence += 0.05
            factors.append("uses_percentage")
        elif structure.ranking_criteria.unit == RankingUnit.COUNT:
            base_confidence += 0.03
            factors.append("uses_count")
        
        # Factor 3: Tiene métrica específica
        if structure.ranking_criteria.metric:
            base_confidence += 0.05
            factors.append("specific_metric")
        
        # Factor 4: Valor razonable
        if structure.ranking_criteria.unit == RankingUnit.COUNT and 1 <= structure.ranking_criteria.value <= 50:
            base_confidence += 0.03
            factors.append("reasonable_count_value")
        elif structure.ranking_criteria.unit == RankingUnit.PERCENTAGE and 1 <= structure.ranking_criteria.value <= 100:
            base_confidence += 0.03
            factors.append("reasonable_percentage_value")
        
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 English factors applied: {factors}")
        print(f"      ⭐ English confidence: {final_confidence:.2f}")
        
        return final_confidence


    def calculate_multi_dimension_confidence_english(self, structure: QueryStructure) -> float:
        """🇺🇸 CALCULADOR DE CONFIANZA MULTI-DIMENSIONAL EN INGLÉS"""
        print(f"   🔍 CALCULATING ENGLISH MULTI-DIMENSION CONFIDENCE:")
        
        base_confidence = 0.6
        factors = ['base_multi_dimension']
        
        # Factor 1: Número de dimensiones
        extra_dims = len(structure.main_dimensions) - 2
        if extra_dims > 0:
            bonus = min(extra_dims * 0.05, 0.15)
            base_confidence += bonus
            factors.append(f"extra_dimensions_{extra_dims}")
        
        # Factor 2: Tiene operación y métrica
        if structure.operations and structure.metrics:
            base_confidence += 0.2
            factors.append("operation_metric")
        
        # Factor 3: Sin filtros complejos
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("no_complex_filters")
        
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 English factors applied: {factors}")
        print(f"      ⭐ English confidence: {final_confidence:.2f}")
        
        return final_confidence


    def calculate_reference_confidence_english(self, structure: QueryStructure) -> float:
        """🇺🇸 CALCULADOR DE CONFIANZA REFERENCIAL EN INGLÉS"""
        print(f"   🔍 CALCULATING ENGLISH REFERENCE CONFIDENCE:")
        
        base_confidence = 0.5
        factors = []
        
        # Factor 1: Tiene dimensión
        if structure.main_dimension:
            base_confidence += 0.15
            factors.append("has_dimension")
        
        # Factor 2: Operación única
        if len(structure.operations) == 1:
            base_confidence += 0.1
            factors.append("single_operation")
        
        # Factor 3: Métrica única
        if len(structure.metrics) == 1:
            base_confidence += 0.1
            factors.append("single_metric")
        
        # Factor 4: Sin filtros de columna
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("no_column_filters")
        
        # Factor 5: Operación de comparación
        if structure.operations and structure.operations[0].value in ['máximo', 'mínimo']:
            base_confidence += 0.2
            factors.append("comparison_operation")
        
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 English factors applied: {factors}")
        print(f"      ⭐ English confidence: {final_confidence:.2f}")
        
        return final_confidence


    def calculate_compound_reference_confidence_english(self, structure: QueryStructure) -> float:
        """🇺🇸 CALCULADOR DE CONFIANZA COMPUESTA EN INGLÉS"""
        print(f"   🔍 CALCULATING ENGLISH COMPOUND REFERENCE CONFIDENCE:")
        
        base_confidence = 0.6
        factors = []
        
        # Factor 1: Tiene dimensión
        if structure.main_dimension:
            base_confidence += 0.1
            factors.append("has_dimension")
        
        # Factor 2: Número de criterios válidos
        valid_criteria = len([c for c in structure.compound_criteria if c.confidence >= 0.7])
        criteria_bonus = min(valid_criteria * 0.05, 0.15)
        base_confidence += criteria_bonus
        factors.append(f"valid_criteria_{valid_criteria}")
        
        # Factor 3: Sin filtros de columna
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("no_column_filters")
        
        # Factor 4: Todas las operaciones son de comparación
        reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
        all_reference = all(
            criteria.operation.value in reference_operations 
            for criteria in structure.compound_criteria
        )
        if all_reference:
            base_confidence += 0.1
            factors.append("all_comparison_operations")
        
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 English factors applied: {factors}")
        print(f"      ⭐ English confidence: {final_confidence:.2f}")
        
        return final_confidence


    def generate_multi_dimension_english_sql(self, structure: QueryStructure, temporal_columns: set) -> str:
        """🔧 GENERADOR SQL PARA MÚLTIPLES DIMENSIONES - VERSIÓN CORREGIDA"""
        print(f"🔗 GENERANDO SQL PARA MÚLTIPLES DIMENSIONES:")
        
        select_parts = []
        group_by_parts = []
        order_by_parts = []
        where_conditions = []
        
        # PASO 1: Agregar todas las dimensiones principales
        for dimension in structure.main_dimensions:
            dim_name = dimension.text
            formatted_dim = self.format_temporal_dimension(dim_name)
            select_parts.append(formatted_dim)
            group_by_parts.append(dim_name)  
                
        # PASO 2: 🔧 BUSCAR LA MÉTRICA CORRECTA PARA EL RANKING
        ranking_metric = None
        operation_value = None
        
        # Prioridad 1: Métrica especificada en ranking_criteria
        if structure.ranking_criteria and structure.ranking_criteria.metric:
            ranking_metric = structure.ranking_criteria.metric
            print(f"   📊 Métrica del ranking: {ranking_metric.text}")
        
        # Prioridad 2: Buscar métricas reales (NO convertidas de dimensiones)
        else:
            real_metrics = [
                m for m in structure.metrics 
                if not m.linguistic_info.get('converted_from') == 'dimension'
            ]
            
            if real_metrics:
                ranking_metric = real_metrics[0]
                print(f"   📊 Métrica real encontrada: {ranking_metric.text}")
            else:
                # Fallback: usar la primera métrica disponible
                if structure.metrics:
                    ranking_metric = structure.metrics[0]
                    print(f"   📊 Métrica fallback: {ranking_metric.text}")
        
        # PASO 3: Determinar operación
        if structure.operations:
            # Buscar operación relevante (no ranking indicators)
            relevant_operations = [
                op for op in structure.operations 
                if op.value not in ['top', 'bottom'] and op.subtype != 'ranking_indicator'
            ]
            
            if relevant_operations:
                operation = relevant_operations[0]
                operation_value = operation.value
                print(f"   ⚡ Operación relevante: {operation.text} → {operation_value}")
            else:
                # Si solo hay indicadores de ranking, usar operación por defecto
                operation_value = 'suma'  # Por defecto para rankings
                print(f"   ⚡ Usando operación por defecto: suma")
        else:
            operation_value = 'suma'
            print(f"   ⚡ Sin operaciones, usando por defecto: suma")
        
        # PASO 4: Construir función de agregación
        if ranking_metric:
            if operation_value == 'máximo':
                agg_function = self._get_contextual_aggregation_english(structure, ranking_metric.text, operation_value)
            else:
                sql_operations = {
                    'mínimo': f'MIN({ranking_metric.text})',
                    'suma': f'SUM({ranking_metric.text})',
                    'promedio': f'AVG({ranking_metric.text})',
                    'conteo': f'COUNT({ranking_metric.text})'
                }
                agg_function = sql_operations.get(operation_value, f'SUM({ranking_metric.text})')
            
            select_parts.append(agg_function)
            
            # Determinar orden basado en ranking
            if structure.ranking_criteria:
                if structure.ranking_criteria.direction == RankingDirection.TOP:
                    order_direction = "DESC"
                else:
                    order_direction = "ASC"
            else:
                # Determinar orden basado en operación
                if operation_value in ['máximo', 'mayor']:
                    order_direction = "DESC"
                elif operation_value in ['mínimo', 'menor']:
                    order_direction = "ASC"
                else:
                    order_direction = "DESC"
            
            order_by_parts.append(f"{agg_function} {order_direction}")
            print(f"   📊 Agregación: {agg_function} {order_direction}")
        else:
            print(f"   ❌ No se encontró métrica válida para el ranking")
            return "SELECT * FROM datos;"
        
        # PASO 5: WHERE conditions
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
        
        # PASO 6: Filtros temporales
        advanced_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
        where_conditions.extend(advanced_conditions)
        
        # PASO 7: Construir SQL final
        sql_parts = [f"SELECT {', '.join(select_parts)}", "FROM datos"]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # PASO 8: Aplicar límite
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking_value = int(structure.ranking_criteria.value)
            sql_parts.append(f"LIMIT {ranking_value}")
            print(f"   🏆 APLICANDO LIMIT de ranking: {ranking_value}")
        else:
            sql_parts.append("LIMIT 10")  # Límite por defecto más razonable
            print(f"   📍 APLICANDO LIMIT por defecto: 10")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 SQL multi-dimensional: {final_sql}")
        
        return final_sql


    def _get_contextual_aggregation_english(self, structure: QueryStructure, metric_text: str, operation: str) -> str:
        """Usar intent semántico original (pre-mapeo) para decidir SUM vs MAX"""
        
        if operation == 'máximo':
            # 🎯 USAR INTENT ORIGINAL (analizado ANTES del mapeo)
            original_intent = getattr(structure, 'original_semantic_intent', 'DEFAULT')
            
            if original_intent == 'MAX':
                print(f"   🎯 INTENT ORIGINAL: MAX → MAX({metric_text}) [palabras originales singulares]")
                return f'MAX({metric_text})'
            elif original_intent == 'SUM':
                print(f"   🎯 INTENT ORIGINAL: SUM → SUM({metric_text}) [palabras originales plurales]")
                return f'SUM({metric_text})'
            else:
                print(f"   🎯 INTENT ORIGINAL: DEFAULT → SUM({metric_text}) [configuración por defecto]")
                return f'SUM({metric_text})'  # Tu configuración por defecto
        
        return f'SUM({metric_text})'  

                
    def generate_optimized_sql_english(self, structure: QueryStructure) -> str:
        """
        🇺🇸 GENERADOR SQL OPTIMIZADO CON LIST ALL MEJORADO Y MULTI-MÉTRICAS
        
        Versión mejorada del método existente - mantiene el mismo nombre
        """
        
        print(f"🔧 GENERATING OPTIMIZED SQL (Enhanced with LIST ALL support):")
        
        # 🆕 FAST PATH PARA MULTI-MÉTRICAS COMPUESTAS - CORREGIDO
        if (structure.is_compound_query and 
            len(structure.compound_criteria) > 1 and 
            not structure.main_dimension):
            
            print(f"📊 MULTI-METRIC COMPOUND detected - using fast path")
            
            select_parts = []
            for criteria in structure.compound_criteria:
                if criteria.operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, criteria.metric.text, criteria.operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({criteria.metric.text})',
                        'suma': f'SUM({criteria.metric.text})',
                        'promedio': f'AVG({criteria.metric.text})',
                        'conteo': f'COUNT({criteria.metric.text})',
                        'total': f'SUM({criteria.metric.text})'  # 🆕 Mapeo para 'total'
                    }
                    agg_function = sql_operations.get(criteria.operation.value, f'SUM({criteria.metric.text})')
                
                select_parts.append(agg_function)
                print(f"   ✅ Added: {agg_function}")
            
            # Construir SQL
            sql_parts = [f"SELECT {', '.join(select_parts)}", "FROM datos"]
            
            # Agregar WHERE con TODOS los filtros
            where_conditions = []
            
            # 1. Filtros de columna
            for condition in structure.column_conditions:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
                print(f"   ✅ Filter: {condition.column_name} = '{condition.value}'")
            
            # 2. 🔧 CORRECCIÓN: AGREGAR FILTROS TEMPORALES
            temporal_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
            if temporal_conditions:
                where_conditions.extend(temporal_conditions)
                print(f"   📅 Temporal filters added: {temporal_conditions}")
            
            # 3. 🆕 OPCIONAL: Agregar filtros de exclusión si existen
            if hasattr(structure, 'exclusion_filters'):
                for exclusion in structure.exclusion_filters:
                    if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                        where_conditions.append(f"{exclusion.column_name} != '{exclusion.value}'")
                        print(f"   🚫 Exclusion filter: {exclusion.column_name} != '{exclusion.value}'")
            
            if where_conditions:
                sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
            
            final_sql = " ".join(sql_parts) + ";"
            print(f"   🎯 Multi-metric SQL: {final_sql}")
            return final_sql
        
        # Verificar si es consulta superlativa PRIMERO
        if hasattr(structure, 'superlative_pattern') and structure.superlative_pattern:
            print(f"🏆 DETECTED: Superlative pattern → using superlative generator")
            return self.generate_superlative_sql_english(structure.superlative_pattern, structure)
        
        # Verificar si es consulta COUNT
        is_count_query = getattr(structure, 'is_count_query', False)
        if is_count_query:
            print(f"🔢 COUNT query detected - using COUNT SQL generator")
            return self._generate_count_sql_simple(structure)
        
        # ✅ CASOS ESPECIALES CON LIST ALL MEJORADO
        if (hasattr(structure, 'list_all_pattern') and structure.list_all_pattern):
            print(f"📋 DETECTED: Enhanced English list all → using ENHANCED specialized generator")
            return self.generate_enhanced_list_all_sql_english(structure.list_all_pattern, structure)
        
        if (hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern):
            print(f"📊 DETECTED: English show rows → using specialized generator")
            return self.generate_show_rows_sql_english(structure.show_rows_pattern) 
        
        if (hasattr(structure, 'temporal_conditional_pattern') and structure.temporal_conditional_pattern):
            print(f"🕒 DETECTED: English temporal conditional → using specialized generator")
            return self.generate_temporal_conditional_sql_english(structure.temporal_conditional_pattern)
        
        # ✅ RESTO DEL CÓDIGO ORIGINAL
        select_parts = []
        from_clause = "FROM datos"
        where_conditions = []
        group_by_parts = []
        order_by_parts = []
        
        # Identificar columnas temporales para evitar duplicación
        temporal_columns = set()
        
        for tf in structure.temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['week', 'weeks', 'semana', 'semanas'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['month', 'months', 'mes', 'meses'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['day', 'days', 'dia', 'dias'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['year', 'years', 'año', 'años'])
        
        print(f"🗄️ Generating OPTIMIZED English SQL:")
        print(f"   ⏰ Temporal columns detected: {temporal_columns}")
        print(f"   🎯 Query pattern: {structure.query_pattern.value}")
        print(f"   🔗 Is compound: {structure.is_compound_query}")
        print(f"   🏆 Is ranking: {structure.is_ranking_query}")
        print(f"   🔗 Is multi-dimensional: {structure.is_multi_dimension_query}")
        
        # 🔧 MANEJAR RANKINGS MULTI-DIMENSIONALES
        if (structure.is_ranking_query and 
            structure.is_multi_dimension_query and 
            len(structure.main_dimensions) >= 2):
            print(f"🏆🔗 DETECTED: English ranking multi-dimensional → using specialized generator")
            return self.generate_multi_dimension_english_sql(structure, temporal_columns)
        
        # MANEJAR CONSULTAS MULTI-DIMENSIONALES SIN RANKING
        if (structure.is_multi_dimension_query and 
            structure.query_pattern == QueryPattern.MULTI_DIMENSION):
            print(f"🔗 DETECTED: English multi-dimensional without ranking → using specialized generator")
            return self.generate_multi_dimension_english_sql(structure, temporal_columns)
        
        # MANEJAR RANKINGS SIMPLES
        if (structure.is_ranking_query and 
            structure.ranking_criteria and 
            not structure.is_multi_dimension_query):
            print(f"🏆 DETECTED: English simple ranking → using ranking generator")
            return self.generate_ranking_sql_english(structure, temporal_columns)
        
        # VERIFICAR SI ES AGREGACIÓN GLOBAL
        is_global_aggregation = not structure.main_dimension and structure.operations and structure.metrics
        
        if is_global_aggregation:
            print(f"🌐 Generating English SQL for global aggregation")
            
            # 🆕 VERIFICAR SI HAY MÚLTIPLES MÉTRICAS EN AGREGACIÓN GLOBAL
            if structure.is_compound_query and len(structure.compound_criteria) > 1:
                print(f"📊 Detected MULTIPLE metrics in global compound aggregation")
                
                # Procesar cada criterio compuesto
                for i, criteria in enumerate(structure.compound_criteria):
                    operation_value = criteria.operation.value
                    metric_text = criteria.metric.text
                    
                    if operation_value == 'máximo':
                        agg_function = self._get_contextual_aggregation_english(structure, metric_text, operation_value)
                    else:
                        sql_operations = {
                            'mínimo': f'MIN({metric_text})',
                            'suma': f'SUM({metric_text})',
                            'promedio': f'AVG({metric_text})',
                            'conteo': f'COUNT({metric_text})',
                            'total': f'SUM({metric_text})'  # 🆕 Mapeo para 'total'
                        }
                        agg_function = sql_operations.get(operation_value, f'SUM({metric_text})')
                    
                    select_parts.append(agg_function)
                    print(f"   ✅ Global metric {i+1}: {agg_function}")
            
            # CASO NORMAL: Una sola métrica
            elif structure.operations and structure.metrics:
                operation = structure.operations[0]
                metric = structure.metrics[0]

                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                                
        else:
            # 🆕 LÓGICA MEJORADA PARA CONSULTAS CON DIMENSIÓN PRINCIPAL
            if structure.main_dimension:
                dim_name = structure.main_dimension.text
                formatted_dim = self.format_temporal_dimension(dim_name)
                select_parts.append(formatted_dim)
                group_by_parts.append(dim_name)
                
                # 🆕 CRÍTICO: Si hay métricas con GROUP BY, agregarlas al SELECT
                if structure.metrics:
                    print(f"🔧 GROUP BY dimension with metrics detected - adding aggregations")
                    
                    for metric in structure.metrics:
                        # Determinar función de agregación basada en operaciones
                        if structure.operations:
                            operation = structure.operations[0]
                            operation_text = operation.text.lower()
                            operation_value = getattr(operation, 'value', operation_text)
                            
                            print(f"   📊 Processing metric '{metric.text}' with operation '{operation_text}' (value: {operation_value})")
                            
                            if operation_value in ['suma', 'total'] or operation_text in ['total', 'sum']:
                                agg_function = f'SUM({metric.text})'
                            elif operation_value == 'promedio' or operation_text in ['average', 'avg']:
                                agg_function = f'AVG({metric.text})'
                            elif operation_value == 'máximo' or operation_text in ['max', 'maximum']:
                                agg_function = f'MAX({metric.text})'
                            elif operation_value == 'mínimo' or operation_text in ['min', 'minimum']:
                                agg_function = f'MIN({metric.text})'
                            else:
                                agg_function = f'SUM({metric.text})'  # Default para 'total'
                        else:
                            # Sin operación explícita, usar SUM por defecto
                            agg_function = f'SUM({metric.text})'
                            print(f"   📊 No operation found, using default SUM for metric '{metric.text}'")
                        
                        select_parts.append(agg_function)
                        
                        # Agregar ORDER BY para ordenar por la métrica
                        order_by_parts.append(f"{agg_function} DESC")
                        
                        print(f"   ✅ Added to GROUP BY query: {agg_function}")
            
            # CONSULTAS COMPUESTAS CON DIMENSIÓN
            if structure.is_compound_query and structure.compound_criteria:
                print(f"🔗 Processing English compound query with {len(structure.compound_criteria)} criteria:")
                
                for i, criteria in enumerate(structure.compound_criteria):
                    operation_value = criteria.operation.value
                    metric_text = criteria.metric.text
                    
                    if operation_value == 'máximo':
                        agg_function = self._get_contextual_aggregation_english(structure, metric_text, operation_value)
                    else:
                        sql_operations = {
                            'mínimo': f'MIN({metric_text})',
                            'suma': f'SUM({metric_text})',
                            'promedio': f'AVG({metric_text})',
                            'conteo': f'COUNT({metric_text})',
                            'total': f'SUM({metric_text})'  # 🆕 Agregar mapeo para 'total'
                        }
                        agg_function = sql_operations.get(operation_value, f'SUM({metric_text})')
                    
                    if agg_function:
                        select_parts.append(agg_function)
                        
                        if operation_value in ['máximo', 'mayor']:
                            order_direction = "DESC"
                        elif operation_value in ['mínimo', 'menor']:
                            order_direction = "ASC"
                        else:
                            order_direction = "DESC"
                        
                        order_by_parts.append(f"{agg_function} {order_direction}")
                        
                        print(f"   🔗 English Criteria {i+1}: {operation_value} {metric_text} → {agg_function} {order_direction}")
                    else:
                        select_parts.append(metric_text)
                        order_by_parts.append(f"{metric_text} DESC")
                        print(f"   🔗 English Criteria {i+1}: {metric_text} → {metric_text} DESC")
            
            # LÓGICA TRADICIONAL (una métrica) - SOLO SI NO HAY MAIN_DIMENSION CON MÉTRICAS
            elif structure.operations and structure.metrics and not (structure.main_dimension and structure.metrics):
                operation = structure.operations[0]
                metric = structure.metrics[0]
                
                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation_english(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                    
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        if operation.value in ['máximo', 'mayor']:
                            order_by_parts.append(f"{agg_function} DESC")
                        elif operation.value in ['mínimo', 'menor']:
                            order_by_parts.append(f"{agg_function} ASC")
                        else:
                            order_by_parts.append(f"{agg_function} DESC")
                    else:
                        order_by_parts.append(f"{agg_function} DESC")
                else:
                    select_parts.append(metric.text)
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        order_by_parts.append(f"{metric.text} DESC")
        
        # WHERE para condiciones de columna (excluyendo temporales duplicadas)
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
                print(f"   ✅ English WHERE condition: {condition.column_name} = '{condition.value}'")
            else:
                print(f"   ⏰ English excluding duplicate temporal condition: {condition.column_name} = '{condition.value}'")
        
        # FILTROS DE EXCLUSIÓN
        if hasattr(structure, 'exclusion_filters'):
            for exclusion in structure.exclusion_filters:
                if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                    where_conditions.append(f"{exclusion.column_name} != '{exclusion.value}'")
                    print(f"   🚫 English exclusion condition: {exclusion.column_name} != '{exclusion.value}'")
        
        # FILTROS TEMPORALES
        advanced_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
        if advanced_conditions:
            where_conditions.extend(advanced_conditions)
            print(f"   ✅ English using temporal filters: {advanced_conditions}")
        
        # CONSTRUCCIÓN DEL SQL FINAL
        sql_parts = []
        
        if select_parts:
            sql_parts.append(f"SELECT {', '.join(select_parts)}")
        else:
            sql_parts.append("SELECT *")
        
        sql_parts.append(from_clause)
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # LIMITAR LA DATA SEGÚN EL PATRÓN
        if structure.query_pattern == QueryPattern.REFERENCED:
            sql_parts.append("LIMIT 1")
            print(f"   🎯 English adding LIMIT 1 for REFERENCED pattern")
            
        elif structure.query_pattern == QueryPattern.TOP_N and structure.limit_value:
            sql_parts.append(f"LIMIT {structure.limit_value}")
            print(f"   🏆 English adding LIMIT {structure.limit_value} for TOP_N pattern")
        
        elif structure.is_ranking_query and structure.ranking_criteria and structure.ranking_criteria.value:
            limit_value = int(structure.ranking_criteria.value)
            sql_parts.append(f"LIMIT {limit_value}")
            print(f"   🏆 English FORCING LIMIT {limit_value} for ranking (pattern: {structure.query_pattern.value})")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 Final OPTIMIZED English SQL: {final_sql}")
        
        return final_sql

        
    def build_unified_structure_english(self, classified_components: Dict, column_value_pairs: List[ColumnValuePair], 
                            temporal_filters: List[TemporalFilter], tokens: List[str], original_intent: str) -> QueryStructure:
        """🇺🇸 CONSTRUCTOR DE ESTRUCTURA UNIFICADA PARA INGLÉS"""
        
        print(f"🏗️ BUILDING UNIFIED ENGLISH QUERY STRUCTURE")
        
# PASO 1: Detectar patrones complejos PRIMERO
        ranking_criteria = self.detect_ranking_criteria_english(tokens, classified_components)
        exclusion_filters = self.detect_exclusion_filters_english(tokens, classified_components)
        is_ranking = self.is_ranking_query_english(ranking_criteria, exclusion_filters)
        
# PASO 2: Detectar múltiples dimensiones
        multi_dimensions = self.detect_multi_dimensions_english(tokens, classified_components)
        is_multi_dimension = len(multi_dimensions) >= 2
        
# PASO 3: Solo SI NO es ranking, procesar otros patrones
        if not is_ranking:
            compound_criteria = self.detect_compound_criteria_english(tokens, classified_components)
            is_compound = self.is_compound_query_english(compound_criteria)
        else:
            compound_criteria = []
            is_compound = False
        
# PASO 4: Construir componentes básicos
        main_dimension = None
        operations = []
        metrics = []
        values = []
        connectors = []
        unknown_tokens = []
        
        for token, component in classified_components.items():
            if component.type == ComponentType.DIMENSION and not main_dimension:
                main_dimension = component
                print(f"   📍 English main dimension: {component.text}")
            elif component.type == ComponentType.OPERATION:
                operations.append(component)
                print(f"   ⚡ English operation: {component.text}")
            elif component.type == ComponentType.METRIC:
                metrics.append(component)
                print(f"   📊 English metric: {component.text}")
            elif component.type == ComponentType.VALUE:
                values.append(component)
            elif component.type == ComponentType.CONNECTOR:
                connectors.append(component)
            elif component.type == ComponentType.UNKNOWN:
                unknown_tokens.append(component)
        
# PASO 5: Construir estructura completa
        structure = QueryStructure(
            main_dimension=main_dimension,
            main_dimensions=multi_dimensions if is_multi_dimension else ([main_dimension] if main_dimension else []),
            is_multi_dimension_query=is_multi_dimension,
            operations=operations,
            metrics=metrics,
            column_conditions=column_value_pairs,
            temporal_filters=temporal_filters,
            values=values,
            connectors=connectors,
            unknown_tokens=unknown_tokens,
            compound_criteria=compound_criteria,
            is_compound_query=is_compound,
            ranking_criteria=ranking_criteria,
            exclusion_filters=exclusion_filters,
            is_ranking_query=is_ranking,
            original_semantic_intent=original_intent
        )
        
# PASO 6: Detectar patrón de consulta
        query_pattern = self.detect_query_pattern_english(structure)
        structure.query_pattern = query_pattern
        
# PASO 7: Configurar límites según el tipo de consulta
        if query_pattern == QueryPattern.TOP_N and structure.ranking_criteria:
            if structure.ranking_criteria.unit == RankingUnit.COUNT:
                structure.limit_value = int(structure.ranking_criteria.value)
            elif structure.ranking_criteria.unit == RankingUnit.PERCENTAGE:
                structure.limit_value = None
            structure.is_single_result = False
            
            print(f"🏆 ENGLISH RANKING CONFIGURATION:")
            print(f"   📍 Target dimension: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
            print(f"   📊 Ranking metric: {structure.ranking_criteria.metric.text if structure.ranking_criteria.metric else 'N/A'}")
            print(f"   🎯 Direction: {structure.ranking_criteria.direction.value}")
            print(f"   📈 Unit: {structure.ranking_criteria.unit.value}")
            print(f"   🔢 Value: {structure.ranking_criteria.value}")
            
        elif query_pattern == QueryPattern.REFERENCED:
            structure.reference_metric = metrics[0] if metrics else None
            structure.is_single_result = True
            structure.limit_value = 1
            
            print(f"🎯 ENGLISH REFERENCED CONFIGURATION:")
            print(f"   📍 Target dimension: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
            print(f"   📊 Reference metric: {structure.reference_metric.text if structure.reference_metric else 'N/A'}")
        
        print(f"🏗️ English structure built with {len(operations)} operations, {len(metrics)} metrics")
        print(f"   🎯 Query pattern: {query_pattern.value}")
        print(f"   🏆 Is ranking: {is_ranking}")
        print(f"   🔗 Is compound: {is_compound}")
        print(f"   🔗 Is multi-dimensional: {is_multi_dimension}")
        
        return structure
        
    

    def structure_to_dict_english(self, structure: QueryStructure) -> Dict:
        """Convertidor de Estructura a Diccionario - CON SOPORTE SHOW_ROWS"""
        
        # Convertir main_dimension de forma segura
        main_dimension_dict = None
        if structure.main_dimension:
            main_dimension_dict = self.component_to_dict(structure.main_dimension)
        
        result = {
            'main_dimension': main_dimension_dict,
            'operations': [self.component_to_dict(op) for op in structure.operations],
            'metrics': [self.component_to_dict(m) for m in structure.metrics],
            'column_conditions': [self.cvp_to_dict(cvp) for cvp in structure.column_conditions],
            'temporal_filters': [self.temporal_to_dict(tf) for tf in structure.temporal_filters],
            'values': [self.component_to_dict(v) for v in structure.values],
            'connectors': [self.component_to_dict(c) for c in structure.connectors],
            'unknown_tokens': [self.component_to_dict(u) for u in structure.unknown_tokens],
            'complexity_level': structure.get_complexity_level()
        }
        
        # Agregar patrones especiales si existen
        if hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern:
            result['show_rows_pattern'] = structure.show_rows_pattern
        
        if hasattr(structure, 'list_all_pattern') and structure.list_all_pattern:
            result['list_all_pattern'] = structure.list_all_pattern
            
        return result
                
        
    def generate_hierarchical_structure_english(self, structure: QueryStructure) -> str:
        """🔧 Generador de Estructura Jerárquica - VERSIÓN CON SOPORTE SHOW_ROWS"""
        
        # 🆕 CASO ESPECIAL: SHOW_ROWS
        if hasattr(structure, 'show_rows_pattern') and structure.show_rows_pattern:
            pattern = structure.show_rows_pattern
            position = pattern.get('position_type', '')
            count = pattern.get('row_count', 0)
            object_type = pattern.get('object_type', 'rows')
            
            if position:
                return f"show {position} {count} {object_type}"
            else:
                return f"show {count} {object_type}"
        
        # CASO ESPECIAL: Rankings - VERSIÓN MULTI-CRITERIO
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking = structure.ranking_criteria
            direction_text = "top" if ranking.direction == RankingDirection.TOP else "worst"
            
            # Verificar si hay dimensión principal
            main_dim_text = structure.main_dimension.text if structure.main_dimension else "records"
            
            if ranking.unit == RankingUnit.COUNT:
                result = f"{direction_text} {int(ranking.value)} ({main_dim_text})"
            else:  # PERCENTAGE
                result = f"{direction_text} {ranking.value}% ({main_dim_text})"
            
            # 🔧 NUEVA LÓGICA: Incluir múltiples criterios
            if len(structure.metrics) > 1:
                operations_available = [op.text.lower() for op in structure.operations if op.text.lower() in ['mas', 'más', 'mayor', 'menor']]
                metrics_available = [m.text for m in structure.metrics]
                
                criteria_parts = []
                for i, metric in enumerate(metrics_available):
                    if i < len(operations_available):
                        op = operations_available[i]
                    else:
                        op = operations_available[0] if operations_available else 'mas'
                    
                    criteria_parts.append(f"({op} {metric})")
                
                # Combinar con " y "
                combined_criteria = " y ".join(criteria_parts)
                result += f" por {combined_criteria}"
                
            else:
                # LÓGICA ORIGINAL: Un criterio
                if ranking.metric:
                    result += f" por ({ranking.metric.text})"
            
            # NUEVA LÓGICA: Agregar filtros temporales avanzados
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                result += f" {temporal_description}"
            
            # NUEVA LÓGICA: Agregar filtros de columna si existen
            if structure.column_conditions:
                filter_parts = []
                for condition in structure.column_conditions:
                    filter_parts.append(f"con {condition.column_name} = '{condition.value}'")
                
                if filter_parts:
                    result += f" {' y '.join(filter_parts)}"
            
            # NUEVA LÓGICA: Agregar exclusiones si existen
            if structure.exclusion_filters:
                exclusion_parts = []
                for exclusion in structure.exclusion_filters:
                    exclusion_parts.append(f"excluyendo {exclusion.column_name} = '{exclusion.value}'")
                
                if exclusion_parts:
                    result += f" {' y '.join(exclusion_parts)}"
            
            print(f"   🏆 Resultado ranking completo: {result}")
            return result
        
        # RESTO DE LA LÓGICA ORIGINAL PARA CONSULTAS NO-RANKING
        parts = []
        
        # PASO 1: Identificar columnas temporales
        temporal_columns = set()
        for tf in structure.temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.add('semana')
                temporal_columns.add('week')
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.add('mes')
                temporal_columns.add('month')
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.add('dia')
                temporal_columns.add('day')
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.add('año')
                temporal_columns.add('year')
        
        print(f"🔍 Generando estructura jerárquica para consulta compuesta:")
        print(f"   📍 Dimensión: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
        print(f"   🔗 Es compuesta: {structure.is_compound_query}")
        print(f"   🔗 Criterios compuestos: {len(structure.compound_criteria)}")
        print(f"   ⏰ Columnas temporales: {temporal_columns}")
        
        # PASO 2: Verificar si dimensión está en filtros
        dimension_in_filter = False
        if structure.main_dimension and structure.column_conditions:
            main_dim_name = structure.main_dimension.text
            for condition in structure.column_conditions:
                if condition.column_name == main_dim_name:
                    dimension_in_filter = True
                    break
        
        print(f"   🔄 ¿Dimensión en filtros? {dimension_in_filter}")
        
        # PASO 3: FILTRAR condiciones temporales duplicadas
        non_temporal_conditions = []
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                non_temporal_conditions.append(condition)
                print(f"   ✅ Conservando filtro: {condition.column_name} = {condition.value}")
            else:
                print(f"   ⏰ EXCLUYENDO filtro temporal duplicado: {condition.column_name} = {condition.value}")
        
        # PASO 4: Construir dimensión principal
        if structure.main_dimension and not dimension_in_filter:
            main_part = f"({structure.main_dimension.text})"
            
            # CRÍTICO: Solo agregar filtros NO temporales
            if non_temporal_conditions:
                conditions = []
                for condition in non_temporal_conditions:
                    conditions.append(f"({condition.column_name} = '{condition.value}')")
                main_part += f" con {' y '.join(conditions)}"
            
            parts.append(main_part)
            print(f"   ✅ Parte principal: {main_part}")
        
        # PASO 5: Filtros directos (solo NO temporales)
        elif non_temporal_conditions:
            filter_parts = []
            for condition in non_temporal_conditions:
                filter_parts.append(f"({condition.column_name} = '{condition.value}')")
            
            if len(filter_parts) == 1:
                parts.append(filter_parts[0])
            else:
                parts.append(f"({' Y '.join(filter_parts)})")
            
            print(f"   ✅ Filtros directos (no temporales): {filter_parts}")
        
        # PASO 6 NUEVA LÓGICA: Operación y métrica COMPUESTA
        if structure.is_compound_query and structure.compound_criteria:
            print(f"🔗 PROCESANDO ESTRUCTURA JERÁRQUICA COMPUESTA:")
            
            # Construir cada criterio como ((operación) (métrica))
            criteria_parts = []
            for i, criteria in enumerate(structure.compound_criteria):
                criteria_part = f"(({criteria.operation.text}) ({criteria.metric.text}))"
                criteria_parts.append(criteria_part)
                print(f"   {i+1}. Criterio: {criteria_part}")
            
            # Unir criterios con " y "
            if len(criteria_parts) == 1:
                operation_part = criteria_parts[0]
            else:
                operation_part = " y ".join(criteria_parts)
            
            # NUEVA LÓGICA: Agregar información temporal avanzada para compuestas
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                operation_part += f" {temporal_description}"
            
            parts.append(operation_part)
            print(f"   ✅ Operación compuesta: {operation_part}")
        
        # PASO 6 LÓGICA TRADICIONAL: Para consultas NO compuestas
        elif structure.operations and structure.metrics:
            op = structure.operations[0]
            metric = structure.metrics[0]
            operation_part = f"(({op.text}) ({metric.text}))"
            
            # NUEVA LÓGICA: Agregar información temporal avanzada
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                operation_part += f" {temporal_description}"
            
            parts.append(operation_part)
            print(f"   ✅ Operación+Métrica tradicional: {operation_part}")
        
        elif structure.operations:
            op = structure.operations[0]
            parts.append(f"({op.text})")
            print(f"   ✅ Solo operación: ({op.text})")
            
        elif structure.metrics:
            # 🔧 Solo agregar métricas que NO están en filtros
            metrics_not_in_filters = []
            for metric in structure.metrics:
                used_in_filter = any(
                    cvp.column_name == metric.text 
                    for cvp in structure.column_conditions
                )
                if not used_in_filter:
                    metrics_not_in_filters.append(metric)
            
            if metrics_not_in_filters:
                metric = metrics_not_in_filters[0]
                parts.append(f"({metric.text})")
        
        # PASO 7: Combinar partes con lógica correcta
        if len(parts) == 1:
            result = parts[0]
        elif len(parts) == 2:
            # Verificar si TODAS las condiciones son temporales
            all_conditions_are_temporal = all(
                condition.column_name in temporal_columns 
                for condition in structure.column_conditions
            )
            
            if all_conditions_are_temporal and structure.main_dimension:
                # Caso: dimensión + operación temporal (sin filtros adicionales)
                result = f"{parts[0]} con {parts[1]}"
                print(f"   🔧 Combinación especial (dimensión con operación temporal): {result}")
            else:
                # Caso: múltiples condiciones independientes
                result = f"{' Y '.join(parts)}"
                print(f"   🔧 Combinación estándar (múltiples condiciones): {result}")
        elif len(parts) > 2:
            result = f"{' Y '.join(parts)}"
        else:
            result = "estructura_incompleta"
        
        print(f"   🎯 Resultado final COMPUESTO: {result}")
        return result
        
    

    def calculate_overall_confidence_english(self, structure: QueryStructure) -> float:
        """Calculador de Confianza General"""
        all_components = []
        
        if structure.main_dimension:
            all_components.append(structure.main_dimension)
        
        all_components.extend(structure.operations)
        all_components.extend(structure.metrics)
        all_components.extend(structure.values)
        all_components.extend(structure.connectors)
        all_components.extend(structure.unknown_tokens)
        
        # Agregar confianza de condiciones de columna
        for condition in structure.column_conditions:
            all_components.append(QueryComponent("dummy", ComponentType.COLUMN_VALUE, condition.confidence))
        
        # Agregar confianza de filtros temporales
        for tf in structure.temporal_filters:
            all_components.append(QueryComponent("dummy", ComponentType.TEMPORAL, tf.confidence))
        
        if not all_components:
            return 0.0
        
        # Calcular promedio ponderado
        total_confidence = sum(comp.confidence for comp in all_components)
        return round(total_confidence / len(all_components), 2)



# ------  "Convertidor de componente a diccionario" -------

    def component_to_dict(self, component: QueryComponent) -> Dict:
        """Convertidor de Componente a Diccionario"""
        if not component:
            return None
        
        return {
            'text': component.text,
            'type': component.type.value,
            'confidence': component.confidence,
            'subtype': component.subtype,
            'value': component.value,
            'column_name': component.column_name,
            'linguistic_info': component.linguistic_info
        }



    def generate_hierarchical_structure_temporal_description(self, structure: QueryStructure) -> str:
        """Genera descripción temporal avanzada para estructura jerárquica"""
        temporal_parts = []
        
        # NUEVA LÓGICA: Usar información temporal avanzada si está disponible
        if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
            for advanced_info in self.advanced_temporal_info:
                # 🔧 FIX: Validar que los valores existen antes de usarlos
                try:
                    if hasattr(advanced_info, 'is_range_between') and advanced_info.is_range_between:
                        start_value = getattr(advanced_info, 'start_value', None)
                        end_value = getattr(advanced_info, 'end_value', None)
                        
                        if start_value is not None and end_value is not None:
                            if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                                temporal_parts.append(f"de semana {start_value} a {end_value}")
                            elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                                temporal_parts.append(f"de mes {start_value} a {end_value}")
                            elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                                temporal_parts.append(f"de día {start_value} a {end_value}")
                except Exception as e:
                    print(f"⚠️ Error processing advanced_temporal_info: {e}")
                    continue
        
        # NUEVA LÓGICA: Usar información temporal avanzada si está disponible
        if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
            for advanced_info in self.advanced_temporal_info:
                if advanced_info.is_range_from:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"desde semana {advanced_info.start_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"desde mes {advanced_info.start_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"desde día {advanced_info.start_value}")
                elif advanced_info.is_range_between:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"de semana {advanced_info.start_value} a {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"de mes {advanced_info.start_value} a {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"de día {advanced_info.start_value} a {advanced_info.end_value}")
                elif advanced_info.is_range_to:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"hasta semana {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"hasta mes {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"hasta día {advanced_info.end_value}")
                else:
                    # Filtros tradicionales existentes
                    tf = advanced_info.original_filter
                    if tf.filter_type == "specific":
                        if tf.unit == TemporalUnit.WEEKS:
                            temporal_parts.append(f"en semana {tf.quantity}")
                        elif tf.unit == TemporalUnit.MONTHS:
                            temporal_parts.append(f"en mes {tf.quantity}")
                        elif tf.unit == TemporalUnit.DAYS:
                            temporal_parts.append(f"en día {tf.quantity}")
                    else:
                        temporal_parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
        else:
            # FALLBACK: Usar filtros temporales tradicionales (para compatibilidad)
            for tf in structure.temporal_filters:
                if tf.filter_type == "specific":
                    if tf.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"en semana {tf.quantity}")
                    elif tf.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"en mes {tf.quantity}")
                    elif tf.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"en día {tf.quantity}")
                else:
                    temporal_parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
        
        return ' y '.join(temporal_parts) if temporal_parts else ""


    def cvp_to_dict(self, cvp: ColumnValuePair) -> Dict:
        """Convertidor de Par Columna-Valor"""
        return {
            'column_name': cvp.column_name,
            'value': cvp.value,
            'confidence': cvp.confidence,
            'raw_text': cvp.raw_text
        }



# EN temporal_to_dict (alrededor de línea 3290)
    def temporal_to_dict(self, tf: TemporalFilter) -> Dict:
        """Convertidor de Filtro Temporal"""
        return {
            'indicator': tf.indicator,
            'quantity': tf.quantity,
            'unit': tf.unit.value,
            'confidence': tf.confidence,
            'filter_type': tf.filter_type,
            # AGREGAR ESTOS CAMPOS:
            'start_value': getattr(tf, 'start_value', None),
            'end_value': getattr(tf, 'end_value', None)
        }


# ------  "Inferidor de dimension por defecto" -------

    def infer_default_dimension_for_ranking(self, ranking_criteria: RankingCriteria) -> Optional[QueryComponent]:
        """Inferidor de Dimensión por Defecto"""
        # Dimensiones comunes por métrica
        metric_to_dimension = {
            'ventas': 'account',
            'venta': 'account', 
            'inventario': 'product',
            'margen': 'product',
            'revenue': 'account',
            'sales': 'account'
        }
        
        if ranking_criteria and ranking_criteria.metric:
            metric_text = ranking_criteria.metric.text.lower()
            if metric_text in metric_to_dimension:
                inferred_dim = metric_to_dimension[metric_text]
                
                return QueryComponent(
                    text=inferred_dim,
                    type=ComponentType.DIMENSION,
                    confidence=0.75,  # Confianza media por ser inferida
                    subtype='inferred',
                    linguistic_info={'source': 'inferred_for_ranking'}
                )
        
        return None



    def _is_potential_metric_english(self, token: str) -> bool:
        """Detecta si un token es potencialmente una métrica"""
        token_lower = token.lower()
        
        # Métricas en diccionario
        if token_lower in self.dictionaries.metricas:
            return True
        
        # Métricas comunes en inglés
        common_metrics = {
            'sales', 'revenue', 'profit', 'margin', 'cost', 'price', 
            'inventory', 'stock', 'amount', 'value', 'total', 'count',
            'volume', 'quantity', 'units', 'dollars', 'euros'
        }
        
        if token_lower in common_metrics:
            return True
        
        # Plurales de métricas
        if token_lower.endswith('s') and token_lower[:-1] in common_metrics:
            return True
        
        return False


    def detect_list_all_pattern_english(self, tokens: List[str]) -> Optional[Dict]:
        """📋 DETECTOR DE PATRÓN LIST ALL EN INGLÉS - MEJORADO PARA TEMPORALES"""
        print(f"📋 DETECTING LIST ALL PATTERN:")
        print(f"   🔤 Tokens: {tokens}")
        
        # 🆕 CASO ESPECIAL: Dimensión temporal sola o casi sola
        temporal_plurals = {'weeks', 'months', 'days', 'years', 'quarters'}
        
        # Si el primer token es una dimensión temporal plural
        if len(tokens) > 0 and tokens[0].lower() in temporal_plurals:
            # Verificar si es una consulta simple (solo la dimensión o con filtros simples)
            is_simple_temporal_query = False
            
            # Caso 1: Solo la dimensión temporal ("weeks")
            if len(tokens) == 1:
                is_simple_temporal_query = True
            
            # Caso 2: Dimensión temporal con filtros simples ("weeks of liverpool")
            elif len(tokens) <= 4 and 'where' not in [t.lower() for t in tokens]:
                # No debe tener verbos de agregación ni "where"
                aggregation_verbs = {'sum', 'total', 'average', 'count', 'max', 'min'}
                has_aggregation = any(t.lower() in aggregation_verbs for t in tokens)
                
                if not has_aggregation:
                    is_simple_temporal_query = True
            
            if is_simple_temporal_query:
                # Normalizar plural a singular para la dimensión objetivo
                temporal_singular_map = {
                    'weeks': 'week',
                    'months': 'month',
                    'days': 'day',
                    'years': 'year',
                    'quarters': 'quarter'
                }
                
                target_dimension = temporal_singular_map.get(tokens[0].lower(), tokens[0].lower())
                
                pattern_result = {
                    'pattern_type': 'LIST_ALL',
                    'list_indicator': 'implicit',  # No hay indicador explícito
                    'has_all_indicator': True,
                    'all_indicator': 'all',
                    'target_dimension': target_dimension,
                    'has_aggregation': False,
                    'confidence': 0.95,
                    'raw_tokens': tokens,
                    'is_temporal_list': True  # 🆕 Marcador especial
                }
                
                print(f"📋 TEMPORAL LIST PATTERN DETECTED:")
                print(f"   📍 Target dimension: {target_dimension}")
                print(f"   ⏰ Is temporal list: True")
                print(f"   ⭐ Confidence: {pattern_result['confidence']:.2f}")
                
                return pattern_result
        
        # CONTINUAR CON LA DETECCIÓN NORMAL...
        if len(tokens) < 2:  # Mínimo: list items
            return None
        
        # STEP 1: Buscar indicadores de "list"
        list_indicators = {'list', 'show', 'display', 'get', 'find', 'give', 'tell'}
        list_start_pos = -1
        
        for i, token in enumerate(tokens):
            if token.lower() in list_indicators:
                list_start_pos = i
                print(f"   ✅ List indicator: '{token}' at position {i}")
                break
        
        if list_start_pos == -1:
            print(f"   ❌ No list indicator found")
            return None
        
        # STEP 2: Buscar "all" (opcional pero común)
        all_indicators = {'all', 'every', 'each'}
        has_all_indicator = False
        all_pos = -1
        
        for i in range(list_start_pos + 1, min(list_start_pos + 3, len(tokens))):
            if i < len(tokens) and tokens[i].lower() in all_indicators:
                has_all_indicator = True
                all_pos = i
                print(f"   ✅ All indicator: '{tokens[i]}' at position {i}")
                break
        
        # STEP 3: Buscar dimensión objetivo
        target_dimension = None
        dimension_pos = -1
        search_start = all_pos + 1 if has_all_indicator else list_start_pos + 1
        
        for i in range(search_start, len(tokens)):
            if i >= len(tokens):
                break
            
            token = tokens[i]
            if self._is_potential_dimension_english(token):
                target_dimension = token.lower()
                dimension_pos = i
                print(f"   ✅ Target dimension: '{target_dimension}'")
                break
        
        if not target_dimension:
            print(f"   ❌ No target dimension found")
            return None
        
        # STEP 4: DETECTAR SI HAY MÉTRICAS/AGREGACIONES DESPUÉS
        has_aggregation = False
        aggregation_keywords = {'and', 'with', 'their', 'including'}
        metric_keywords = {
            'total', 'sum', 'average', 'count', 'sales', 'revenue', 
            'inventory', 'profit', 'cost', 'amount', 'quantity'
        }
        
        # Buscar indicadores de agregación después de la dimensión
        for i in range(dimension_pos + 1, len(tokens)):
            if tokens[i].lower() in aggregation_keywords:
                # Verificar si hay métricas después
                for j in range(i + 1, len(tokens)):
                    if tokens[j].lower() in metric_keywords:
                        has_aggregation = True
                        print(f"   ✅ Aggregation detected: '{tokens[i]}' ... '{tokens[j]}'")
                        break
                if has_aggregation:
                    break
        
        # STEP 5: Calcular confianza
        confidence = 0.7  # Base
        confidence += 0.2  # Has list indicator
        if has_all_indicator:
            confidence += 0.1  # Has "all"
        
        # STEP 6: Construir resultado
        pattern_result = {
            'pattern_type': 'LIST_ALL',
            'list_indicator': tokens[list_start_pos].lower(),
            'has_all_indicator': has_all_indicator,
            'all_indicator': tokens[all_pos].lower() if has_all_indicator else None,
            'target_dimension': target_dimension,
            'has_aggregation': has_aggregation,
            'confidence': min(1.0, confidence),
            'raw_tokens': tokens,
            'is_temporal_list': False  # Normal list, no temporal
        }
        
        print(f"📋 LIST ALL PATTERN DETECTED:")
        print(f"   📋 List indicator: {pattern_result['list_indicator']}")
        print(f"   🌐 Has 'all': {pattern_result['has_all_indicator']}")
        print(f"   📍 Target dimension: {pattern_result['target_dimension']}")
        print(f"   📊 Has aggregation: {pattern_result['has_aggregation']}")
        print(f"   ⭐ Confidence: {pattern_result['confidence']:.2f}")
        
        return pattern_result


    def _is_potential_dimension_english(self, token: str) -> bool:
        """📍 VERIFICADOR DE DIMENSIONES PARA LIST ALL"""
        token_lower = token.lower()
        
        # PRIORIDAD 1: Dimensiones en diccionario
        if token_lower in self.dictionaries.dimensiones:
            return True
        
        # PRIORIDAD 2: Dimensiones comunes en inglés (plurales)
        common_dimensions = {
            'items', 'products', 'customers', 'stores', 'accounts', 'partners',
            'orders', 'users', 'clients', 'vendors', 'suppliers', 'categories',
            'regions', 'countries', 'cities', 'brands', 'models', 'types'
        }
        
        if token_lower in common_dimensions:
            return True
        
        # PRIORIDAD 3: Versiones singulares
        singular_dimensions = {
            'item', 'product', 'customer', 'store', 'account', 'partner',
            'order', 'user', 'client', 'vendor', 'supplier',
            'region', 'country', 'city', 'brand', 'model', 'type'
        }
        
        if token_lower in singular_dimensions:
            return True
        
        # PRIORIDAD 4: Snake_case dimensions
        if '_' in token_lower and len(token_lower) > 3:
            return True
        
        return False


    def generate_list_all_sql_english(self, pattern_data: Dict) -> str:
        """📋 GENERADOR SQL PARA PATRÓN LIST ALL"""
        print(f"📋 GENERATING LIST ALL SQL:")
        
        target_dimension = pattern_data['target_dimension']
        list_indicator = pattern_data['list_indicator']
        
        print(f"   📋 List type: {list_indicator}")
        print(f"   📍 Target dimension: {target_dimension}")
        
        # Construir SQL simple
        sql_parts = [
            f"SELECT DISTINCT {target_dimension}",
            "FROM datos",
            f"ORDER BY {target_dimension}"
        ]
        
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 List all SQL: {final_sql}")
        return final_sql


    def detect_show_rows_pattern_english(self, tokens: List[str]) -> Optional[Dict]:
        """📊 DETECTOR DE PATRÓN SHOW ROWS EN INGLÉS"""
        print(f"📊 DETECTING SHOW ROWS PATTERN:")
        print(f"   🔤 Tokens: {tokens}")
        
        if len(tokens) < 2:  # Mínimo: show rows
            return None
        
# STEP 1: Buscar indicadores de "show/display"
        show_indicators = {'show', 'display', 'get', 'fetch', 'list', 'give', 'return'}
        show_start_pos = -1
        
        for i, token in enumerate(tokens):
            if token.lower() in show_indicators:
                show_start_pos = i
                print(f"   ✅ Show indicator: '{token}' at position {i}")
                break
        
        if show_start_pos == -1:
            print(f"   ❌ No show indicator found")
            return None
        
# STEP 2: Buscar indicadores de posición (opcional)
        position_indicators = {'first', 'last', 'top', 'bottom', 'initial', 'final'}
        position_type = None
        position_pos = -1
        
        # Buscar posición después del indicador de show
        for i in range(show_start_pos + 1, min(show_start_pos + 3, len(tokens))):
            if i < len(tokens) and tokens[i].lower() in position_indicators:
                position_type = tokens[i].lower()
                position_pos = i
                print(f"   ✅ Position indicator: '{tokens[i]}' at position {i}")
                break
        
# STEP 3: Buscar número de filas
        row_count = None
        number_pos = -1
        search_start = position_pos + 1 if position_pos != -1 else show_start_pos + 1
        
        for i in range(search_start, min(search_start + 3, len(tokens))):
            if i >= len(tokens):
                break
            
            token = tokens[i]
            
            # Número directo
            if token.isdigit():
                row_count = int(token)
                number_pos = i
                print(f"   ✅ Row count (number): {row_count}")
                break
            
            # Números en palabras en inglés
            elif token.lower() in self.dictionaries.numeros_palabras_en:
                row_count = self.dictionaries.numeros_palabras_en[token.lower()]
                number_pos = i
                print(f"   ✅ Row count (word): '{token}' = {row_count}")
                break
        
        if row_count is None:
            print(f"   ❌ No row count found")
            return None
        
# STEP 4: Buscar indicador de objeto (rows, records, entries)
        object_indicators = {'rows', 'row', 'records', 'record', 'entries', 'entry', 'lines', 'line', 'items', 'item'}
        object_type = None
        
        search_start = number_pos + 1
        for i in range(search_start, min(search_start + 2, len(tokens))):
            if i < len(tokens) and tokens[i].lower() in object_indicators:
                object_type = tokens[i].lower()
                print(f"   ✅ Object type: '{object_type}'")
                break
        
        # Si no encuentra objeto específico pero los otros componentes están, asumir "rows"
        if object_type is None:
            object_type = 'rows'
            print(f"   ✅ Object type (default): 'rows'")
        
# STEP 5: Calcular confianza
        confidence = 0.7  # Base
        confidence += 0.2  # Has show indicator
        if position_type:
            confidence += 0.1  # Has position
        if object_type in object_indicators:
            confidence += 0.1  # Has valid object type
        
# STEP 6: Construir resultado
        pattern_result = {
            'pattern_type': 'SHOW_ROWS',
            'show_indicator': tokens[show_start_pos].lower(),
            'position_type': position_type,
            'row_count': row_count,
            'object_type': object_type,
            'confidence': min(1.0, confidence),
            'raw_tokens': tokens
        }
        
        print(f"📊 SHOW ROWS PATTERN DETECTED:")
        print(f"   📊 Show indicator: {pattern_result['show_indicator']}")
        print(f"   📍 Position: {pattern_result['position_type']}")
        print(f"   🔢 Row count: {pattern_result['row_count']}")
        print(f"   📋 Object type: {pattern_result['object_type']}")
        print(f"   ⭐ Confidence: {pattern_result['confidence']:.2f}")
        
        return pattern_result


    def generate_show_rows_sql_english(self, pattern_data: Dict) -> str:
        """📊 GENERADOR SQL PARA PATRÓN SHOW ROWS"""
        print(f"📊 GENERATING SHOW ROWS SQL:")
        
        show_indicator = pattern_data['show_indicator']
        position_type = pattern_data.get('position_type')
        row_count = pattern_data['row_count']
        object_type = pattern_data['object_type']
        
        print(f"   📊 Show type: {show_indicator}")
        print(f"   📍 Position: {position_type}")
        print(f"   🔢 Count: {row_count}")
        print(f"   📋 Object: {object_type}")
        
        # Construir SQL base
        select_part = "SELECT *"
        from_part = "FROM datos"
        
        # Determinar ORDER BY según la posición
        if position_type in ['last', 'bottom', 'final']:
            # Para últimas filas, necesitamos ordenar descendente
            # Nota: esto depende de tener una columna de ID o timestamp
            # Por simplicidad, usamos ROWID (disponible en SQLite)
            order_part = "ORDER BY id DESC"
            print(f"   🔄 Using descending order for '{position_type}' rows")
        else:
            # Para primeras filas o sin posición específica
            order_part = "ORDER BY id ASC"
            print(f"   🔄 Using ascending order for '{position_type or 'default'}' rows")
        
        limit_part = f"LIMIT {row_count}"
        
        # Construir SQL final
        sql_parts = [select_part, from_part, order_part, limit_part]
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 Show rows SQL: {final_sql}")
        return final_sql


    def generate_multi_metric_sql_direct(self, pattern: MultiMetricPattern, normalized_query: str, original_query: str) -> Dict:
        """
        📊 GENERADOR DIRECTO DE SQL PARA MULTI-MÉTRICA
        Genera el resultado completo sin pasar por toda la pipeline
        """
        
        print(f"📊 GENERATING DIRECT MULTI-METRIC SQL:")
        
        # Construir SELECT
        select_parts = []
        
        # Si hay dimensión, agregarla primero
        if pattern.dimension:
            select_parts.append(pattern.dimension)
        
        # Agregar cada métrica con su operación
        for i, metric in enumerate(pattern.metrics):
            if i < len(pattern.operations):
                op = pattern.operations[i]
            else:
                op = pattern.operations[0] if pattern.operations else 'total'
            
            # Mapear operación a SQL
            if op in ['total', 'sum']:
                sql_func = f"SUM({metric})"
            elif op in ['average', 'avg']:
                sql_func = f"AVG({metric})"
            elif op == 'max':
                sql_func = f"MAX({metric})"
            elif op == 'min':
                sql_func = f"MIN({metric})"
            elif op == 'count':
                sql_func = f"COUNT({metric})"
            else:
                sql_func = f"SUM({metric})"
            
            alias = f"{op}_{metric}"
            select_parts.append(f"{sql_func} as {alias}")
            print(f"   ✅ Added: {sql_func} as {alias}")
        
        # Construir WHERE
        where_conditions = []
        for filter_item in pattern.filters:
            condition = f"{filter_item['column']} = '{filter_item['value']}'"
            where_conditions.append(condition)
            print(f"   🔍 Filter: {condition}")
        
        # Construir SQL
        sql_parts = [f"SELECT {', '.join(select_parts)}", "FROM datos"]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if pattern.dimension:
            sql_parts.append(f"GROUP BY {pattern.dimension}")
        
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 Final SQL: {final_sql}")
        
        # Retornar resultado completo
        return {
            'success': True,
            'language': 'english',
            'original_input': original_query,
            'normalized_query': normalized_query,
            'tokens': pattern.raw_tokens,
            'conceptual_sql': final_sql,
            'sql_query': final_sql,
            'complexity_level': 'multi_metric',
            'processing_method': 'multi_metric_direct',
            'note': '📊 Processed with Multi-Metric Pattern',
            'query_structure': {
                'pattern': 'MULTI_METRIC',
                'metrics': pattern.metrics,
                'operations': pattern.operations,
                'dimension': pattern.dimension,
                'filters': pattern.filters
            },
            'confidence': pattern.confidence
        }
        

    def detect_this_week_pattern_english(self, tokens: List[str]) -> Optional[ThisWeekPattern]:
        """
        📅 DETECTOR DE PATRÓN 'THIS WEEK'
        Detecta: "store with more sales this week"
        """
        
        print(f"📅 DETECTING THIS WEEK PATTERN:")
        print(f"   📤 Tokens: {tokens}")
        
        # Buscar "this week" en los tokens
        this_week_patterns = [
            ['this', 'week'],
            ['this_week'],
            ['thisweek']
        ]
        
        for pattern in this_week_patterns:
            pattern_length = len(pattern)
            
            for i in range(len(tokens) - pattern_length + 1):
                # Verificar si los tokens coinciden con el patrón
                match = True
                for j, pattern_token in enumerate(pattern):
                    if tokens[i + j].lower() != pattern_token.lower():
                        match = False
                        break
                
                if match:
                    indicator_text = ' '.join(tokens[i:i + pattern_length])
                    
                    this_week_pattern = ThisWeekPattern(
                        indicator_text=indicator_text,
                        position_start=i,
                        position_end=i + pattern_length - 1,
                        confidence=0.95,
                        raw_tokens=tokens[i:i + pattern_length]
                    )
                    
                    print(f"📅 THIS WEEK PATTERN DETECTED:")
                    print(f"   📅 Text: '{indicator_text}'")
                    print(f"   📍 Positions: {i}-{i + pattern_length - 1}")
                    print(f"   ⭐ Confidence: {this_week_pattern.confidence:.2f}")
                    
                    return this_week_pattern
        
        print(f"   ❌ No 'this week' pattern found")
        return None


    def detect_multi_metric_pattern_english(self, tokens: List[str]) -> Optional[MultiMetricPattern]:
        """
        📊 DETECTOR DE PATRÓN MULTI-MÉTRICA EN INGLÉS
        Detecta consultas con múltiples métricas
        """
        
        print(f"📊 DETECTING MULTI-METRIC PATTERN:")
        print(f"   🔤 Tokens: {tokens}")
        
        # Métricas conocidas
        known_metrics = {
            'sales', 'inventory', 'profit', 'revenue', 'margin', 
            'cost', 'stock', 'units', 'amount', 'quantity', 'volume'
        }
        
        # Operaciones conocidas
        known_operations = {
            'total', 'sum', 'average', 'avg', 'max', 'min', 'count'
        }
        
        # STEP 1: Buscar métricas en los tokens
        found_metrics = []
        metric_positions = []
        
        for i, token in enumerate(tokens):
            token_lower = token.lower()
            if token_lower in known_metrics:
                found_metrics.append(token_lower)
                metric_positions.append(i)
                print(f"   📊 Metric found: '{token_lower}' at position {i}")
        
        # Necesitamos al menos 2 métricas
        if len(found_metrics) < 2:
            print(f"   ❌ Not enough metrics (found {len(found_metrics)})")
            return None
        
        # STEP 2: Buscar conectores
        has_and = 'and' in [t.lower() for t in tokens]
        has_comma = any(',' in t for t in tokens)
        
        if not (has_and or has_comma):
            print(f"   ❌ No connectors found between metrics")
            return None
        
        print(f"   ✅ Found {len(found_metrics)} metrics with connectors")
        
        # STEP 3: Buscar operaciones
        found_operations = []
        for token in tokens:
            if token.lower() in known_operations:
                found_operations.append(token.lower())
                print(f"   ⚡ Operation found: '{token.lower()}'")
        
        # Si no hay operaciones, usar 'total' por defecto
        if not found_operations:
            found_operations = ['total'] * len(found_metrics)
            print(f"   ⚡ Using default operation: 'total'")
        
        # STEP 4: Buscar dimensión
        dimension = None
        dimension_keywords = ['store', 'account', 'item', 'product', 'customer', 'week', 'month']
        for token in tokens:
            if token.lower() in dimension_keywords:
                dimension = token.lower()
                print(f"   📍 Dimension found: '{dimension}'")
                break
        
        # STEP 5: Buscar filtros simples (of X, in X)
        filters = []
        for i in range(len(tokens) - 1):
            if tokens[i].lower() in ['of', 'in', 'for']:
                next_token = tokens[i + 1]
                # Verificar si el siguiente token es un valor (mayúsculas o alfanumérico)
                if next_token.isupper() or (next_token.isalnum() and not next_token.islower()):
                    filters.append({
                        'type': 'simple',
                        'column': 'account',  # Por defecto
                        'value': next_token.upper()
                    })
                    print(f"   🔍 Filter found: {next_token.upper()}")
        
        # STEP 6: Calcular confianza
        confidence = 0.7  # Base
        confidence += min(len(found_metrics) * 0.05, 0.15)
        if has_and or has_comma:
            confidence += 0.1
        if dimension:
            confidence += 0.05
        
        # Crear el patrón
        pattern = MultiMetricPattern(
            metrics=found_metrics,
            operations=found_operations,
            has_dimension=dimension is not None,
            dimension=dimension,
            has_filters=len(filters) > 0,
            filters=filters,
            confidence=min(1.0, confidence),
            raw_tokens=tokens
        )
        
        print(f"📊 MULTI-METRIC PATTERN DETECTED:")
        print(f"   📊 Metrics: {found_metrics}")
        print(f"   ⚡ Operations: {found_operations}")
        print(f"   📍 Dimension: {dimension}")
        print(f"   🔍 Filters: {len(filters)}")
        print(f"   ⭐ Confidence: {pattern.confidence:.2f}")
        
        return pattern


    def detect_stock_out_pattern_english(self, tokens: List[str]) -> Optional[YNColumnPattern]:
        """
        📦 DETECTOR DE PATRÓN 'IN STOCK OUT'
        Detecta: "which products are in stock out" vs "which products are not in stock out"
        """
        
        print(f"📦 DETECTING STOCK OUT PATTERN:")
        print(f"   📤 Tokens: {tokens}")
        
        # Buscar patrones de stock out
        stock_out_patterns = [
            # Patrones afirmativos (Stock_Out = 'Y')
            (['in', 'stock', 'out'], True),
            (['in_stock_out'], True),
            (['instockout'], True),
            
            # Patrones negativos (Stock_Out = 'N')
            (['not', 'in', 'stock', 'out'], False),
            (['not_in_stock_out'], False),
            (['aren\'t', 'in', 'stock', 'out'], False),
            (['arent', 'in', 'stock', 'out'], False),
            (['are', 'not', 'in', 'stock', 'out'], False),
        ]
        
        for pattern_tokens, is_positive in stock_out_patterns:
            pattern_length = len(pattern_tokens)
            
            for i in range(len(tokens) - pattern_length + 1):
                # Verificar si los tokens coinciden con el patrón
                match = True
                for j, pattern_token in enumerate(pattern_tokens):
                    if tokens[i + j].lower() != pattern_token.lower():
                        match = False
                        break
                
                if match:
                    indicator_text = ' '.join(tokens[i:i + pattern_length])
                    negation_detected = not is_positive
                    
                    stock_out_pattern = YNColumnPattern(
                        is_in_stock_out=is_positive,
                        negation_detected=negation_detected,
                        indicator_text=indicator_text,
                        position_start=i,
                        position_end=i + pattern_length - 1,
                        confidence=0.95,
                        raw_tokens=tokens[i:i + pattern_length]
                    )
                    
                    print(f"📦 STOCK OUT PATTERN DETECTED:")
                    print(f"   📦 Text: '{indicator_text}'")
                    print(f"   ✅ Is in stock out: {is_positive}")
                    print(f"   🚫 Negation detected: {negation_detected}")
                    print(f"   📍 Positions: {i}-{i + pattern_length - 1}")
                    print(f"   ⭐ Confidence: {stock_out_pattern.confidence:.2f}")
                    print(f"   🎯 SQL Value: Stock_Out = {'Y' if is_positive else 'N'}")
                    
                    return stock_out_pattern
        
        print(f"   ❌ No stock out pattern found")
        return None


    def generate_this_week_sql_condition(self) -> str:
        """
        📅 GENERADOR DE CONDICIÓN SQL PARA 'THIS WEEK'
        Genera: WHERE week = (SELECT MAX(week) FROM datos)
        """
        
        print(f"📅 GENERATING THIS WEEK SQL CONDITION:")
        
        # Usar subconsulta para obtener la semana máxima
        condition = "week = (SELECT MAX(week) FROM datos)"
        
        print(f"   📅 This week condition: {condition}")
        return condition


    def generate_stock_out_sql_condition(self, pattern: YNColumnPattern) -> str:
        """
        📦 GENERADOR DE CONDICIÓN SQL PARA STOCK OUT
        """
        
        print(f"📦 GENERATING STOCK OUT SQL CONDITION:")
        print(f"   📦 Is in stock out: {pattern.is_in_stock_out}")
        
        if pattern.is_in_stock_out:
            # "in stock out" → Stock_Out = 'Y'
            condition = "Stock_Out = 'Y'"
        else:
            # "not in stock out" → Stock_Out = 'N'  
            condition = "Stock_Out = 'N'"
        
        print(f"   📦 Stock out condition: {condition}")
        return condition


    def _is_potential_row_object_english(self, token: str) -> bool:
        """📋 VERIFICADOR DE OBJETOS DE FILA PARA SHOW ROWS"""
        token_lower = token.lower()
        
        # Objetos que representan filas/registros
        row_objects = {
            'rows', 'row', 'records', 'record', 'entries', 'entry', 
            'lines', 'line', 'items', 'item', 'data', 'results'
        }
        
        return token_lower in row_objects


    def detect_and_apply_count_pattern(self, query_structure: QueryStructure, tokens: List[str]) -> QueryStructure:
        """
        🔢 DETECTA Y APLICA PATRÓN COUNT - VERSIÓN CORREGIDA
        Evita falsos positivos con "count of" vs "account of"
        """
        
        # Crear texto completo para buscar patrones
        full_text = ' '.join(tokens).lower()
        
        print(f"🔢 DETECTING COUNT PATTERNS IN: '{full_text}'")
        
        # 🆕 PATRONES MÁS ESPECÍFICOS Y CONTEXTUALES
        count_patterns = [
            'how many', 'how much', 'cuántos', 'cuántas', 'cuantos', 'cuantas',
            'total number', 'number of', 'cantidad de', 'número de'
        ]
        # REMOVIDO 'count of' porque causa falsos positivos con "account of"
        
        # 🆕 DETECTAR "COUNT OF" SOLO EN CONTEXTO CORRECTO
        count_of_pattern = False
        if 'count of' in full_text:
            # Verificar que NO sea parte de otra construcción como "account of"
            count_of_index = full_text.find('count of')
            if count_of_index != -1:
                # Verificar que "count" esté al inicio de una palabra (no sea parte de "account")
                is_part_of_account = (count_of_index > 0 and 
                                    full_text[count_of_index - 1:count_of_index + 8] == 'account of')
                
                if not is_part_of_account:
                    # Verificar que después de "count of" hay una métrica, no una preposición
                    after_count_of = full_text[count_of_index + 8:].strip()  # 8 = len('count of')
                    
                    # Si después de "count of" hay construcciones como "item", "store", es falso positivo
                    if after_count_of:
                        first_words = after_count_of.split()[:2]  # Primeras 2 palabras
                        false_positive_constructions = [
                            'item', 'store', 'account', 'product', 'customer', 'brand',
                            'category', 'line', 'city', 'state', 'country'
                        ]
                        
                        is_false_positive = any(word in false_positive_constructions for word in first_words)
                        
                        if not is_false_positive:
                            count_of_pattern = True
                            print(f"   ✅ Valid COUNT OF pattern found")
                        else:
                            print(f"   🚫 FALSE POSITIVE: 'count of {first_words[0] if first_words else ''}' - not a count pattern")
                    else:
                        print(f"   🚫 Invalid COUNT OF: no content after 'count of'")
                else:
                    print(f"   🚫 FALSE POSITIVE: 'account of' detected, not 'count of'")
        
        # Verificar otros patrones válidos
        count_detected = count_of_pattern
        detected_pattern = 'count of' if count_of_pattern else None
        
        if not count_detected:
            for pattern in count_patterns:
                if pattern in full_text:
                    count_detected = True
                    detected_pattern = pattern
                    print(f"   ✅ COUNT pattern detected: '{pattern}'")
                    break
        
        if not count_detected:
            print(f"   ❌ No COUNT pattern detected")
            return query_structure  # Devolver sin cambios
        
        # APLICAR TRANSFORMACIÓN COUNT solo si es válida
        print(f"   🔧 Applying COUNT transformation...")
        
        # Agregar operación COUNT si no existe
        count_operation = QueryComponent(
            text=detected_pattern,
            type=ComponentType.OPERATION,
            confidence=0.95,
            subtype='count_operation', 
            value='conteo',
            linguistic_info={
                'source': 'count_pattern_detector',
                'pattern': detected_pattern,
                'sql_function': 'COUNT'
            }
        )
        
        # Agregar a la lista de operaciones
        query_structure.operations.append(count_operation)
        
        # Marcar que es una consulta COUNT
        query_structure.is_count_query = True
        query_structure.count_pattern = detected_pattern
        
        print(f"   ✅ COUNT operation added: {detected_pattern} → conteo")
        print(f"   🔢 Structure marked as COUNT query")
        
        return query_structure


    def _generate_count_sql_simple(self, structure: QueryStructure) -> str:
        """🔢 GENERADOR SQL SIMPLE PARA COUNT"""
        
        print(f"   🔢 Generating COUNT SQL:")
        print(f"      📍 Main dimension: {structure.main_dimension.text if structure.main_dimension else 'None'}")
        print(f"      🔗 Filters: {len(structure.column_conditions)}")
        print(f"      ⏰ Temporal filters: {len(structure.temporal_filters)}")  # AGREGAR
        
        # Determinar qué contar
        if structure.main_dimension:
            count_target = f'COUNT(DISTINCT {structure.main_dimension.text})'
            print(f"      🎯 Counting distinct: {structure.main_dimension.text}")
        else:
            count_target = 'COUNT(*)'
            print(f"      🎯 Counting all records")
        
        # WHERE conditions
        where_conditions = []
        
        # Condiciones de columna
        for condition in structure.column_conditions:
            where_conditions.append(f"{condition.column_name} = '{condition.value}'")
            print(f"      ✅ WHERE: {condition.column_name} = '{condition.value}'")
        
        # AGREGAR: Filtros temporales
        temporal_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
        if temporal_conditions:
            where_conditions.extend(temporal_conditions)
            print(f"      📅 Temporal conditions: {temporal_conditions}")
        
        # Construir SQL
        sql_parts = [f"SELECT {count_target}", "FROM datos"]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"      🎯 COUNT SQL: {final_sql}")
        return final_sql



# =====================================================================
# ========= MÉTODOS COMPLETOS PARA AGREGAR AL FINAL ==================
# =====================================================================

    def detect_superlative_pattern_english(self, tokens: List[str]) -> Optional[SuperlativePattern]:
        """
        🏆 DETECTOR DE PATRÓN SUPERLATIVO EN INGLÉS
        Detecta: "which account sold the most", "who had the least", etc.
        """
        
        print(f"🏆 DETECTING SUPERLATIVE PATTERN:")
        print(f"   🔤 Tokens: {tokens}")
        
        if len(tokens) < 4:  # Mínimo: which store sold most
            return None
        
        # STEP 1: Buscar palabra interrogativa
        question_words = {'which', 'who', 'what', 'where'}
        question_word = None
        question_pos = -1
        
        for i, token in enumerate(tokens):
            if token.lower() in question_words:
                question_word = token.lower()
                question_pos = i
                break
        
        if not question_word:
            print(f"   ❌ No question word found")
            return None
        
        print(f"   ✅ Question word: '{question_word}' at position {question_pos}")
        
        # STEP 2: Buscar dimensión objetivo (después de la palabra interrogativa)
        target_dimension = None
        dimension_pos = -1
        
        for i in range(question_pos + 1, min(question_pos + 3, len(tokens))):
            if i < len(tokens):
                token = tokens[i]
                if self._is_potential_dimension_english(token):
                    target_dimension = token.lower()
                    dimension_pos = i
                    print(f"   ✅ Target dimension: '{target_dimension}' at position {i}")
                    break
        
        if not target_dimension:
            print(f"   ❌ No target dimension found")
            return None
        
        # STEP 3: Buscar verbo de acción
        action_verbs = {
            'sold', 'generated', 'produced', 'made', 'earned', 'achieved',
            'had', 'has', 'got', 'obtained', 'reached', 'recorded'
        }
        
        action_verb = None
        verb_pos = -1
        
        for i in range(dimension_pos + 1, len(tokens)):
            if tokens[i].lower() in action_verbs:
                action_verb = tokens[i].lower()
                verb_pos = i
                print(f"   ✅ Action verb: '{action_verb}' at position {i}")
                break
        
        if not action_verb:
            print(f"   ❌ No action verb found")
            return None
        
        # STEP 4: Buscar superlativo
        superlative_patterns = {
            'the most': {'type': 'most', 'direction': 'DESC'},
            'the least': {'type': 'least', 'direction': 'ASC'},
            'the highest': {'type': 'highest', 'direction': 'DESC'},
            'the lowest': {'type': 'lowest', 'direction': 'ASC'},
            'most': {'type': 'most', 'direction': 'DESC'},
            'least': {'type': 'least', 'direction': 'ASC'},
            'highest': {'type': 'highest', 'direction': 'DESC'},
            'lowest': {'type': 'lowest', 'direction': 'ASC'}
        }
        
        superlative_info = None
        superlative_text = None
        
        # Buscar patrones de superlativo después del verbo
        remaining_text = ' '.join(tokens[verb_pos + 1:]).lower()
        
        for pattern, info in superlative_patterns.items():
            if pattern in remaining_text:
                superlative_info = info
                superlative_text = pattern
                print(f"   ✅ Superlative: '{pattern}' → {info['direction']}")
                break
        
        if not superlative_info:
            print(f"   ❌ No superlative pattern found")
            return None
        
        # STEP 5: Inferir métrica implícita basada en el verbo
        implied_metric = self._infer_metric_from_verb_english(action_verb)
        print(f"   📊 Implied metric from '{action_verb}': {implied_metric}")
        
        # STEP 6: Calcular confianza
        confidence = 0.6  # Base
        confidence += 0.2  # Tiene palabra interrogativa
        confidence += 0.1  # Tiene dimensión
        confidence += 0.1  # Tiene verbo de acción
        confidence += 0.1  # Tiene superlativo
        
        superlative_pattern = SuperlativePattern(
            question_word=question_word,
            target_dimension=target_dimension,
            action_verb=action_verb,
            superlative_type=superlative_info['type'],
            direction=superlative_info['direction'],
            implied_metric=implied_metric,
            confidence=min(1.0, confidence),
            raw_tokens=tokens
        )
        
        print(f"🏆 SUPERLATIVE PATTERN DETECTED:")
        print(f"   ❓ Question: {question_word}")
        print(f"   📍 Target: {target_dimension}")
        print(f"   ⚡ Action: {action_verb}")
        print(f"   🏆 Superlative: {superlative_info['type']} ({superlative_info['direction']})")
        print(f"   📊 Implied metric: {implied_metric}")
        print(f"   ⭐ Confidence: {superlative_pattern.confidence:.2f}")
        
        return superlative_pattern


    def _infer_metric_from_verb_english(self, action_verb: str) -> Optional[str]:
        """
        📊 INFERIR MÉTRICA BASADA EN EL VERBO DE ACCIÓN
        """
        
        verb_to_metric = {
            'sold': 'sales',
            'generated': 'revenue', 
            'produced': 'production',
            'made': 'revenue',
            'earned': 'revenue',
            'achieved': 'performance',
            'had': 'sales',  # Default para "had"
            'has': 'sales',
            'got': 'sales',
            'obtained': 'revenue',
            'reached': 'sales',
            'recorded': 'sales'
        }
        
        inferred = verb_to_metric.get(action_verb.lower())
        print(f"      📊 Verb '{action_verb}' → metric '{inferred}'")
        
        return inferred


    def generate_superlative_sql_english(self, pattern: SuperlativePattern, structure: QueryStructure) -> str:
        """
        🏆 GENERADOR SQL PARA PATRONES SUPERLATIVOS
        """
        
        print(f"🏆 GENERATING SUPERLATIVE SQL:")
        print(f"   📍 Target dimension: {pattern.target_dimension}")
        print(f"   📊 Implied metric: {pattern.implied_metric}")
        print(f"   🎯 Direction: {pattern.direction}")
        
        # PASO 1: Construir SELECT con dimensión + métrica agregada
        select_parts = [pattern.target_dimension]
        
        # Usar métrica implícita o buscar en structure
        metric_to_use = pattern.implied_metric
        if not metric_to_use and structure.metrics:
            metric_to_use = structure.metrics[0].text
        
        if not metric_to_use:
            # Fallback: usar sales como default
            metric_to_use = 'sales'
            print(f"   📊 Using fallback metric: {metric_to_use}")
        
        # Construir función de agregación
        if pattern.superlative_type in ['most', 'highest']:
            agg_function = f'SUM({metric_to_use})'
        elif pattern.superlative_type in ['least', 'lowest']:
            agg_function = f'SUM({metric_to_use})'
        else:
            agg_function = f'SUM({metric_to_use})'
        
        select_parts.append(agg_function)
        
        # PASO 2: Construir WHERE conditions de structure
        where_conditions = []
        
        for condition in structure.column_conditions:
            where_conditions.append(f"{condition.column_name} = '{condition.value}'")
            print(f"   ✅ WHERE condition: {condition.column_name} = '{condition.value}'")
        
        # Filtros temporales
        if structure.temporal_filters:
            temporal_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
            where_conditions.extend(temporal_conditions)
        
        # PASO 3: Construir SQL completo
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            "FROM datos"
        ]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        sql_parts.extend([
            f"GROUP BY {pattern.target_dimension}",
            f"ORDER BY {agg_function} {pattern.direction}",
            "LIMIT 1"
        ])
        
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 Superlative SQL: {final_sql}")
        return final_sql


    def detect_enhanced_yn_column_pattern_english(self, tokens: List[str]):
        """📦 DETECTA STOCK OUT Y DEAD INVENTORY
        
        Detecta:
        - "stock out" → Stock_Out = 'Y'
        - "not stock out" → Stock_Out = 'N'
        - "dead inventory" → Dead_Inventory = 'Y'
        - "without dead inventory" → Dead_Inventory = 'N'
        """
        
        print(f"📦 DETECTING ENHANCED Y/N COLUMN PATTERN:")
        print(f"   📤 Tokens: {tokens}")
        
        # 🔍 PASO 1: Buscar patrones de columnas Y/N
        yn_positions = []
        
        # Configuración de patrones para cada columna
        column_configs = {
            'Stock_Out': {
                'two_words': [['stock', 'out']],
                'single_words': ['stock_out', 'stockout', 'stock-out'],
                'display_name': 'stock out'
            },
            'Dead_Inventory': {
                'two_words': [['dead', 'inventory']],
                'single_words': ['dead_inventory', 'deadinventory', 'dead-inventory'],
                'display_name': 'dead inventory'
            }
        }
        
        # Buscar patrones para cada columna
        for column_name, config in column_configs.items():
            # Buscar patrones de dos palabras separadas
            for word_pair in config['two_words']:
                for i in range(len(tokens) - 1):
                    if (tokens[i].lower() == word_pair[0] and 
                        i + 1 < len(tokens) and 
                        tokens[i + 1].lower() == word_pair[1]):
                        yn_positions.append((i, i + 1, f'{word_pair[0]} {word_pair[1]}', 'separated', column_name))
                        print(f"   ✅ Found '{word_pair[0]} {word_pair[1]}' ({column_name}) at positions {i}-{i+1}")
            
            # Buscar patrones de una sola palabra
            for i in range(len(tokens)):
                token_lower = tokens[i].lower()
                if token_lower in config['single_words']:
                    yn_positions.append((i, i, token_lower, 'single', column_name))
                    print(f"   ✅ Found '{token_lower}' ({column_name}) at position {i}")
            
            # 🆕 NUEVO: Buscar patrones como "in stock_out" o "in dead_inventory"
            for i in range(len(tokens) - 1):
                if (tokens[i].lower() == 'in' and 
                    i + 1 < len(tokens) and 
                    tokens[i + 1].lower() in config['single_words']):
                    yn_positions.append((i, i + 1, f'in {tokens[i + 1]}', 'in_pattern', column_name))
                    print(f"   ✅ Found 'in {tokens[i + 1]}' pattern ({column_name}) at positions {i}-{i+1}")
        
        if not yn_positions:
            print(f"   ❌ No Y/N column pattern found")
            return None
        
        # 🔍 PASO 2: Procesar el primer patrón encontrado
        start_pos, end_pos, pattern_text, pattern_type, column_name = yn_positions[0]
        print(f"   🔍 Analyzing pattern '{pattern_text}' (column: {column_name}, type: {pattern_type}) at {start_pos}-{end_pos}")
        
        # 🆕 CASO ESPECIAL: "in [column]" = POSITIVO (no buscar negaciones)
        if pattern_type == 'in_pattern':
            yn_value = True   # = 'Y'
            negation_found = False
            negation_type = None
            negation_start = start_pos
            indicator_text = pattern_text
            print(f"   ✅ POSITIVE 'in' pattern: '{indicator_text}' → {column_name} = 'Y'")
        else:
            # LÓGICA NORMAL: Buscar negaciones en las 3 posiciones anteriores
            negation_found = False
            negation_type = None
            negation_start = start_pos
            
            # Palabras de negación en inglés
            negation_words = {
                'not': 'not',
                'no': 'no', 
                'without': 'without',
                'aren\'t': 'aren\'t',
                'arent': 'wasn\'t',
                'wasnt': 'aren\'t',
                'isnt': 'isn\'t',
                'isn\'t': 'isn\'t',
                'dont': 'don\'t',
                'don\'t': 'don\'t',
                'never': 'never',
                'doesnt': 'doesn\'t',
                'doesn\'t': 'doesn\'t'
            }
            
            # Buscar negación en las 3 posiciones anteriores
            search_start = max(0, start_pos - 3)
            for neg_pos in range(search_start, start_pos):
                if tokens[neg_pos].lower() in negation_words:
                    negation_found = True
                    negation_type = negation_words[tokens[neg_pos].lower()]
                    negation_start = neg_pos
                    print(f"   🚫 Negation found: '{tokens[neg_pos]}' → '{negation_type}' at position {neg_pos}")
                    break
            
            # 🔍 PASO 3: Determinar valor Y/N
            if negation_found:
                yn_value = False  # = 'N'
                indicator_text = f"{negation_type} {pattern_text}"
                print(f"   ✅ NEGATIVE pattern: '{indicator_text}' → {column_name} = 'N'")
            else:
                yn_value = True   # = 'Y'
                indicator_text = pattern_text
                print(f"   ✅ POSITIVE pattern: '{indicator_text}' → {column_name} = 'Y'")
        
        # 🔍 PASO 4: Calcular confianza
        confidence = 0.90  # Alta confianza para patrones directos
        
        # Bonus por patrones específicos
        if pattern_type == 'separated':  # Patrón de dos palabras es más natural
            confidence += 0.05
        
        if negation_found:
            confidence += 0.03  # Negación clara
        
        confidence = min(1.0, confidence)
        
        # 🔍 PASO 5: Crear YNColumnPattern
        yn_pattern = YNColumnPattern(
            column_name=column_name,  # Ahora es dinámico: Stock_Out o Dead_Inventory
            value='Y' if yn_value else 'N',
            negation_detected=negation_found,
            indicator_text=indicator_text,
            position_start=negation_start if negation_found else start_pos,
            position_end=end_pos,
            confidence=confidence,
            raw_tokens=tokens[negation_start if negation_found else start_pos:end_pos + 1]
        )
        
        print(f"📦 Y/N COLUMN PATTERN DETECTED:")
        print(f"   📋 Column: {column_name}")
        print(f"   📦 Text: '{indicator_text}'")
        print(f"   ✅ Value: {'Y' if yn_value else 'N'}")
        print(f"   🚫 Negation detected: {negation_found}")
        print(f"   📍 Positions: {negation_start if negation_found else start_pos}-{end_pos}")
        print(f"   ⭐ Confidence: {confidence:.2f}")
        print(f"   🎯 SQL: {column_name} = '{yn_pattern.value}'")
        
        # Retornar el primer patrón encontrado (más específico)
        return yn_pattern


    def detect_groupby_pattern_english(self, tokens: List[str]) -> Optional[QueryComponent]:
        """
        🔍 DETECTOR DE PATRÓN 'BY [DIMENSIÓN]' usando diccionarios existentes
        """
        
        print(f"🔍 DETECTING GROUP BY PATTERN:")
        
        for i, token in enumerate(tokens):
            if token.lower() == 'by' and i + 1 < len(tokens):
                next_token = tokens[i + 1]
                
                # Usar el método existente que ya consulta self.dictionaries.dimensiones
                column_info = self._identify_potential_column_english(next_token)
                
                if column_info['is_column'] and column_info['type'] == 'dimension':
                    # Es una dimensión válida según el diccionario
                    normalized_dimension = column_info['normalized_name']
                    
                    groupby_dimension = QueryComponent(
                        text=normalized_dimension,
                        type=ComponentType.DIMENSION,
                        confidence=0.95,
                        subtype='groupby_dimension',
                        linguistic_info={
                            'source': 'by_pattern',
                            'original_form': next_token,
                            'pattern': f'by {next_token}'
                        }
                    )
                    
                    print(f"   ✅ GROUP BY pattern detected: 'by {next_token}' → GROUP BY {normalized_dimension}")
                    return groupby_dimension
        
        return None


    def generate_enhanced_list_all_sql_english(self, pattern_data: Dict, structure: QueryStructure) -> str:
        """
        📋 GENERADOR SQL INTELIGENTE PARA LIST ALL - CORREGIDO PARA VALORES ÚNICOS
        """
        
        print(f"📋 GENERATING ENHANCED LIST ALL SQL (WITH DISTINCT/GROUP BY LOGIC):")
        
        target_dimension = pattern_data['target_dimension']
        list_indicator = pattern_data['list_indicator']
        has_aggregation = pattern_data.get('has_aggregation', False)
        
        print(f"   📋 List type: {list_indicator}")
        print(f"   📍 Target dimension: {target_dimension}")
        print(f"   📊 Has aggregation: {has_aggregation}")
        
        # PASO 1: Construir SELECT
        formatted_dim = self.format_temporal_dimension(target_dimension)
        select_parts = []
        
        # 🔧 LÓGICA CORREGIDA: Determinar si necesitamos DISTINCT o GROUP BY
        needs_group_by = False
        use_distinct = True  # Por defecto usar DISTINCT
        
        # PASO 1.5: SI HAY MÉTRICAS Y OPERACIONES, AGREGARLAS
        if has_aggregation or (structure.operations and structure.metrics):
            print(f"   📊 Detected metrics and operations - adding aggregations")
            needs_group_by = True
            use_distinct = False  # No usar DISTINCT cuando hay GROUP BY
            
            # Agregar dimensión SIN DISTINCT
            select_parts.append(formatted_dim)
            
            # Procesar operaciones y métricas
            if structure.operations and structure.metrics:
                for i, metric in enumerate(structure.metrics):
                    # Determinar operación para esta métrica
                    if i < len(structure.operations):
                        operation = structure.operations[i]
                    else:
                        operation = structure.operations[0] if structure.operations else None
                    
                    if operation:
                        operation_value = operation.value
                        
                        # Mapear operación a función SQL
                        if operation_value == 'máximo':
                            agg_function = self._get_contextual_aggregation_english(
                                structure, metric.text, operation_value
                            )
                        else:
                            sql_operations = {
                                'mínimo': f'MIN({metric.text})',
                                'suma': f'SUM({metric.text})',
                                'promedio': f'AVG({metric.text})',
                                'conteo': f'COUNT({metric.text})',
                                'total': f'SUM({metric.text})'
                            }
                            agg_function = sql_operations.get(operation_value, f'SUM({metric.text})')
                        
                        # Agregar alias descriptivo
                        alias = f"total_{metric.text}" if 'sum' in agg_function.lower() else agg_function
                        select_parts.append(f"{agg_function} as {alias}")
                        print(f"   ✅ Added aggregation: {agg_function} as {alias}")
                    else:
                        # Si no hay operación, asumir SUM por defecto
                        select_parts.append(f"SUM({metric.text}) as total_{metric.text}")
                        print(f"   ✅ Added default aggregation: SUM({metric.text})")
            
            # Si solo hay métricas sin operaciones
            elif structure.metrics and not structure.operations:
                for metric in structure.metrics:
                    select_parts.append(f"SUM({metric.text}) as total_{metric.text}")
                    print(f"   ✅ Added metric aggregation: SUM({metric.text})")
        else:
            # 🔧 CASO SIMPLE: Solo listar valores únicos
            print(f"   📋 Simple list - using DISTINCT")
            use_distinct = True
            needs_group_by = False
            select_parts.append(formatted_dim)
        
        # 🔧 CONSTRUIR SELECT CLAUSE CON O SIN DISTINCT
        if use_distinct:
            select_clause = f"SELECT DISTINCT {', '.join(select_parts)}"
            print(f"   ✅ Using DISTINCT for unique values")
        else:
            select_clause = f"SELECT {', '.join(select_parts)}"
            print(f"   ✅ Not using DISTINCT (GROUP BY will handle uniqueness)")
        
        # PASO 2: FROM clause
        from_part = "FROM datos"
        
        # PASO 3: WHERE conditions
        where_conditions = []
        
        # 3.1: Filtros de columna
        for condition in structure.column_conditions:
            # No agregar la dimensión objetivo como filtro si es la misma que estamos listando
            if condition.column_name.lower() != target_dimension.lower():
                sql_condition = f"{condition.column_name} = '{condition.value}'"
                where_conditions.append(sql_condition)
                print(f"   ✅ Column filter: {sql_condition}")
        
        # 3.2: Filtros temporales
        temporal_conditions = self._get_temporal_conditions_for_list_all(structure)
        if temporal_conditions:
            where_conditions.extend(temporal_conditions)
        
        # 3.3: Filtros de exclusión
        if hasattr(structure, 'exclusion_filters'):
            for exclusion in structure.exclusion_filters:
                if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                    sql_condition = f"{exclusion.column_name} != '{exclusion.value}'"
                    where_conditions.append(sql_condition)
                    print(f"   ✅ Exclusion filter: {sql_condition}")
        
        # PASO 4: GROUP BY si es necesario
        group_by_part = None
        if needs_group_by:
            group_by_part = f"GROUP BY {target_dimension}"
            print(f"   ✅ GROUP BY added: {target_dimension}")
        
        # PASO 5: ORDER BY
        order_by_parts = []
        
        # Si hay agregaciones, podemos ordenar por ellas también
        if needs_group_by and len(select_parts) > 1:
            # Extraer la primera función de agregación para ordenar por ella
            for part in select_parts[1:]:  # Saltar la dimensión
                if 'SUM' in part or 'MAX' in part or 'MIN' in part or 'AVG' in part:
                    # Extraer el alias o la función completa
                    if ' as ' in part:
                        alias = part.split(' as ')[1]
                        order_by_parts.append(f"{alias} DESC")
                    else:
                        order_by_parts.append(f"{part} DESC")
                    break
        
        # Siempre ordenar también por la dimensión
        order_by_parts.append(target_dimension)
        
        order_part = f"ORDER BY {', '.join(order_by_parts)}" if order_by_parts else f"ORDER BY {target_dimension}"
        
        # PASO 6: Construir SQL final
        sql_parts = [select_clause, from_part]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_part:
            sql_parts.append(group_by_part)
        
        sql_parts.append(order_part)
        
        # Agregar LIMIT para consultas muy grandes
        # Puedes descomentar esto si quieres limitar resultados por defecto
        # if not needs_group_by:  # Solo para listados simples
        #     sql_parts.append("LIMIT 1000")
        
        final_sql = " ".join(sql_parts) + ";"
        
        print(f"   🎯 Enhanced LIST ALL SQL: {final_sql}")
        return final_sql


    def _get_temporal_conditions_for_list_all(self, structure: QueryStructure) -> List[str]:
        """
        ⏰ MÓDULO DE FILTROS TEMPORALES PARA LIST ALL
        Reutiliza la lógica existente de filtros temporales
        """
        
        print(f"   ⏰ Processing temporal conditions for LIST ALL...")
        
        # Reutilizar el método existente que ya funciona bien
        if hasattr(self, 'get_advanced_temporal_sql_conditions_english'):
            temporal_conditions = self.get_advanced_temporal_sql_conditions_english(structure)
            print(f"   ⏰ Found {len(temporal_conditions)} temporal conditions")
            return temporal_conditions
        else:
            print(f"   ❌ Temporal method not available")
            return []




# =========================================================        
# =========== PIPELINE PARA CONSULTAS EN ESPAÑOL ==========
# =========================================================     


# clase dedicada al manejo de las consultas de principio a fin
class UnifiedNLPParser:
    """Parser NLP unificado - EXPANDIDO para datos referenciados"""
    
                
                
        # ====================================================
        # GRUPO 1: CONFIGURACIÓN Y CONTROL 
        # Funciones de inicialización y coordinación principal
        # ====================================================
    
    
# ------ "Inicializador del Sistema" -------
    
    def __init__(self, enable_logging: bool = True):
        """Inicializador del Sistema - VERSIÓN MEJORADA"""
        self.dictionaries = JSONDictionaryLoader()
        self.enable_logging = enable_logging
        self.query_history = []
        
        # Analizador pre-mapeo (NO afecta diccionarios)
        self.pre_mapping_analyzer = PreMappingSemanticAnalyzer()
        
        self.session_stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'simple_queries': 0,
            'complex_queries': 0,
            'session_start': datetime.now()
        }
        
        
        
# -------------------------------------------------------------------------
# ---------------- CONTROL DE CONSULTAS DESCONOCIDAS ----------------------
# -------------------------------------------------------------------------        
        
        
# Sistema de palabras desconocidas
        self.unknown_words_log_path = "control/consultas_sin_respuestas/unknown_words_log.json"
        self.confidence_threshold = 0.6
        self.unknown_words_log = self._load_unknown_words_log()
        self.session_id = self._generate_session_id()
        
        print("🚀 Parser NLP Unificado iniciado")
        print(f"📚 Diccionarios cargados: {self.dictionaries.get_statistics()}")
        print(f"🚨 Sistema de palabras desconocidas activado")
        print(f"📁 Log de palabras desconocidas: {self.unknown_words_log_path}")


# Método auxiliar deteccion de palabras desconocidas
    def _generate_session_id(self) -> str:
        """Generar ID único de sesión"""
        return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    
# metodo de apoyo para la deteccion de palabras desconocidas
    def _load_unknown_words_log(self) -> Dict:
        """Cargar log existente de palabras desconocidas"""
        if os.path.exists(self.unknown_words_log_path):
            try:
                with open(self.unknown_words_log_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"📋 Log de palabras desconocidas cargado: {len(data.get('failures', []))} consultas previas")
                    return data
            except Exception as e:
                print(f"⚠️ Error cargando log: {e}")
        
        return {
            'failures': [],
            'statistics': {
                'total_failures': 0,
                'most_common_unknown_words': {},
                'last_updated': datetime.now().isoformat()
            }
        }
    


# =============================================
# MÉTODOS DE DETECCIÓN DE PALABRAS DESCONOCIDAS
# =============================================


    # Método para manejar palabras desconocidas
        
    def detect_unknown_words(self, tokens: List[str], classified_components: Dict) -> Tuple[List[UnknownWord], bool]:
        """
        🔍 DETECTOR PRINCIPAL DE PALABRAS DESCONOCIDAS - VERSIÓN CON TEMPORAL
        Retorna: (lista_palabras_desconocidas, hay_palabras_críticas)
        """
        unknown_words = []
        has_critical_unknowns = False
        
        print(f"\n🔍 VERIFICANDO PALABRAS DESCONOCIDAS:")
        
        for i, token in enumerate(tokens):
            # Obtener contexto
            context_before = tokens[max(0, i-2):i]
            context_after = tokens[i+1:min(len(tokens), i+3)]
            
            # Verificar si el token está clasificado
            component = classified_components.get(token)
            
            if component is None:
                # Buscar en diccionario temporal antes de marcar como desconocido
                temporal_entry = self.dictionaries.search_in_temporal_dictionary(token)
                
                if temporal_entry:
                    # Encontrado en temporal - crear componente temporal
                    temporal_type = self.dictionaries.get_temporal_component_type(token)
                    temporal_component = QueryComponent(
                        text=token,
                        type=temporal_type or ComponentType.VALUE,
                        confidence=temporal_entry.get('confidence', 0.9),
                        subtype='temporal_data',
                        value=temporal_entry.get('original_value'),
                        column_name=temporal_entry.get('column_name'),
                        linguistic_info={
                            'source': 'temporal_dictionary',
                            'original_value': temporal_entry.get('original_value'),
                            'column_name': temporal_entry.get('column_name'),
                            'column_type': temporal_entry.get('column_type')
                        }
                    )
                    
                    # Agregar al diccionario de componentes clasificados
                    classified_components[token] = temporal_component
                    
                    print(f"   ✅ TEMPORAL: '{token}' encontrado como {temporal_entry.get('original_value')} en {temporal_entry.get('column_name')}")
                    continue
                
                # Si no está en temporal tampoco, entonces es desconocido
                unknown_word = UnknownWord(
                    word=token,
                    position=i,
                    context_before=context_before,
                    context_after=context_after,
                    suggested_type='unknown',
                    confidence=0.0,
                    timestamp=datetime.now().isoformat(),
                    full_query=' '.join(tokens)
                )
                unknown_words.append(unknown_word)
                has_critical_unknowns = True
                print(f"   ❌ CRÍTICO: '{token}' no encontrado en operacionales NI temporal")
                
            elif component.confidence < self.confidence_threshold:
                # Token con confianza muy baja - mantener lógica existente
                unknown_word = UnknownWord(
                    word=token,
                    position=i,
                    context_before=context_before,
                    context_after=context_after,
                    suggested_type=component.type.value,
                    confidence=component.confidence,
                    timestamp=datetime.now().isoformat(),
                    full_query=' '.join(tokens)
                )
                unknown_words.append(unknown_word)
                
                if component.confidence < 0.4:
                    has_critical_unknowns = True
                    print(f"   🚨 CRÍTICO: '{token}' confianza muy baja ({component.confidence:.2f})")
                else:
                    print(f"   ⚠️ SOSPECHOSO: '{token}' confianza baja ({component.confidence:.2f})")
        
        print(f"📊 Palabras desconocidas: {len(unknown_words)} | Críticas: {has_critical_unknowns}")
        return unknown_words, has_critical_unknowns
        
        
    # DETENER PROCESAMIENTO EN CASO DE DATO DESCONOCIDO
        
    def should_stop_processing(self, unknown_words: List[UnknownWord], query_complexity: str) -> bool:
        """🛑 DECISOR: ¿Debe detenerse el procesamiento?"""
        if not unknown_words:
            return False
        
        critical_words = [w for w in unknown_words if w.confidence < 0.4]
        
        print(f"🛑 EVALUANDO DETENCIÓN: {len(critical_words)} críticas, complejidad: {query_complexity}")
        
        # REGLAS DE DECISIÓN
        if len(critical_words) >= 2:
            print(f"   🛑 DETENER: Demasiadas palabras críticas")
            return True
        
        if len(critical_words) >= 1 and query_complexity in ['compleja', 'muy_compleja']:
            print(f"   🛑 DETENER: Palabra crítica en consulta compleja")
            return True
        
        total_tokens = len(unknown_words[0].full_query.split()) if unknown_words else 0
        unknown_percentage = len(unknown_words) / total_tokens if total_tokens > 0 else 0
        
        if unknown_percentage > 0.3:
            print(f"   🛑 DETENER: Demasiados tokens desconocidos ({unknown_percentage:.1%})")
            return True
        
        if len(critical_words) > 0:
            print(f"   🛑 DETENER: Modo conservador - hay palabra crítica")
            return True
        
        print(f"   ✅ CONTINUAR: Sin problemas críticos")
        return False
    
    
    # FEED BACK PARA EL USUARIO
        
    def generate_user_feedback(self, unknown_words: List[UnknownWord], original_query: str) -> Dict:
        """💡 GENERAR FEEDBACK ÚTIL PARA EL USUARIO"""
        feedback = {
            'type': 'error',
            'original_query': original_query,
            'unknown_words': [],
            'suggestions': [],
            'similar_words': []
        }
        
        # Procesar cada palabra desconocida
        for word in unknown_words:
            word_info = {
                'word': word.word,
                'position': word.position,
                'context': f"...{' '.join(word.context_before)} [{word.word}] {' '.join(word.context_after)}...",
                'confidence': word.confidence,
                'severity': 'critical' if word.confidence < 0.4 else 'suspicious'
            }
            feedback['unknown_words'].append(word_info)
        
        # Generar sugerencias
        feedback['suggestions'] = self._generate_suggestions(unknown_words)
        feedback['similar_words'] = self._find_similar_words(unknown_words)
        
        return feedback
    
    
    # OFRECER SOLUCIONES TEMPORALES AL USUARIO
        
    def _generate_suggestions(self, unknown_words: List[UnknownWord]) -> List[str]:
        """Generar sugerencias útiles"""
        suggestions = [
            "Verifica la ortografía de las palabras no reconocidas",
            "Usa términos del vocabulario: account, tienda, partner_code, ventas, inventario",
            "Para términos compuestos usa guiones bajos: sales_amount, customer_id",
            "Operaciones válidas: mas, mayor, menor, suma, promedio, maximo, minimo"
        ]
        
        return suggestions
    
    
    # OFRECER PALABRAS SIMILARES 
    
    def _find_similar_words(self, unknown_words: List[UnknownWord]) -> List[Dict]:
        """Buscar palabras similares"""
        similar_words = []
        
        common_alternatives = {
            'cuenta': 'account', 'cuentas': 'account',
            'tiendas': 'tienda', 'producto': 'product',
            'venta': 'ventas', 'sale': 'ventas',
            'inventari': 'inventario', 'stock': 'inventario',
            'maximo': 'mas', 'máximo': 'mas',
            'minimo': 'menor', 'mínimo': 'menor'
        }
        
        for word in unknown_words:
            word_lower = word.word.lower()
            for incorrect, correct in common_alternatives.items():
                if incorrect in word_lower:
                    similar_words.append({
                        'original': word.word,
                        'suggested': correct,
                        'reason': 'Término similar encontrado'
                    })
        
        return similar_words
    
    
    
    # VERIFICAR LA CONSULTA FALLIDA
    
    def log_query_failure(self, original_query: str, unknown_words: List[UnknownWord]):
        """📝 REGISTRAR CONSULTA FALLIDA"""
        failure = QueryFailure(
            original_query=original_query,
            unknown_words=[asdict(word) for word in unknown_words],
            timestamp=datetime.now().isoformat(),
            session_id=self.session_id
        )
        
        self.unknown_words_log['failures'].append(asdict(failure))
        self._update_unknown_statistics(unknown_words)
        self._save_unknown_log()
        
        print(f"📝 Consulta fallida registrada con {len(unknown_words)} palabras desconocidas")
    
    
    # ACTUALIZAR LA LISTA DE PALABRAS NO RECONOCIDAS
    
    def _update_unknown_statistics(self, unknown_words: List[UnknownWord]):
        """Actualizar estadísticas"""
        stats = self.unknown_words_log['statistics']
        stats['total_failures'] += 1
        
        if 'most_common_unknown_words' not in stats:
            stats['most_common_unknown_words'] = {}
        
        for word in unknown_words:
            word_key = word.word.lower()
            if word_key not in stats['most_common_unknown_words']:
                stats['most_common_unknown_words'][word_key] = {'count': 0, 'contexts': []}
            
            stats['most_common_unknown_words'][word_key]['count'] += 1
            stats['most_common_unknown_words'][word_key]['contexts'].append(word.full_query)
        
        stats['last_updated'] = datetime.now().isoformat()
    
    
    #  GUARDAR LA PALABRA NO RECONOCIDA
            
    def _save_unknown_log(self):
        """Guardar log en archivo JSON"""
        try:
            with open(self.unknown_words_log_path, 'w', encoding='utf-8') as f:
                json.dump(self.unknown_words_log, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Error guardando log: {e}")



# ====================================
# PROCESAMIENTO DE INPUT POST FILTRADO
# ====================================


# ------ "Punto de Entrada Principal" -------

    def process_user_input(self, user_input: str) -> Dict:
        """Punto de Entrada Principal"""
        self.session_stats['total_queries'] += 1
        
        query_entry = {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'input': user_input,
            'processed': False
        }
        
        try:
            result = self.analyze_unified_query(user_input)
            
            if result.get('success', False):
                self.session_stats['successful_queries'] += 1
                query_entry['processed'] = True
                query_entry['result'] = result
                
                if result.get('complexity_level') in ['simple', 'moderada']:
                    self.session_stats['simple_queries'] += 1
                else:
                    self.session_stats['complex_queries'] += 1
            else:
                self.session_stats['failed_queries'] += 1
                query_entry['error'] = result.get('error', 'Error desconocido')
            
            self.query_history.append(query_entry)
            return result
            
        except Exception as e:
            self.session_stats['failed_queries'] += 1
            error_result = {
                'success': False,
                'error': f"Error procesando consulta: {str(e)}",
                'original_input': user_input,
                'suggestions': self.generate_error_suggestions(user_input)
            }
            
            query_entry['error'] = str(e)
            self.query_history.append(query_entry)
            return error_result
    
    
# ------  "Coordinador de Pipeline" -------
    

    def analyze_unified_query(self, query: str) -> Dict:
        """Cerebro Coordinador del Pipeline - ROUTER LIMPIO"""
        if not query or not query.strip():
            return {
                'success': False,
                'error': 'Consulta vacía',
                'suggestions': ['Intenta con: "partner code con mas ventas"']
            }
        
        print(f"\n🔍 ANALIZANDO CONSULTA: '{query}'")
        
        # PASO 0.1: NORMALIZAR FRASES COMPUESTAS PRIMERO
        pre_normalized_query = self.dictionaries._detect_compound_phrases_dictionary_based(query)
        
        # PASO 0.2: DETECCIÓN DE IDIOMA CON TOKENS YA NORMALIZADOS
        preliminary_tokens = pre_normalized_query.lower().split()
        detected_language = self.dictionaries.detect_language_from_tokens(preliminary_tokens)
        
        print(f"🌍 IDIOMA DETECTADO: {detected_language.upper()}")
        
        # 🎯 ROUTER PRINCIPAL
        if detected_language == 'en':
            print(f"🇺🇸 CONSULTA EN INGLÉS DETECTADA - ENVIANDO A PIPELINE INGLÉS")
            
            # Crear instancia del parser inglés
            english_parser = EnglishNLPParser(self.dictionaries)
            
            # Procesar con pipeline inglés
            return english_parser.process_query(query, pre_normalized_query, preliminary_tokens)
        
        else:
            print(f"🇪🇸 CONSULTA EN ESPAÑOL - ENVIANDO A PIPELINE ESPAÑOL")
            
            # Procesar con pipeline español
            return self.process_spanish_query(query, pre_normalized_query, preliminary_tokens)
            
        
    def process_spanish_query(self, query: str, pre_normalized_query: str, preliminary_tokens: List[str]) -> Dict:
        """🇪🇸 PIPELINE ESPAÑOL - TODO TU CÓDIGO ORIGINAL MOVIDO AQUÍ"""
        
        print(f"🇪🇸 PROCESANDO CONSULTA EN ESPAÑOL")
        
        # PASO 1: NORMALIZACIÓN COMPLETA (ahora usa la query ya pre-normalizada)
        normalized_query = self.normalize_query_with_compounds(pre_normalized_query)
        tokens = normalized_query.split()
        
        print(f"🔤 Tokens: {tokens}")
            
        # PASO 1.5: ANÁLISIS SEMÁNTICO PRE-MAPEO
        original_intent = self.pre_mapping_analyzer.analyze_original_intent(tokens)
        print(f"🧠 Intent semántico original: {original_intent}")
            
        # PASO 2: DETECCIÓN DE PATRONES COMPLEJOS
        temporal_filters = self.detect_temporal_patterns_advanced(tokens)
        column_value_pairs = self.detect_column_value_patterns(tokens, temporal_filters)
            
        # PASO 3: CLASIFICACIÓN DE COMPONENTES
        classified_components = self.classify_all_components(tokens, column_value_pairs)
        
        # PASO 3.5: VERIFICAR PALABRAS DESCONOCIDAS
        unknown_words, has_critical = self.detect_unknown_words(tokens, classified_components)
        
        # Calcular complejidad preliminar para tomar decisión
        preliminary_complexity = self._calculate_preliminary_complexity(
            classified_components, temporal_filters, column_value_pairs
        )
        
        # DECISIÓN: ¿Continuar o detener?
        should_stop = self.should_stop_processing(unknown_words, preliminary_complexity)
        
        if should_stop:
            print(f"🛑 PROCESAMIENTO DETENIDO - Palabras desconocidas críticas")
            
            # Generar feedback detallado
            feedback = self.generate_user_feedback(unknown_words, query)
            
            # Registrar falla
            self.log_query_failure(query, unknown_words)
            
            return {
                'success': False,
                'error': 'Consulta contiene palabras no reconocidas',
                'error_type': 'unknown_words',
                'unknown_words_feedback': feedback,
                'original_input': query,
                'processing_stopped': True,
                'suggestions': feedback['suggestions'],
                'unknown_words_count': len(unknown_words),
                'critical_words': [w.word for w in unknown_words if w.confidence < 0.4],
                'language': 'spanish'
            }
        
        # Si hay palabras sospechosas pero no críticas, continuar con advertencia
        if unknown_words:
            print(f"⚠️ CONTINUANDO con {len(unknown_words)} palabras sospechosas")
        
        # PASO 4: CONSTRUCCIÓN DE ESTRUCTURA
        self._current_original_intent = original_intent
        query_structure = self.build_unified_structure(classified_components, column_value_pairs, temporal_filters, tokens)
        
        # PASO 5: VALIDACIÓN
        validation_result = self.validate_structure(query_structure)
        if not validation_result['valid']:
            return {
                'success': False,
                'error': validation_result['error'],
                'original_input': query,
                'suggestions': validation_result['suggestions'],
                'partial_analysis': self.structure_to_dict(query_structure),
                'language': 'spanish'
            }

        hierarchical_structure = self.generate_hierarchical_structure(query_structure)
        sql_query = self.generate_optimized_sql(query_structure)
        
        # PASO FINAL: Normalización de esquema SQL
        try:
            schema_mapper = SQLSchemaMapper()
            sql_query = schema_mapper.normalize_sql(sql_query)
            print(f"🔗 SQL normalizado aplicado")
        except Exception as e:
            print(f"⚠️ Error en normalización SQL: {e}")
            print(f"🔄 Continuando con SQL original")
                
        # RESULTADO CON INFORMACIÓN ADICIONAL
        result = {
            'success': True,
            'language': 'spanish',
            'original_input': query,
            'normalized_query': normalized_query,
            'tokens': tokens,
            'query_structure': self.structure_to_dict(query_structure),
            'hierarchical_structure': hierarchical_structure,
            'classified_components': {token: self.component_to_dict(comp) 
                                    for token, comp in classified_components.items()},
            'column_value_pairs': [self.cvp_to_dict(cvp) for cvp in column_value_pairs],
            'temporal_filters': [self.temporal_to_dict(tf) for tf in temporal_filters],
            'sql_query': sql_query,
            'complexity_level': query_structure.get_complexity_level(),
            'confidence': self.calculate_overall_confidence(query_structure),
            'interpretation': self.generate_natural_interpretation(query_structure),
            'processing_method': 'unified_hybrid',
            'unknown_words_detected': len(unknown_words),
            'unknown_words_details': [asdict(word) for word in unknown_words] if unknown_words else []
        }
        
        return result    
            
        
        
        
    def _calculate_preliminary_complexity(self, classified_components: Dict, temporal_filters: List, column_value_pairs: List) -> str:
        """Calcular complejidad preliminar para tomar decisiones tempranas"""
        score = 0
        score += len([c for c in classified_components.values() if c.type.value == 'operation'])
        score += len([c for c in classified_components.values() if c.type.value == 'metric'])
        score += len(temporal_filters) * 2
        score += len(column_value_pairs) * 2
            
        if score <= 2:
            return "simple"
        elif score <= 4:
            return "moderada"
        elif score <= 6:
            return "compleja"
        else:
            return "muy_compleja"
        

# ------  "Generador de sugerencias de error" -------
    
    def generate_error_suggestions(self, query: str) -> List[str]:
        """Generador de Sugerencias de Error"""
        return [
            "Intenta con: 'partner code con mas ventas'",
            "Ejemplo: 'product group con mayor sell out'",
            "Estructura: [entidad] con [operación] [métrica]",
            "Frases compuestas: partner_code, customer_id, sales_amount"
        ]
    
    
    
        # ===================================================
        # GRUPO 2: PROCESAMIENTO DE TEXTO 
        # Normalización, tokenización y detección de patrones
        # ===================================================
    

# ------  "Limpiador y normalizador de texto" -------
            
    def normalize_query_with_compounds(self, query: str) -> str:
        """🔧 NORMALIZADOR - REGLA ABSOLUTA PARA MAYÚSCULAS"""
        
        print(f"🔍 DEBUG 0: Query después de frases compuestas: '{query}'")
        
        words = query.split()
        corrected_words = []
        
        for word in words:
            
# REGLA ABSOLUTA: NUNCA tocar letras mayúsculas individuales
            if len(word) == 1 and word.isupper() and word.isalpha():
                corrected_words.append(word)  # PRESERVAR EXACTAMENTE
                print(f"🔒 PRESERVANDO DATO ABSOLUTO: '{word}' (letra mayúscula)")
            else:
                # Solo aplicar correcciones a palabras que NO sean datos
                corrected_word = self.dictionaries.correct_typo(word)
                corrected_words.append(corrected_word)
                if corrected_word != word:
                    print(f"🔧 Corrección: '{word}' → '{corrected_word}'")
        
        query = ' '.join(corrected_words)
        
# PASO 2: Limpiar caracteres especiales pero preservar espacios y guiones bajos
        query = re.sub(r'[^\w\s_]', '', query)
        
# PASO 3: Normalizar espacios múltiples
        query = re.sub(r'\s+', ' ', query).strip()
        
        print(f"🔍 DEBUG FINAL: Query normalizada: '{query}'")
        
        return query


    def _detect_compound_phrases_layer1_dictionary(self, query: str) -> str:
        """
        🥇 CAPA 1: DETECCIÓN BASADA EN DICCIONARIOS EXISTENTES
        Usa synonym_groups y diccionarios conocidos - MÁS RÁPIDA
        """
        print(f"🔍 CAPA 1: Detección por diccionarios")
        
        text_lower = query.lower()
        changes_made = []
        
        # Usar synonym_groups existente (tu lógica actual mejorada)
        sorted_phrases = sorted(self.synonym_groups.keys(), key=len, reverse=True)
        
        for phrase in sorted_phrases:
            if phrase in text_lower:
                normalized = self.synonym_groups[phrase]
                text_lower = text_lower.replace(phrase, normalized)
                changes_made.append(f"'{phrase}' → '{normalized}'")
        
        # También buscar directamente en dimensiones y métricas con espacios
        all_known_phrases = set()
        
        # Agregar dimensiones que tienen espacios o guiones
        for dim in self.dimensiones:
            if '_' in dim:
                space_version = dim.replace('_', ' ')
                all_known_phrases.add((space_version, dim))
        
        # Agregar métricas que tienen espacios o guiones  
        for metric in self.metricas:
            if '_' in metric:
                space_version = metric.replace('_', ' ')
                all_known_phrases.add((space_version, metric))
        
        # Aplicar reemplazos de frases conocidas
        for space_phrase, underscore_phrase in sorted(all_known_phrases, key=lambda x: len(x[0]), reverse=True):
            if space_phrase in text_lower:
                text_lower = text_lower.replace(space_phrase, underscore_phrase)
                changes_made.append(f"'{space_phrase}' → '{underscore_phrase}'")
        
        if changes_made:
            print(f"   ✅ CAPA 1 detectó: {changes_made}")
        
        return text_lower


# ------  "Detector de expresiones temporales" -------

    def detect_temporal_patterns_advanced(self, tokens: List[str]) -> List[TemporalFilter]:
        """
        🔧 Detector de Expresiones Temporales - VERSIÓN CORREGIDA CON ORDEN CORRECTO
        PRIORIDAD: Patrones largos PRIMERO, patrones cortos DESPUÉS
        """
        
        print(f"🔍 DETECTANDO PATRONES TEMPORALES AVANZADOS:")
        print(f"   🔤 Tokens: {tokens}")
        
        temporal_filters = []
        advanced_temporal_info = []
        i = 0
        
        while i < len(tokens):
            
# 🆕 PATRÓN MÁS ESPECÍFICO 1: "desde [UNIDAD] [NÚMERO] a [NÚMERO]" - desde semana 8 a 12
            if (i < len(tokens) - 4 and
                tokens[i].lower() == 'desde' and
                tokens[i + 1].lower() in self.dictionaries.unidades_tiempo and
                (tokens[i + 2].isdigit() or tokens[i + 2] in self.dictionaries.numeros_palabras) and
                tokens[i + 3].lower() == 'a' and
                (tokens[i + 4].isdigit() or tokens[i + 4] in self.dictionaries.numeros_palabras)):
                
                unit = self.dictionaries.unidades_tiempo[tokens[i + 1].lower()]
                
                if tokens[i + 2].isdigit():
                    start_value = int(tokens[i + 2])
                else:
                    start_value = self.dictionaries.numeros_palabras[tokens[i + 2]]
                    
                if tokens[i + 4].isdigit():
                    end_value = int(tokens[i + 4])
                else:
                    end_value = self.dictionaries.numeros_palabras[tokens[i + 4]]
                
                # Crear TemporalFilter básico
                basic_filter = TemporalFilter(
                    indicator="desde_a",
                    quantity=abs(end_value - start_value) + 1,
                    unit=unit,
                    confidence=0.95,
                    filter_type="range_between"
                )
                
                # Crear información avanzada
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    is_range_between=True,
                    start_value=start_value,
                    end_value=end_value,
                    raw_tokens=tokens[i:i+5]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ✅ PATRÓN 'DESDE_A': desde {tokens[i + 1]} {start_value} a {end_value}")
                i += 5  # Avanzar 5 tokens
                continue
            
# PATRÓN EXISTENTE 1: "entre [UNIDAD] [NÚMERO] y [NÚMERO]" - entre semana 5 y 9
            if (i < len(tokens) - 4 and
                tokens[i].lower() == 'entre' and
                tokens[i + 1].lower() in self.dictionaries.unidades_tiempo and
                (tokens[i + 2].isdigit() or tokens[i + 2] in self.dictionaries.numeros_palabras) and
                tokens[i + 3].lower() == 'y' and
                (tokens[i + 4].isdigit() or tokens[i + 4] in self.dictionaries.numeros_palabras)):
                
                unit = self.dictionaries.unidades_tiempo[tokens[i + 1].lower()]
                
                if tokens[i + 2].isdigit():
                    start_value = int(tokens[i + 2])
                else:
                    start_value = self.dictionaries.numeros_palabras[tokens[i + 2]]
                    
                if tokens[i + 4].isdigit():
                    end_value = int(tokens[i + 4])
                else:
                    end_value = self.dictionaries.numeros_palabras[tokens[i + 4]]
                
                # Crear TemporalFilter básico
                basic_filter = TemporalFilter(
                    indicator="entre_y",
                    quantity=abs(end_value - start_value) + 1,
                    unit=unit,
                    confidence=0.95,
                    filter_type="range_between"
                )
                
                # Crear información avanzada
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    is_range_between=True,
                    start_value=start_value,
                    end_value=end_value,
                    raw_tokens=tokens[i:i+5]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ✅ PATRÓN 'ENTRE_Y': entre {tokens[i + 1]} {start_value} y {end_value}")
                i += 5
                continue
            
            # 🔧 PATRÓN EXISTENTE 2: "de [UNIDAD] [NÚMERO] a [NÚMERO]" - de semana 8 a 4  
            if (i < len(tokens) - 4 and
                tokens[i].lower() == 'de' and
                tokens[i + 1].lower() in self.dictionaries.unidades_tiempo and
                (tokens[i + 2].isdigit() or tokens[i + 2] in self.dictionaries.numeros_palabras) and
                tokens[i + 3].lower() == 'a' and
                (tokens[i + 4].isdigit() or tokens[i + 4] in self.dictionaries.numeros_palabras)):
                
                unit = self.dictionaries.unidades_tiempo[tokens[i + 1].lower()]
                
                if tokens[i + 2].isdigit():
                    start_value = int(tokens[i + 2])
                else:
                    start_value = self.dictionaries.numeros_palabras[tokens[i + 2]]
                    
                if tokens[i + 4].isdigit():
                    end_value = int(tokens[i + 4])
                else:
                    end_value = self.dictionaries.numeros_palabras[tokens[i + 4]]
                
                # Crear TemporalFilter básico
                basic_filter = TemporalFilter(
                    indicator="de_a",
                    quantity=abs(end_value - start_value) + 1,
                    unit=unit,
                    confidence=0.95,
                    filter_type="range_between"
                )
                
                # Crear información avanzada
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    is_range_between=True,
                    start_value=start_value,
                    end_value=end_value,
                    raw_tokens=tokens[i:i+5]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ✅ PATRÓN 'DE_A': de {tokens[i + 1]} {start_value} a {end_value}")
                i += 5
                continue
            
#  PATRÓN MODIFICADO: "desde [UNIDAD] [NÚMERO]" - desde semana 8 (SOLO si no es "desde...a")
            if (i < len(tokens) - 2 and
                tokens[i].lower() == 'desde' and
                tokens[i + 1].lower() in self.dictionaries.unidades_tiempo and
                (tokens[i + 2].isdigit() or tokens[i + 2] in self.dictionaries.numeros_palabras)):
                
                # 🚨 VERIFICACIÓN CRÍTICA: ¿Es realmente "desde X" o es "desde X a Y"?
                is_desde_a_pattern = False
                if i + 4 < len(tokens):
                    next_token = tokens[i + 3].lower()
                    fourth_token_is_number = (tokens[i + 4].isdigit() or tokens[i + 4] in self.dictionaries.numeros_palabras)
                    if next_token == 'a' and fourth_token_is_number:
                        is_desde_a_pattern = True
                        print(f"   🔍 Detectado patrón 'desde...a' - saltando procesamiento como 'desde' simple")
                
                # Solo procesar como "desde" simple si NO es "desde...a"
                if not is_desde_a_pattern:
                    unit = self.dictionaries.unidades_tiempo[tokens[i + 1].lower()]
                    
                    if tokens[i + 2].isdigit():
                        start_value = int(tokens[i + 2])
                    else:
                        start_value = self.dictionaries.numeros_palabras[tokens[i + 2]]
                    
                    # Crear TemporalFilter básico
                    basic_filter = TemporalFilter(
                        indicator="desde",
                        quantity=start_value,
                        unit=unit,
                        confidence=0.95,
                        filter_type="range_from"
                    )
                    
                    # Crear información avanzada complementaria
                    advanced_info = AdvancedTemporalInfo(
                        original_filter=basic_filter,
                        is_range_from=True,
                        start_value=start_value,
                        raw_tokens=tokens[i:i+3]
                    )
                    
                    temporal_filters.append(basic_filter)
                    advanced_temporal_info.append(advanced_info)
                    
                    print(f"   ✅ PATRÓN 'DESDE' (simple): desde {tokens[i + 1]} {start_value}")
                    i += 3
                    continue
                
            # 🔧 PATRÓN EXISTENTE: "hasta [UNIDAD] [NÚMERO]" - hasta semana 5
            if (i < len(tokens) - 2 and
                tokens[i].lower() == 'hasta' and
                tokens[i + 1].lower() in self.dictionaries.unidades_tiempo and
                (tokens[i + 2].isdigit() or tokens[i + 2] in self.dictionaries.numeros_palabras)):
                
                unit = self.dictionaries.unidades_tiempo[tokens[i + 1].lower()]
                
                if tokens[i + 2].isdigit():
                    end_value = int(tokens[i + 2])
                else:
                    end_value = self.dictionaries.numeros_palabras[tokens[i + 2]]
                
                # Crear TemporalFilter básico
                basic_filter = TemporalFilter(
                    indicator="hasta",
                    quantity=end_value,
                    unit=unit,
                    confidence=0.95,
                    filter_type="range_to"
                )
                
                # Crear información avanzada
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    is_range_to=True,
                    end_value=end_value,
                    raw_tokens=tokens[i:i+3]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ✅ PATRÓN 'HASTA': hasta {tokens[i + 1]} {end_value}")
                i += 3
                continue
            
            # 🔧 PATRÓN EXISTENTE: [INDICADOR] [NÚMERO] [UNIDAD] - "ultimas 8 semanas"
            if (i < len(tokens) - 2 and
                tokens[i] in self.dictionaries.indicadores_temporales and
                (tokens[i + 1].isdigit() or tokens[i + 1] in self.dictionaries.numeros_palabras) and
                tokens[i + 2] in self.dictionaries.unidades_tiempo):
                
                indicator = self.dictionaries.indicadores_temporales[tokens[i]]
                
                if tokens[i + 1].isdigit():
                    quantity = int(tokens[i + 1])
                else:
                    quantity = self.dictionaries.numeros_palabras[tokens[i + 1]]
                
                unit = self.dictionaries.unidades_tiempo[tokens[i + 2]]
                
                basic_filter = TemporalFilter(
                    indicator=indicator,
                    quantity=quantity,
                    unit=unit,
                    confidence=0.95,
                    filter_type="range"
                )
                
                # Información básica para mantener compatibilidad
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    raw_tokens=tokens[i:i+3]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ⏰ Filtro temporal (rango): {indicator} {quantity} {unit.value}")
                i += 3
                continue
            
            # 🔧 PATRÓN EXISTENTE: [UNIDAD] [NÚMERO] - "semana 8", "week 8"
            elif (i < len(tokens) - 1 and
                tokens[i] in self.dictionaries.unidades_tiempo and
                (tokens[i + 1].isdigit() or tokens[i + 1] in self.dictionaries.numeros_palabras)):
                
                unit = self.dictionaries.unidades_tiempo[tokens[i]]
                
                if tokens[i + 1].isdigit():
                    quantity = int(tokens[i + 1])
                else:
                    quantity = self.dictionaries.numeros_palabras[tokens[i + 1]]
                
                basic_filter = TemporalFilter(
                    indicator="específica",
                    quantity=quantity,
                    unit=unit,
                    confidence=0.90,
                    filter_type="specific"
                )
                
                # Información básica para mantener compatibilidad
                advanced_info = AdvancedTemporalInfo(
                    original_filter=basic_filter,
                    raw_tokens=tokens[i:i+2]
                )
                
                temporal_filters.append(basic_filter)
                advanced_temporal_info.append(advanced_info)
                
                print(f"   ⏰ Filtro temporal (específico): {tokens[i]} {quantity}")
                i += 2
                continue
            
            i += 1
        
        # GUARDAR información avanzada para uso posterior
        self.advanced_temporal_info = advanced_temporal_info
        
        print(f"🔍 TOTAL FILTROS TEMPORALES DETECTADOS: {len(temporal_filters)}")
        for i, tf in enumerate(temporal_filters, 1):
            print(f"   {i}. Tipo: {tf.filter_type}, Unidad: {tf.unit.value}")
        
        return temporal_filters


# ------  "Detector de pares Columna valor" -------

    def detect_column_value_patterns(self, tokens: List[str], temporal_filters: List[TemporalFilter]) -> List[ColumnValuePair]:
        """Detector de Pares Columna-Valor - VERSIÓN GENÉRICA AMPLIADA"""
        print(f"🎯 DEBUG 3: Tokens recibidos: {tokens}")
        
        column_value_pairs = []
        
        # Identificar TODAS las columnas temporales (mantener lógica existente)
        temporal_columns = set()
        for tf in temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['semana', 'semanas', 'week', 'weeks'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['mes', 'meses', 'month', 'months'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['dia', 'dias', 'day', 'days'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['año', 'años', 'year', 'years'])
        
        print(f"⏰ Columnas temporales a excluir: {temporal_columns}")
        
        i = 0
        while i < len(tokens) - 1:
            
            # 🆕 PATRÓN 1: [preposición] [columna] [valor] (ej: "de sku QN55S90DAFXZX")
            if i < len(tokens) - 2:
                pattern_result = self._detect_preposition_column_value_pattern(tokens, i, temporal_columns)
                if pattern_result:
                    column_value_pairs.append(pattern_result['pair'])
                    print(f"✅ DEBUG 5: FILTRO CREADO (preposición): {pattern_result['raw_text']}")
                    i += pattern_result['tokens_consumed']
                    continue
            
            # PATRÓN ORIGINAL: [columna] [valor] (mantener lógica existente)
            current_token = tokens[i]
            next_token = tokens[i + 1]
            
            print(f"🔍 DEBUG 4: Analizando '{current_token}' + '{next_token}'")
            
            column_info = self._identify_potential_column(current_token)
            
            print(f"     Columna? {column_info}")
            
            if column_info['is_column']:
                if column_info['normalized_name'] in temporal_columns:
                    print(f"⏰ Saltando '{current_token}' - ya procesado como temporal")
                    i += 1
                    continue
                
                value_info = self._identify_potential_value(next_token, i + 1, tokens)
                
                print(f"     Valor? {value_info}")
                
                if value_info['is_value']:
                    column_value_pairs.append(ColumnValuePair(
                        column_name=column_info['normalized_name'],
                        value=value_info['normalized_value'], 
                        confidence=min(column_info['confidence'], value_info['confidence']),
                        raw_text=f"{current_token} {next_token}"
                    ))
                    
                    print(f"✅ DEBUG 5: FILTRO CREADO: {current_token} = '{next_token}'")
                    
                    i += 2
                    continue
            
            i += 1
        
        print(f"🎯 DEBUG 6: Total filtros detectados: {len(column_value_pairs)}")
        
        return column_value_pairs


    # 🆕 MÉTODO AUXILIAR GENÉRICO: Detectar patrones con preposiciones
    def _detect_preposition_column_value_pattern(self, tokens: List[str], start_idx: int, temporal_columns: set) -> Optional[Dict]:
        """
        Detecta patrones genéricos: [preposición] [columna] [valor]
        
        Args:
            tokens: Lista completa de tokens
            start_idx: Índice donde empezar a buscar
            temporal_columns: Columnas temporales a excluir
        
        Returns:
            Dict con 'pair', 'tokens_consumed', 'raw_text' o None
        """
        
        if start_idx + 2 >= len(tokens):
            return None
        
        preposition_token = tokens[start_idx]
        column_token = tokens[start_idx + 1] 
        value_token = tokens[start_idx + 2]
        
        # 🔧 PREPOSICIONES GENÉRICAS (usando conectores del diccionario + específicas)
        common_prepositions = {'de', 'en', 'para', 'con', 'desde', 'por'}
        # Agregar conectores del diccionario que puedan ser preposiciones
        all_prepositions = common_prepositions.union(
            {conn for conn in self.dictionaries.conectores if conn in common_prepositions}
        )
        
        if preposition_token.lower() not in all_prepositions:
            return None
        
        print(f"🔍 DEBUG 4.1: Analizando patrón preposición: '{preposition_token}' + '{column_token}' + '{value_token}'")
        
        # Verificar si es columna válida
        column_info = self._identify_potential_column(column_token)
        print(f"     Columna? {column_info}")
        
        if not column_info['is_column']:
            return None
        
        # Excluir columnas temporales
        if column_info['normalized_name'] in temporal_columns:
            print(f"⏰ Saltando '{column_token}' - ya procesado como temporal")
            return None
        
        # Verificar si es valor válido
        value_info = self._identify_potential_value(value_token, start_idx + 2, tokens)
        print(f"     Valor? {value_info}")
        
        if not value_info['is_value']:
            return None
        
        # 🆕 AJUSTE DE CONFIANZA: Reducir ligeramente por ser patrón indirecto
        confidence_adjustment = 0.95  # 5% de reducción por indirección
        final_confidence = min(column_info['confidence'], value_info['confidence']) * confidence_adjustment
        
        # Crear par columna-valor
        pair = ColumnValuePair(
            column_name=column_info['normalized_name'],
            value=value_info['normalized_value'],
            confidence=final_confidence,
            raw_text=f"{preposition_token} {column_token} {value_token}"
        )
        
        return {
            'pair': pair,
            'tokens_consumed': 3,  # preposición + columna + valor
            'raw_text': f"{preposition_token} {column_token} = '{value_token}'"
        }


# ------  "Identificador de columnas potenciales" -------

    def _identify_potential_column(self, token: str) -> Dict:
        """Identificador de Columnas Potenciales"""
        token_lower = token.lower()
        
        if token_lower in self.dictionaries.dimensiones:
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'dimension',
                'confidence': 0.95
            }
        
        if token_lower in self.dictionaries.metricas:
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'metric',
                'confidence': 0.90
            }
        
        if token_lower in self.dictionaries.frases_compuestas:
            normalized = self.dictionaries.frases_compuestas[token_lower]
            return {
                'is_column': True,
                'normalized_name': normalized,
                'type': 'compound',
                'confidence': 0.95
            }
        
        if self._looks_like_column_name(token):
            return {
                'is_column': True,
                'normalized_name': token_lower,
                'type': 'inferred',
                'confidence': 0.70
            }
        
        return {
            'is_column': False,
            'normalized_name': None,
            'type': None,
            'confidence': 0.0
        }


# ------  "Identificador de valores especificos" -------

    def _identify_potential_value(self, token: str, position: int, tokens: List[str]) -> Dict:
        """Identificador de Valores Específicos - VERSIÓN GENÉRICA MEJORADA"""
        
        # PRIORIDAD MÁXIMA: Letras individuales mayúsculas (mantener lógica existente)
        if len(token) == 1 and token.isupper() and token.isalpha():
            return {
                'is_value': True,
                'normalized_value': token,
                'confidence': 0.98
            }
        
        token_lower = token.lower()
        token_upper = token.upper()
        
        # DESCARTAR: Palabras del lenguaje natural usando diccionarios existentes
        language_words = self.dictionaries.conectores.union({
            'entre', 'desde', 'hasta', 'con'  # Solo conectores temporales/contextuales
        })
        
        if token_lower in language_words and token != 'Y':
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        # DESCARTAR: Usar diccionarios existentes para operaciones y métricas
        if token_lower in self.dictionaries.operaciones:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        if token_lower in self.dictionaries.metricas:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}
        
        if token_lower in self.dictionaries.dimensiones:
            return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}

        # REGLA GENÉRICA: Códigos alfanuméricos largos (sin patrones específicos)
        if self._is_generic_code_value(token):
            context_confidence = self._calculate_generic_context_confidence(token, position, tokens)
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': context_confidence
            }

        # REGLAS EXISTENTES (mantener intactas)
        if len(token) == 1 and token.isalpha():
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': 0.90
            }
        
        if token.isdigit():
            return {
                'is_value': True,
                'normalized_value': token,
                'confidence': 0.95
            }
        
        # 🔧 REGLA EXPANDIDA: Códigos alfanuméricos cortos/medianos
        if re.match(r'^[A-Za-z0-9\-/\.]+$', token) and 2 <= len(token) <= 30:
            context_confidence = self._calculate_generic_context_confidence(token, position, tokens)
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': context_confidence
            }
        
        # REGLAS EXISTENTES para estados comunes (mantener)
        common_states = {
            'activo', 'inactivo', 'pendiente', 'completado', 'cancelado',
            'si', 'no', 'yes', 'true', 'false', 'on', 'off',
            'alto', 'medio', 'bajo', 'premium', 'basico', 'vip'
        }
        if token_lower in common_states:
            return {
                'is_value': True,
                'normalized_value': token_upper,
                'confidence': 0.85
            }
        
        return {'is_value': False, 'normalized_value': None, 'confidence': 0.0}



    # MÉTODO AUXILIAR GENÉRICO: Detectar códigos sin patrones específicos
    def _is_generic_code_value(self, token: str) -> bool:
        """Detecta si un token parece un código/valor genérico usando reglas universales"""
        
        # REGLA 1: Debe ser alfanumérico (puede incluir guiones, puntos, barras)
        if not re.match(r'^[A-Za-z0-9\-/\.]+$', token):
            return False
        
        # REGLA 2: Longitud mínima para ser considerado código
        if len(token) < 3:
            return False
        
        # REGLA 3: Debe tener al menos una letra Y un número (característica de códigos)
        has_letter = any(c.isalpha() for c in token)
        has_number = any(c.isdigit() for c in token)
        
        if has_letter and has_number:
            return True
        
        # REGLA 4: Solo letras pero de longitud significativa (ej: códigos de país, estados)
        if has_letter and not has_number and len(token) >= 2:
            return True
        
        # REGLA 5: Solo números pero de longitud significativa (ej: códigos numéricos)
        if has_number and not has_letter and len(token) >= 4:
            return True
        
        return False


    # MÉTODO AUXILIAR GENÉRICO: Confianza basada en contexto usando diccionarios
    def _calculate_generic_context_confidence(self, token: str, position: int, tokens: List[str]) -> float:
        """Calcula confianza usando el contexto y los diccionarios existentes"""
        
        base_confidence = 0.70  # Confianza base para códigos genéricos
        
        # CONTEXTO +: Token anterior es una dimensión conocida (del diccionario)
        if position > 0:
            prev_token = tokens[position - 1].lower()
            if prev_token in self.dictionaries.dimensiones:
                base_confidence += 0.20  # Gran boost si está después de dimensión
                print(f"      🎯 Contexto dimensión: '{prev_token}' → +0.20 confianza")
        
        # CONTEXTO +: Patrón "de [DIMENSIÓN] [VALOR]"
        if position >= 2:
            two_before = tokens[position - 2].lower()
            one_before = tokens[position - 1].lower()
            if two_before == 'de' and one_before in self.dictionaries.dimensiones:
                base_confidence += 0.15
                print(f"      🎯 Patrón 'de dimensión valor': +0.15 confianza")
        
        # CONTEXTO +: Características del token
        # Más confianza para códigos con buena mezcla alfanumérica
        has_letter = any(c.isalpha() for c in token)
        has_number = any(c.isdigit() for c in token)
        
        if has_letter and has_number:
            if 5 <= len(token) <= 15:  # Longitud típica de códigos
                base_confidence += 0.10
            elif 3 <= len(token) <= 20:  # Rango más amplio
                base_confidence += 0.05
        
        # CONTEXTO -: Penalizar si es demasiado largo (podría ser texto)
        if len(token) > 25:
            base_confidence -= 0.15
        
        # CONTEXTO +: Si contiene patrones típicos de códigos (sin ser específicos)
        if any(char in token for char in ['-', '/', '.']):
            base_confidence += 0.05  # Separadores típicos de códigos
        
        return min(0.95, max(0.40, base_confidence))  # Entre 0.40 y 0.95


# ------  "Verificador de nombres de columna" -------

    def _looks_like_column_name(self, token: str) -> bool:
        """Verificador de Nombres de Columna"""
        if '_' in token:
            return True
        
        column_suffixes = ['_id', '_code', '_number', '_key', '_ref', '_name', '_type', '_status']
        if any(token.lower().endswith(suffix) for suffix in column_suffixes):
            return True
        
        column_prefixes = ['id_', 'code_', 'num_', 'ref_']
        if any(token.lower().startswith(prefix) for prefix in column_prefixes):
            return True
        
        return False



        # =================================================
        # GRUPO 3: ANÁLISIS SEMÁNTICO 
        # Clasificación de componentes y análisis semántico
        # =================================================



# ------  "Clasificador principal de tokens" -------

    def classify_all_components(self, tokens: List[str], column_value_pairs: List[ColumnValuePair]) -> Dict[str, QueryComponent]:
        """Clasificador Principal de Tokens"""
        classified = {}
        processed_tokens = set()
        
        # Marcar tokens procesados en pares columna-valor
        for cvp in column_value_pairs:
            pair_tokens = cvp.raw_text.split()
            processed_tokens.update(pair_tokens)
            print(f"🔗 Filtro detectado: {cvp.column_name} = '{cvp.value}' (tokens: {pair_tokens})")
        
        # Clasificar tokens individuales
        for token in tokens:
            classified[token] = self.classify_single_component(token)
            
            if token in processed_tokens:
                classified[token].linguistic_info['used_in_filter'] = True
                print(f"🎯 Token '{token}' clasificado como {classified[token].type.value} (usado en filtro)")
            else:
                print(f"🔍 Token '{token}' clasificado como {classified[token].type.value}")
        
        return classified


# ------  "Clasificador individual de tokens" -------

    def classify_single_component(self, token: str) -> QueryComponent:
        """Clasificador Individual de Tokens - VERSIÓN MEJORADA"""
        
        # NUEVO: VERIFICACIÓN ESPECIAL PARA INDICADORES DE RANKING
        ranking_indicators = {
            'top', 'mejores', 'mejore', 'mejor', 'primeros', 'primero', 
            'highest', 'best', 'máximos', 'máximo', 'worst', 'peores', 
            'peor', 'últimos', 'último', 'bottom', 'lowest', 'mínimos', 'mínimo'
        }
        
        if token.lower() in ranking_indicators:
            return QueryComponent(
                text=token,
                type=ComponentType.OPERATION,  # Cambiar de UNKNOWN a OPERATION
                confidence=0.90,
                subtype='ranking_indicator',
                value=token.lower(),
                linguistic_info={'source': 'ranking_indicator'}
            )
        
        # VERIFICACIÓN TEMPRANA: Letras individuales mayúsculas
        if len(token) == 1 and token.isupper() and token.isalpha():
            return QueryComponent(
                text=token,
                type=ComponentType.VALUE,
                confidence=0.98,
                subtype='letter',
                value=token,
                linguistic_info={'source': 'uppercase_letter_value'}
            )
        
        corrected_token = self.dictionaries.correct_typo(token)
        if corrected_token != token:
            corrected_component = self.classify_single_component(corrected_token)
            if corrected_component.type != ComponentType.UNKNOWN:
                corrected_component.linguistic_info = {
                    'source': 'typo_correction',
                    'original': token,
                    'corrected': corrected_token
                }
                corrected_component.confidence *= 0.85
                return corrected_component
        
        component_type = self.dictionaries.get_component_type(token)
        
        if component_type == ComponentType.DIMENSION:
            return QueryComponent(
                text=token,
                type=ComponentType.DIMENSION,
                confidence=0.95,
                linguistic_info={'source': 'dimension_dictionary'}
            )
        elif component_type == ComponentType.OPERATION:
            operation_type = self.dictionaries.get_operation_type(token)
            return QueryComponent(
                text=token,
                type=ComponentType.OPERATION,
                confidence=0.95,
                value=operation_type.value if operation_type else None,
                linguistic_info={'source': 'operation_dictionary'}
            )
        elif component_type == ComponentType.METRIC:
            return QueryComponent(
                text=token,
                type=ComponentType.METRIC,
                confidence=0.95,
                linguistic_info={'source': 'metric_dictionary'}
            )
        elif component_type == ComponentType.TEMPORAL:
            if token in self.dictionaries.indicadores_temporales:
                return QueryComponent(
                    text=token,
                    type=ComponentType.TEMPORAL,
                    confidence=0.9,
                    subtype='indicator',
                    value=self.dictionaries.indicadores_temporales[token],
                    linguistic_info={'source': 'temporal_dictionary'}
                )
            elif token in self.dictionaries.unidades_tiempo:
                return QueryComponent(
                    text=token,
                    type=ComponentType.TEMPORAL,
                    confidence=0.95,
                    subtype='unit',
                    value=self.dictionaries.unidades_tiempo[token],
                    linguistic_info={'source': 'temporal_dictionary'}
                )
        elif component_type == ComponentType.VALUE:
            if token.isdigit():
                return QueryComponent(
                    text=token,
                    type=ComponentType.VALUE,
                    confidence=0.95,
                    subtype='number',
                    value=int(token),
                    linguistic_info={'source': 'numeric_literal'}
                )
            elif token in self.dictionaries.numeros_palabras:
                return QueryComponent(
                    text=token,
                    type=ComponentType.VALUE,
                    confidence=0.9,
                    subtype='number',
                    value=self.dictionaries.numeros_palabras[token],
                    linguistic_info={'source': 'number_word'}
                )
        elif component_type == ComponentType.CONNECTOR:
            return QueryComponent(
                text=token,
                type=ComponentType.CONNECTOR,
                confidence=0.8,
                linguistic_info={'source': 'connector_dictionary'}
            )

    # Buscar en diccionario temporal antes de marcar como UNKNOWN
        temporal_entry = self.dictionaries.search_in_temporal_dictionary(token)
                    
        if temporal_entry:
            temporal_type = self.dictionaries.get_temporal_component_type(token)
            
            # 🔧 VERIFICACIÓN: Confirmar que es VALUE
            print(f"   🗄️ TEMPORAL CLASIFICADO: '{token}' → {temporal_type.value}")
            
            return QueryComponent(
                text=token,
                type=temporal_type or ComponentType.VALUE,  # Fallback a VALUE
                confidence=temporal_entry.get('confidence', 0.9),
                subtype='temporal_data',
                value=temporal_entry.get('original_value'),
                column_name=temporal_entry.get('column_name'),
                linguistic_info={
                    'source': 'temporal_dictionary',
                    'original_value': temporal_entry.get('original_value'),
                    'column_name': temporal_entry.get('column_name'),
                    'column_type': temporal_entry.get('column_type'),
                    'forced_as_value': True  # 🆕 Marcador para debugging
                }
            )
        
        # Si no está en temporal tampoco, entonces es UNKNOWN
        return QueryComponent(
            text=token,
            type=ComponentType.UNKNOWN,
            confidence=0.3,
            linguistic_info={'source': 'unknown'}
        )


# ------  "Detector de tipo de consulta" -------

    def detect_query_pattern(self, structure: QueryStructure) -> QueryPattern:
        """Detector de Tipo de Consulta - VERSIÓN CORREGIDA PARA RANKINGS MULTI-DIMENSIONALES"""
        print(f"🔍 DETECTANDO PATRÓN DE CONSULTA:")
        print(f"   📍 Dimensión: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
        print(f"   🔗 Múltiples dimensiones: {len(structure.main_dimensions) if structure.main_dimensions else 0}")
        print(f"   ⚡ Operaciones: {[op.text for op in structure.operations]}")
        print(f"   📊 Métricas: {[m.text for m in structure.metrics]}")
        print(f"   🎛️ Filtros: {len(structure.column_conditions)}")
        print(f"   ⏰ Filtros temporales: {len(structure.temporal_filters)}")
        print(f"   🔗 Es compuesta: {structure.is_compound_query}")
        print(f"   🔗 Criterios compuestos: {len(structure.compound_criteria)}")
        print(f"   🏆 Es ranking: {structure.is_ranking_query}")
        print(f"   📐 Es multi-dimensional: {structure.is_multi_dimension_query}")
        
        # 🔧 PATRÓN PRIORITARIO CORREGIDO: RANKING (incluyendo multi-dimensionales)
        if structure.is_ranking_query and structure.ranking_criteria:
            confidence = self.calculate_ranking_confidence(structure)
            if confidence >= 0.7:
                print(f"   🏆 PATRÓN DETECTADO: TOP_N (ranking con {len(structure.main_dimensions) if structure.main_dimensions else 1} dimensiones, confianza: {confidence:.2f})")
                structure.confidence_score = confidence
                return QueryPattern.TOP_N
        
        # PATRÓN 2: MÚLTIPLES DIMENSIONES SIN RANKING
        if (structure.is_multi_dimension_query and 
            len(structure.main_dimensions) >= 2 and 
            not structure.is_ranking_query):
            confidence = self.calculate_multi_dimension_confidence(structure)
            if confidence >= 0.7:
                print(f"   🔗 PATRÓN DETECTADO: MULTI_DIMENSION ({len(structure.main_dimensions)} dimensiones sin ranking, confianza: {confidence:.2f})")
                structure.confidence_score = confidence
                return QueryPattern.MULTI_DIMENSION
        
        # PATRÓN 3: CONSULTAS COMPUESTAS REFERENCIADAS
        if (structure.is_compound_query and 
            structure.main_dimension and 
            len(structure.compound_criteria) >= 2):
            
            all_reference_operations = True
            reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
            
            for criteria in structure.compound_criteria:
                if criteria.operation.value not in reference_operations:
                    all_reference_operations = False
                    break
            
            if all_reference_operations:
                confidence = self.calculate_compound_reference_confidence(structure)
                if confidence >= 0.7:
                    print(f"   🎯 PATRÓN DETECTADO: REFERENCED (compuesta, confianza: {confidence:.2f})")
                    structure.confidence_score = confidence
                    return QueryPattern.REFERENCED
            
        # PATRÓN 4: DATOS REFERENCIADOS SIMPLES
        if (structure.main_dimension and 
            len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1 and 
            len(structure.column_conditions) == 0 and
            not structure.is_ranking_query):
            
            operation = structure.operations[0]
            reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
            
            if operation.value in reference_operations:
                confidence = self.calculate_reference_confidence(structure)
                if confidence >= 0.7:
                    print(f"   🎯 PATRÓN DETECTADO: REFERENCED (simple, confianza: {confidence:.2f})")
                    structure.confidence_score = confidence
                    return QueryPattern.REFERENCED
        
        # PATRÓN 5: AGREGACIÓN COMPLETA
        if (len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1 and 
            not structure.main_dimension):
            
            print(f"   📊 PATRÓN DETECTADO: AGGREGATION (agregación global)")
            structure.confidence_score = 0.90
            return QueryPattern.AGGREGATION
        
        # PATRÓN 6: AGREGACIÓN CON DIMENSIÓN
        if (structure.main_dimension and 
            len(structure.operations) >= 1 and 
            len(structure.metrics) >= 1):
            
            print(f"   📊 PATRÓN DETECTADO: AGGREGATION (con agrupación)")
            structure.confidence_score = 0.85
            return QueryPattern.AGGREGATION
        
        # PATRÓN 7: LISTAR TODOS
        if (structure.main_dimension and 
            len(structure.operations) == 0):
            
            print(f"   📋 PATRÓN DETECTADO: LIST_ALL")
            structure.confidence_score = 0.80
            return QueryPattern.LIST_ALL
        
        # PATRÓN 8: FILTRADO CON AGREGACIÓN
        if len(structure.column_conditions) >= 1:
            print(f"   🎛️ PATRÓN DETECTADO: AGGREGATION (con filtros)")
            structure.confidence_score = 0.75
            return QueryPattern.AGGREGATION
        
        print(f"   ❓ PATRÓN DETECTADO: UNKNOWN (no se pudo determinar)")
        structure.confidence_score = 0.4
        return QueryPattern.UNKNOWN


# ==================================================
# ------  "Detector de consultas compuestas" -------
# ==================================================

    def detect_compound_criteria(self, tokens: List[str], classified_components: Dict) -> List[CompoundCriteria]:
        """Detector de Consultas Compuestas"""
        print(f"🔗 DETECTANDO CRITERIOS COMPUESTOS:")
        print(f"   🔤 Tokens: {tokens}")
        
        compound_criteria = []
        
        segments = self.split_by_connector(tokens, 'y')
        
        print(f"   📊 Segmentos detectados: {segments}")
        
        for i, segment in enumerate(segments):
            print(f"\n   🎯 Procesando segmento {i+1}: {segment}")
            
            criteria = self.extract_criteria_from_segment(segment, classified_components)
            if criteria:
                compound_criteria.append(criteria)
                print(f"      ✅ Criterio extraído: {criteria.operation.text} {criteria.metric.text}")
            else:
                print(f"      ❌ No se pudo extraer criterio del segmento")
        
        print(f"\n🔗 TOTAL CRITERIOS DETECTADOS: {len(compound_criteria)}")
        for i, criteria in enumerate(compound_criteria):
            print(f"   {i+1}. {criteria.operation.text} {criteria.metric.text} (confianza: {criteria.confidence:.2f})")
        
        return compound_criteria



# ------  "Divisor por conectores" -------

    def split_by_connector(self, tokens: List[str], connector: str) -> List[List[str]]:
        """Divisor por Conectores (Y, O)"""
        segments = []
        current_segment = []
        
        for token in tokens:
            if token.lower() == connector.lower():
                if current_segment:
                    segments.append(current_segment)
                    current_segment = []
            else:
                current_segment.append(token)
        
        if current_segment:
            segments.append(current_segment)
        
        return segments


# ------  "Extractor de criterios de segmentos" -------

    def extract_criteria_from_segment(self, segment: List[str], classified_components: Dict) -> Optional[CompoundCriteria]:
        """
        🔧 Extractor de Criterios de Segmentos - VERSIÓN CORREGIDA
        Prioriza métricas reales antes de convertir dimensiones
        """
        operation_found = None
        metric_found = None
        dimension_candidate = None  # 🆕 Guardar dimensión como candidato
        confidence_sum = 0.0
        count = 0
        
        print(f"      🔍 Analizando segmento: {segment}")
        
        # 🆕 PRIMERA PASADA: Buscar operaciones y métricas REALES
        for token in segment:
            if token in classified_components:
                component = classified_components[token]
                
                # Buscar operación
                if component.type == ComponentType.OPERATION and not operation_found:
                    operation_found = component
                    confidence_sum += component.confidence
                    count += 1
                    print(f"         ⚡ Operación encontrada: {token}")
                
                # 🔧 PRIORIDAD: Métricas reales PRIMERO
                elif component.type == ComponentType.METRIC and not metric_found:
                    metric_found = component
                    confidence_sum += component.confidence
                    count += 1
                    print(f"         📊 Métrica REAL encontrada: {token}")
                
                # 🆕 GUARDAR dimensión como candidato (NO convertir aún)
                elif component.type == ComponentType.DIMENSION and not dimension_candidate:
                    dimension_candidate = component
                    print(f"         📍 Dimensión candidata: {token} (no convertida aún)")
        
        # 🆕 SEGUNDA PASADA: Solo si NO hay métrica real, usar dimensión
        if not metric_found and dimension_candidate:
            metric_component = QueryComponent(
                text=dimension_candidate.text,
                type=ComponentType.METRIC,
                confidence=dimension_candidate.confidence * 0.85,
                subtype='converted_from_dimension',
                value=dimension_candidate.value,
                column_name=dimension_candidate.column_name,
                linguistic_info={'converted_from': 'dimension'}
            )
            metric_found = metric_component
            confidence_sum += metric_component.confidence
            count += 1
            print(f"         🔄 Dimensión convertida a métrica (fallback): {dimension_candidate.text}")
        
        # 🔧 VALIDACIÓN FINAL
        if operation_found and metric_found:
            avg_confidence = confidence_sum / count if count > 0 else 0.0
            
            print(f"         ✅ Criterio completo: {operation_found.text} + {metric_found.text}")
            
            return CompoundCriteria(
                operation=operation_found,
                metric=metric_found,
                confidence=avg_confidence,
                raw_tokens=segment
            )
        
        # 🚨 DIAGNÓSTICO DE ERROR
        print(f"         ❌ Criterio incompleto:")
        print(f"             Operación: {operation_found.text if operation_found else 'NO ENCONTRADA'}")
        print(f"             Métrica: {metric_found.text if metric_found else 'NO ENCONTRADA'}")
        print(f"             Dimensión candidata: {dimension_candidate.text if dimension_candidate else 'NO ENCONTRADA'}")
        
        return None


# ------  "Verificador de consultas compuestas" -------

    def is_compound_query(self, compound_criteria: List[CompoundCriteria]) -> bool:
        """Verificador de Consulta Compuesta"""
        valid_criteria = [c for c in compound_criteria if c.confidence >= 0.6]
        
        is_compound = len(valid_criteria) >= 2
        
        print(f"🔗 EVALUANDO SI ES CONSULTA COMPUESTA:")
        print(f"   📊 Criterios válidos: {len(valid_criteria)}")
        print(f"   🎯 Es compuesta: {is_compound}")
        
        return is_compound


# ------  "Detector de criterios de ranking" -------

    # def detect_ranking_criteria(self, tokens: List[str], classified_components: Dict) -> Optional[RankingCriteria]:
    #     """Detector de Criterios de Ranking"""
    #     print(f"🏆 DETECTANDO CRITERIOS DE RANKING:")
    #     print(f"   🔤 Tokens: {tokens}")
        
    #     top_indicators = {
    #         'top', 'mejores', 'mejore', 'mejor', 'primeros', 'primero', 'highest', 'best', 'máximos', 'máximo'
    #     }
        
    #     bottom_indicators = {
    #         'worst', 'peores', 'peor', 'últimos', 'último', 'bottom', 'lowest', 'mínimos', 'mínimo'
    #     }
        
    #     ranking_direction = None
    #     ranking_start_idx = -1
        
    #     for i, token in enumerate(tokens):
    #         token_lower = token.lower()
            
    #         if token_lower in top_indicators:
    #             ranking_direction = RankingDirection.TOP
    #             ranking_start_idx = i
    #             print(f"   🔝 Indicador TOP encontrado: '{token}' en posición {i}")
    #             break
    #         elif token_lower in bottom_indicators:
    #             ranking_direction = RankingDirection.BOTTOM
    #             ranking_start_idx = i
    #             print(f"   📉 Indicador BOTTOM encontrado: '{token}' en posición {i}")
    #             break
        
    #     if not ranking_direction:
    #         print(f"   ❌ No se encontraron indicadores de ranking")
    #         return None
        
    #     ranking_value = None
    #     ranking_unit = None
    #     value_tokens = []
        
    #     search_end = min(ranking_start_idx + 4, len(tokens))
        
    #     for i in range(ranking_start_idx + 1, search_end):
    #         if i >= len(tokens):
    #             break
                
    #         token = tokens[i]
            
    #         if token.endswith('%'):
    #             try:
    #                 percent_value = float(token[:-1])
    #                 ranking_value = percent_value
    #                 ranking_unit = RankingUnit.PERCENTAGE
    #                 value_tokens.append(token)
    #                 print(f"   📊 Porcentaje detectado: {percent_value}%")
    #                 break
    #             except ValueError:
    #                 continue
            
    #         elif token.isdigit():
    #             ranking_value = int(token)
    #             ranking_unit = RankingUnit.COUNT
    #             value_tokens.append(token)
    #             print(f"   🔢 Número detectado: {ranking_value}")
    #             break
            
    #         elif token.lower() in self.dictionaries.numeros_palabras:
    #             ranking_value = self.dictionaries.numeros_palabras[token.lower()]
    #             ranking_unit = RankingUnit.COUNT
    #             value_tokens.append(token)
    #             print(f"   🔤 Número en palabras detectado: {token} = {ranking_value}")
    #             break
        
    #     if ranking_value is None:
    #         print(f"   ❌ No se encontró valor numérico después del indicador")
    #         return None
        
    #     ranking_metric = None
    #     ranking_operation = None
        
    #     for token, component in classified_components.items():
    #         if component.type == ComponentType.METRIC and not ranking_metric:
    #             ranking_metric = component
    #             print(f"   📊 Métrica de ranking: {component.text}")
    #         elif component.type == ComponentType.OPERATION and not ranking_operation:
    #             ranking_operation = component
    #             print(f"   ⚡ Operación de ranking: {component.text}")
        
    #     confidence_factors = []
    #     base_confidence = 0.5
        
    #     base_confidence += 0.3
    #     confidence_factors.append("indicador_ranking")
        
    #     base_confidence += 0.2
    #     confidence_factors.append("valor_numérico")
        
    #     if ranking_metric:
    #         base_confidence += 0.1
    #         confidence_factors.append("métrica_encontrada")
        
    #     if ranking_operation:
    #         base_confidence += 0.1
    #         confidence_factors.append("operación_encontrada")
        
    #     final_confidence = min(1.0, base_confidence)
        
    #     raw_tokens = tokens[ranking_start_idx:ranking_start_idx + len(value_tokens) + 1]
        
    #     ranking_criteria = RankingCriteria(
    #         direction=ranking_direction,
    #         unit=ranking_unit,
    #         value=ranking_value,
    #         metric=ranking_metric,
    #         operation=ranking_operation,
    #         confidence=final_confidence,
    #         raw_tokens=raw_tokens
    #     )
        
    #     print(f"🏆 CRITERIO DE RANKING DETECTADO:")
    #     print(f"   🎯 Dirección: {ranking_direction.value}")
    #     print(f"   📊 Unidad: {ranking_unit.value}")
    #     print(f"   🔢 Valor: {ranking_value}")
    #     print(f"   📈 Métrica: {ranking_metric.text if ranking_metric else 'N/A'}")
    #     print(f"   ⚡ Operación: {ranking_operation.text if ranking_operation else 'N/A'}")
    #     print(f"   ⭐ Confianza: {final_confidence:.2f}")
    #     print(f"   🔤 Tokens: {raw_tokens}")
        
    #     return ranking_criteria


# --- DETECTAR MULTIDIMENSIONES ---

    def detect_multi_dimensions(self, tokens: List[str], classified_components: Dict) -> List[QueryComponent]:
            """🔧 DETECTOR GENÉRICO DE MÚLTIPLES DIMENSIONES"""
            
            print(f"🔗 DETECTANDO MÚLTIPLES DIMENSIONES:")
            
            # PASO 1: Identificar dimensiones y conectores
            dimension_candidates = []
            connector_positions = []
            
            for i, token in enumerate(tokens):
                if token in classified_components:
                    component = classified_components[token]
                    if component.type == ComponentType.DIMENSION:
                        dimension_candidates.append((i, component))
                    elif (component.type == ComponentType.CONNECTOR and 
                        token.lower() in ['y', 'and', ',']):
                        connector_positions.append(i)
            
            print(f"   📍 Dimensiones encontradas: {[(i, comp.text) for i, comp in dimension_candidates]}")
            print(f"   🔗 Conectores en posiciones: {connector_positions}")
            
            # PASO 2: Validar patrón secuencial
            if len(dimension_candidates) >= 2 and len(connector_positions) >= 1:
                valid_dimensions = self._validate_dimension_sequence(
                    dimension_candidates, connector_positions, tokens
                )
                
                if len(valid_dimensions) >= 2:
                    print(f"   ✅ MÚLTIPLES DIMENSIONES válidas: {[d.text for d in valid_dimensions]}")
                    return valid_dimensions
            
            print(f"   ❌ No se detectó patrón multi-dimensional válido")
            return []
        
        
    def _is_complex_multi_dimensional_case(self, tokens: List[str], classified_components: Dict) -> bool:
        """🔒 Detecta si es un caso complejo que necesita nueva lógica - CONSERVADOR"""
        
        # CONDICIÓN 1: Debe tener ranking (top/mejores)
        has_ranking = any(
            token.lower() in ['top', 'mejores', 'mejor', 'primeros'] 
            for token in tokens
        )
        
        # CONDICIÓN 2: Debe tener múltiples dimensiones conectadas por 'y'
        dimension_count = sum(
            1 for comp in classified_components.values() 
            if comp.type == ComponentType.DIMENSION
        )
        has_connector_y = 'y' in [token.lower() for token in tokens]
        
        # CONDICIÓN 3: Debe tener filtros temporales complejos
        has_complex_temporal = any(
            token.lower() in ['entre', 'desde'] 
            for token in tokens
        )
        
        # CONDICIÓN 4: Verificar que NO sea un caso simple conocido
        is_simple_case = (
            dimension_count == 1 and 
            not has_complex_temporal and
            len(tokens) <= 6
        )
        
        # SOLO aplicar nueva lógica si TODAS las condiciones se cumplen Y no es simple
        is_complex_case = (
            has_ranking and 
            dimension_count >= 2 and 
            has_connector_y and 
            has_complex_temporal and
            not is_simple_case
        )
        
        print(f"🔍 ¿Es caso complejo multi-dimensional? {is_complex_case}")
        print(f"   📊 Ranking: {has_ranking}, Dims: {dimension_count}, Conector: {has_connector_y}")
        print(f"   ⏰ Temporal complejo: {has_complex_temporal}, Simple: {is_simple_case}")
        
        return is_complex_case


    def _validate_compatibility_requirements(self, structure: QueryStructure) -> bool:
        """🔒 Valida que nueva lógica sea realmente necesaria"""
        
        # Evitar nueva lógica para casos que ya funcionan bien
        simple_patterns = [
            structure.query_pattern == QueryPattern.AGGREGATION and len(structure.column_conditions) <= 1,
            structure.query_pattern == QueryPattern.REFERENCED and not structure.is_multi_dimension_query,
            len(structure.operations) == 1 and len(structure.metrics) == 1 and not structure.is_ranking_query
        ]
        
        if any(simple_patterns):
            print(f"🔒 Caso simple detectado - mantener lógica original")
            return False
        
        return True
            
    
        
    def _validate_dimension_sequence(self, dimension_candidates: List, connector_positions: List, tokens: List[str]) -> List[QueryComponent]:
        """Validador de secuencia dimensional"""
        valid_dimensions = []
        
        # REGLA: dim1 + conector + dim2 [+ conector + dim3...]
        for i, (pos, component) in enumerate(dimension_candidates):
            if i == 0:
                # Primera dimensión siempre válida
                valid_dimensions.append(component)
            else:
                # Verificar que hay conector antes de esta dimensión
                prev_dim_pos = dimension_candidates[i-1][0]
                has_connector_between = any(
                    prev_dim_pos < conn_pos < pos 
                    for conn_pos in connector_positions
                )
                
                if has_connector_between:
                    valid_dimensions.append(component)
                    print(f"      ✅ '{component.text}' válida (conector encontrado)")
                else:
                    print(f"      ❌ '{component.text}' inválida (sin conector)")
                    break
        
        return valid_dimensions



        # ====================================
        # GRUPO 4: CONSTRUCCIÓN DE ESTRUCTURA 
        # construcción de estructura principal
        # ====================================


# ------  "Conatructor principal de estructura" -------

    def build_unified_structure(self, classified_components: Dict, column_value_pairs: List[ColumnValuePair], temporal_filters: List[TemporalFilter], tokens: List[str]) -> QueryStructure:
        """Constructor Principal de Estructura - VERSIÓN CORREGIDA PARA MULTI-DIMENSIONES"""
        
        # PASO 0: Calcular columnas temporales
        temporal_columns = set()
        for tf in temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.update(['semana', 'semanas', 'week', 'weeks'])
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.update(['mes', 'meses', 'month', 'months'])
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.update(['dia', 'dias', 'day', 'days'])
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.update(['año', 'años', 'year', 'years'])
        
        print(f"⏰ Columnas temporales calculadas: {temporal_columns}")
        
        main_dimension = None
        operations = []
        metrics = []
        values = []
        connectors = []
        unknown_tokens = []
        
        print(f"🔍 DEBUG: Buscando dimensión principal...")
        
        # PASO 1: Detectar rankings y exclusiones primero
        ranking_criteria = self.detect_ranking_criteria(tokens, classified_components)
        exclusion_filters = self.detect_exclusion_filters(tokens, classified_components)
        is_ranking = self.is_ranking_query(ranking_criteria, exclusion_filters)
        
        # PASO 1.2: Detectar múltiples dimensiones
        multi_dimensions = self.detect_multi_dimensions(tokens, classified_components)
        is_multi_dimension = len(multi_dimensions) >= 2
        
        # PASO 1.5: Solo SI NO es ranking, procesar otros patrones
        if not is_ranking:
            compound_criteria = self.detect_compound_criteria(tokens, classified_components)
            is_compound = self.is_compound_query(compound_criteria)
        else:
            compound_criteria = []
            is_compound = False
        
        # PASO 2: Recopilar todas las dimensiones candidatas (con filtro temporal mejorado)
        dimension_candidates = []
        for token, component in classified_components.items():
            
            # Exclusión temporal específica para rankings
            if (is_ranking and 
                component.type == ComponentType.DIMENSION and
                component.text.lower() in temporal_columns):
                print(f"🏆⏰ Excluyendo '{component.text}' en contexto de ranking")
                continue
            
            if component.type == ComponentType.DIMENSION:
                
                # Usar la versión mejorada de exclusión temporal
                if self.should_exclude_temporal_dimension_enhanced(component, temporal_filters, is_ranking):
                    continue
                    
                dimension_candidates.append((token, component))
                print(f"   📍 Candidato válido: '{component.text}' (tipo: {component.type.value})")
        
        # PASO 3: Construir estructura temporal para verificar agregación global
        temp_structure = QueryStructure(
            main_dimension=None,
            operations=[comp for comp in classified_components.values() if comp.type == ComponentType.OPERATION],
            metrics=[comp for comp in classified_components.values() if comp.type == ComponentType.METRIC],
            column_conditions=column_value_pairs,
            temporal_filters=temporal_filters,
            values=[comp for comp in classified_components.values() if comp.type == ComponentType.VALUE],
            connectors=[comp for comp in classified_components.values() if comp.type == ComponentType.CONNECTOR],
            unknown_tokens=[comp for comp in classified_components.values() if comp.type == ComponentType.UNKNOWN]
        )
        
        # PASO 4: Verificar agregación global
        available_dimension_components = [candidate[1] for candidate in dimension_candidates]
        
        if self.is_global_aggregation_query(temp_structure, available_dimension_components):
            print(f"🌐 Consulta identificada como AGREGACIÓN GLOBAL - sin dimensión principal")
            main_dimension = None
        else:
            
            # PASO 5: Determinar dimensión principal
            if dimension_candidates:
                if len(dimension_candidates) == 1:
                    main_dimension = dimension_candidates[0][1]
                    print(f"✅ Dimensión única: '{main_dimension.text}'")
                else:
                    print(f"🤔 Múltiples dimensiones detectadas: {[d[1].text for d in dimension_candidates]}")
                    
                    # 🔧 NUEVA LÓGICA: Para múltiples dimensiones, usar la primera como principal
                    # y NO convertir las otras a métricas si es ranking multi-dimensional
                    if is_multi_dimension and is_ranking:
                        main_dimension = dimension_candidates[0][1]
                        print(f"✅ RANKING MULTI-DIMENSIONAL: Primera dimensión como principal: '{main_dimension.text}'")
                        print(f"🔗 Manteniendo otras dimensiones en main_dimensions (NO convertir)")
                        
                    else:
                        # Aplicar heurísticas existentes para casos NO multi-dimensionales
                        dimensions_not_in_filters = []
                        dimensions_in_filters = []
                        
                        for token, dimension in dimension_candidates:
                            has_filter = any(cvp.column_name == dimension.text for cvp in column_value_pairs)
                            has_exclusion = any(ef.column_name == dimension.text for ef in exclusion_filters)
                            
                            if has_filter or has_exclusion:
                                dimensions_in_filters.append((token, dimension))
                                print(f"   🎛️ '{dimension.text}' tiene filtro o exclusión asociada")
                            else:
                                dimensions_not_in_filters.append((token, dimension))
                                print(f"   📍 '{dimension.text}' NO tiene filtro ni exclusión")
                        
                        if dimensions_not_in_filters:
                            main_dimension = dimensions_not_in_filters[0][1]
                            print(f"✅ Dimensión principal (sin filtro): '{main_dimension.text}'")
                            
                            # Solo convertir si NO es ranking multi-dimensional
                            for i in range(1, len(dimensions_not_in_filters)):
                                _, remaining_dimension = dimensions_not_in_filters[i]
                                
                                # Verificar si esta dimensión ya está en criterios compuestos
                                used_in_compound = any(
                                    criteria.metric.text == remaining_dimension.text 
                                    for criteria in compound_criteria
                                )
                                
                                # Verificar si está en criterios de ranking
                                used_in_ranking = (ranking_criteria and 
                                                ranking_criteria.metric and 
                                                ranking_criteria.metric.text == remaining_dimension.text)
                                
                                if not used_in_compound and not used_in_ranking and not is_multi_dimension:
                                    # Solo convertir si NO es multi-dimensional
                                    metric_component = QueryComponent(
                                        text=remaining_dimension.text,
                                        type=ComponentType.METRIC,
                                        confidence=remaining_dimension.confidence * 0.9,
                                        subtype='converted_from_dimension',
                                        value=remaining_dimension.value,
                                        column_name=remaining_dimension.column_name,
                                        linguistic_info={'converted_from': 'dimension', 'original_type': 'dimension'}
                                    )
                                    metrics.append(metric_component)
                                    print(f"🔄 Convirtiendo '{remaining_dimension.text}' de dimensión a métrica")
                                else:
                                    print(f"🔗 Manteniendo '{remaining_dimension.text}' como dimensión (multi-dimensional o en uso)")
                            
                        elif dimensions_in_filters:
                            main_dimension = dimensions_in_filters[0][1]
                            print(f"✅ Dimensión principal (con filtro): '{main_dimension.text}'")

        # PASO 6: Separar resto de componentes (🔧 LÓGICA CORREGIDA)
        for token, component in classified_components.items():
            
            # 🔧 NUEVO: NO auto-convertir dimensiones si es ranking multi-dimensional
            if component.type == ComponentType.DIMENSION:
                is_main_dimension = (main_dimension and component.text == main_dimension.text)
                is_in_multi_dimensions = any(dim.text == component.text for dim in multi_dimensions)
                
                # Si es ranking multi-dimensional, NO convertir ninguna dimensión
                if is_ranking and is_multi_dimension and is_in_multi_dimensions:
                    print(f"🔗 MANTENIENDO '{component.text}' como dimensión (ranking multi-dimensional)")
                    continue
                    
                # Lógica original para otros casos
                existing_real_metrics = [
                    comp for comp in classified_components.values() 
                    if comp.type == ComponentType.METRIC and 
                    not comp.linguistic_info.get('converted_from') == 'dimension'
                ]
                
                has_real_metrics = len(existing_real_metrics) > 0
                
                if not is_main_dimension and not has_real_metrics and not is_multi_dimension:
                    # Solo auto-convertir si NO es multi-dimensional
                    metric_component = QueryComponent(
                        text=component.text,
                        type=ComponentType.METRIC,
                        confidence=component.confidence * 0.85,
                        subtype='converted_from_dimension',
                        value=component.value,
                        column_name=component.column_name,
                        linguistic_info={'converted_from': 'dimension', 'original_type': 'dimension'}
                    )
                    metrics.append(metric_component)
                    print(f"🔄 Auto-convirtiendo '{component.text}' de dimensión secundaria a métrica")
                else:
                    print(f"🛑 NO auto-convertir '{component.text}': es_principal={is_main_dimension}, hay_métricas_reales={has_real_metrics}, multi_dim={is_multi_dimension}")
                    
            elif component.type == ComponentType.OPERATION:
                operations.append(component)
            elif component.type == ComponentType.METRIC:
                metrics.append(component)
                print(f"✅ Métrica real detectada: '{component.text}'")

        # PASO 7: Para consultas compuestas, extraer operaciones y métricas de criterios
        if is_compound:
            print(f"🔗 PROCESANDO CONSULTA COMPUESTA:")
            for criteria in compound_criteria:
                # Agregar operaciones y métricas desde criterios compuestos
                if not any(op.text == criteria.operation.text for op in operations):
                    operations.append(criteria.operation)
                    print(f"   ⚡ Agregando operación desde criterio: {criteria.operation.text}")
                
                if not any(m.text == criteria.metric.text for m in metrics):
                    metrics.append(criteria.metric)
                    print(f"   📊 Agregando métrica desde criterio: {criteria.metric.text}")
        
        # PASO 8: Para consultas de ranking, extraer operaciones y métricas de criterios
        if is_ranking and ranking_criteria:
            print(f"🏆 PROCESANDO CONSULTA DE RANKING:")
            
            if ranking_criteria.operation and not any(op.text == ranking_criteria.operation.text for op in operations):
                operations.append(ranking_criteria.operation)
                print(f"   ⚡ Agregando operación desde ranking: {ranking_criteria.operation.text}")
            
            if ranking_criteria.metric and not any(m.text == ranking_criteria.metric.text for m in metrics):
                metrics.append(ranking_criteria.metric)
                print(f"   📊 Agregando métrica desde ranking: {ranking_criteria.metric.text}")
        
        # PASO 9: Construir estructura final
        structure = QueryStructure(
            main_dimension=main_dimension,
            main_dimensions=multi_dimensions if is_multi_dimension else ([main_dimension] if main_dimension else []),  
            is_multi_dimension_query=is_multi_dimension,  
            operations=operations,
            metrics=metrics,
            column_conditions=column_value_pairs,
            temporal_filters=temporal_filters,
            values=values,
            connectors=connectors,
            unknown_tokens=unknown_tokens,
            compound_criteria=compound_criteria,
            is_compound_query=is_compound,
            ranking_criteria=ranking_criteria,
            exclusion_filters=exclusion_filters,
            is_ranking_query=is_ranking
        )
        
        # DETECTAR PATRÓN DE CONSULTA
        query_pattern = self.detect_query_pattern(structure)
        structure.query_pattern = query_pattern
        
        # CONFIGURAR LÍMITES según el tipo de consulta
        if query_pattern == QueryPattern.TOP_N and structure.ranking_criteria:
            if structure.ranking_criteria.unit == RankingUnit.COUNT:
                structure.limit_value = int(structure.ranking_criteria.value)
            elif structure.ranking_criteria.unit == RankingUnit.PERCENTAGE:
                structure.limit_value = None  # Se calculará en tiempo de ejecución
            structure.is_single_result = False
            
            print(f"🏆 CONFIGURACIÓN DE RANKING:")
            print(f"   📍 Dimensión objetivo: {structure.main_dimension.text}")
            print(f"   📊 Métrica de ranking: {structure.ranking_criteria.metric.text if structure.ranking_criteria.metric else 'N/A'}")
            print(f"   🎯 Dirección: {structure.ranking_criteria.direction.value}")
            print(f"   📈 Unidad: {structure.ranking_criteria.unit.value}")
            print(f"   🔢 Valor: {structure.ranking_criteria.value}")
            print(f"   🚫 Exclusiones: {len(structure.exclusion_filters)}")
            print(f"   🔢 Límite: {structure.limit_value}")
        
        elif query_pattern == QueryPattern.REFERENCED:
            structure.reference_metric = metrics[0] if metrics else None
            structure.is_single_result = True
            structure.limit_value = 1
            
            print(f"🎯 CONFIGURACIÓN DE DATOS REFERENCIADOS:")
            print(f"   📍 Dimensión objetivo: {structure.main_dimension.text}")
            print(f"   📊 Métrica de referencia: {structure.reference_metric.text if structure.reference_metric else 'N/A'}")
            print(f"   ⚡ Operación de referencia: {operations[0].value if operations else 'N/A'}")
            print(f"   🔗 Es compuesta: {structure.is_compound_query}")
            print(f"   🔢 Límite: {structure.limit_value}")
        
        # DEBUG: Mostrar estructura final
        print(f"🏗️ ESTRUCTURA FINAL:")
        print(f"   📍 Dimensión principal: {main_dimension.text if main_dimension else 'NINGUNA (agregación global)'}")
        print(f"   🔗 Múltiples dimensiones: {[d.text for d in multi_dimensions] if is_multi_dimension else 'No'}")
        print(f"   🎛️ Filtros: {[f'{cvp.column_name} = {cvp.value}' for cvp in column_value_pairs]}")
        print(f"   🚫 Exclusiones: {[f'{ef.column_name} != {ef.value}' for ef in exclusion_filters]}")
        print(f"   ⚡ Operaciones: {[op.text for op in operations]}")
        print(f"   📊 Métricas: {[m.text for m in metrics]}")
        print(f"   🔗 Criterios compuestos: {len(compound_criteria)}")
        print(f"   🏆 Es ranking: {is_ranking}")
        print(f"   ⏰ Filtros temporales: {len(temporal_filters)}")
        print(f"   🎯 Patrón de consulta: {query_pattern.value}")
        
        if hasattr(self, '_current_original_intent'):
            structure.original_semantic_intent = self._current_original_intent
            print(f"   🧠 Intent semántico: {structure.original_semantic_intent}") 
        
        return structure


# ------  "Detector de agregacion global" -------

    def is_global_aggregation_query(self, structure: QueryStructure, available_dimensions: List[QueryComponent] = None) -> bool:
        """Detector de Agregación Global"""
        has_operation = len(structure.operations) > 0
        has_metric = len(structure.metrics) > 0
        has_column_filters = len(structure.column_conditions) > 0
        has_available_dimensions = available_dimensions and len(available_dimensions) > 0
        
        print(f"🔍 EVALUANDO AGREGACIÓN GLOBAL:")
        print(f"   📊 Tiene operación: {has_operation}")
        print(f"   📈 Tiene métrica: {has_metric}")
        print(f"   🎛️ Tiene filtros de columna: {has_column_filters}")
        print(f"   📍 Tiene dimensiones disponibles: {has_available_dimensions}")
        
        # CRITERIO REFINADO: Es agregación global SOLO si:
        # 1. Tiene operación + métrica
        # 2. NO tiene filtros de columna específicos
        # 3. NO tiene dimensiones principales válidas disponibles
        
        if has_operation and has_metric and not has_column_filters and not has_available_dimensions:
            print(f"🌐 Detectada agregación global: operación + métrica sin filtros ni dimensiones")
            return True
        
        # Si hay dimensiones disponibles, NO es agregación global
        if has_available_dimensions:
            print(f"📍 NO es agregación global: hay dimensiones principales disponibles")
            return False
        
        return False


# ------  "Exclusor de dimensiones temporales" -------

    def should_exclude_temporal_dimension(self, dimension_candidate: QueryComponent, temporal_filters: List[TemporalFilter]) -> bool:
        """Exclusor de Dimensiones Temporales"""
        temporal_units = {'semana', 'semanas', 'mes', 'meses', 'año', 'años', 'dia', 'dias'}
        
        if dimension_candidate.text.lower() in temporal_units and len(temporal_filters) > 0:
            print(f"⏰ Excluyendo '{dimension_candidate.text}' como dimensión principal (es parte de filtro temporal)")
            return True
        
        return False


# ------  "Detector temporal mejorado" -------

    def should_exclude_temporal_dimension_enhanced(self, dimension_candidate: QueryComponent, temporal_filters: List[TemporalFilter], is_ranking_query: bool = False) -> bool:
        """Exclusor Temporal Mejorado"""
        temporal_units = {'semana', 'semanas', 'week', 'weeks', 'mes', 'meses', 'dia', 'dias'}
        token_lower = dimension_candidate.text.lower()
        
        # CRITERIO 1: Siempre excluir unidades temporales si hay filtros temporales
        if token_lower in temporal_units and len(temporal_filters) > 0:
            print(f"⏰ Excluyendo '{dimension_candidate.text}' (unidad temporal con filtros)")
            return True
        
        # CRITERIO 2: En rankings, ser más agresivo excluyendo temporales
        if is_ranking_query and token_lower in temporal_units:
            print(f"🏆⏰ Excluyendo '{dimension_candidate.text}' (temporal en ranking)")
            return True
        
        return False


# ------  "Detector de filtros de exclusion" -------

    def detect_exclusion_filters(self, tokens: List[str], classified_components: Dict) -> List[ExclusionFilter]:
        """Detector de Filtros de Exclusión"""
        print(f"🚫 DETECTANDO FILTROS DE EXCLUSIÓN:")
        
        exclusion_filters = []
        
        # DICCIONARIOS DE INDICADORES DE EXCLUSIÓN
        exclusion_indicators = {
            'excluyendo', 'exceptuando', 'excepto', 'sin', 'excluding', 'except', 'without',
            'menos', 'quitando', 'omitiendo', 'descartando'
        }
        
        # PASO 1: Buscar indicadores de exclusión
        for i, token in enumerate(tokens):
            token_lower = token.lower()
            
            if token_lower in exclusion_indicators:
                print(f"   🚫 Indicador de exclusión encontrado: '{token}' en posición {i}")
                
                # PASO 2: Buscar patrón [COLUMNA] [VALOR] después del indicador
                exclusion_filter = self.extract_exclusion_from_position(tokens, i + 1, classified_components)
                
                if exclusion_filter:
                    exclusion_filters.append(exclusion_filter)
                    print(f"   ✅ Filtro de exclusión extraído: {exclusion_filter.column_name} != '{exclusion_filter.value}'")
        
        print(f"🚫 TOTAL FILTROS DE EXCLUSIÓN: {len(exclusion_filters)}")
        return exclusion_filters


# ------  "Extractor de exclusiones posicionales" -------

    def extract_exclusion_from_position(self, tokens: List[str], start_pos: int, classified_components: Dict) -> Optional[ExclusionFilter]:
        """Extractor de Exclusiones Posicionales"""
        if start_pos >= len(tokens) - 1:
            return None
        
        # Buscar patrón [COLUMNA] [VALOR] en las siguientes posiciones
        search_end = min(start_pos + 3, len(tokens))
        
        for i in range(start_pos, search_end - 1):
            if i + 1 >= len(tokens):
                break
                
            current_token = tokens[i]
            next_token = tokens[i + 1]
            
            print(f"      🔍 Analizando exclusión: '{current_token}' + '{next_token}'")
            
            # Verificar si current_token es una columna potencial
            column_info = self._identify_potential_column(current_token)
            
            if column_info['is_column']:
                # Verificar si next_token es un valor
                value_info = self._identify_potential_value(next_token, i + 1, tokens)
                
                if value_info['is_value']:
                    # Construir filtro de exclusión
                    confidence = min(column_info['confidence'], value_info['confidence']) * 0.9  # Reducir por ser exclusión
                    
                    return ExclusionFilter(
                        exclusion_type=ExclusionType.NOT_EQUALS,  # Por defecto, NOT_EQUALS
                        column_name=column_info['normalized_name'],
                        value=value_info['normalized_value'],
                        confidence=confidence,
                        raw_tokens=tokens[start_pos-1:i+2]  # Incluir indicador de exclusión
                    )
        
        return None


# ------  "Verificador de consultas de ranking" -------

    def is_ranking_query(self, ranking_criteria: Optional[RankingCriteria], exclusion_filters: List[ExclusionFilter]) -> bool:
        """Verificador de Consulta de Ranking"""
        # Es ranking si tiene criterios válidos
        has_valid_ranking = ranking_criteria and ranking_criteria.confidence >= 0.6
        
        is_ranking = bool(has_valid_ranking)
        
        print(f"🏆 EVALUANDO SI ES CONSULTA DE RANKING:")
        print(f"   📊 Tiene criterios válidos: {has_valid_ranking}")
        print(f"   🚫 Filtros de exclusión: {len(exclusion_filters)}")
        print(f"   🎯 Es ranking: {is_ranking}")
        
        return is_ranking



        # ================================================
        # GRUPO 5: VALIDACIÓN Y CONFIANZA 
        # Validación de estructura y cálculos de confianza
        # ================================================



# ------  "Validador de estructura completa" -------

    def validate_structure(self, structure: QueryStructure) -> Dict:
        """Validador de Estructura Completa"""
        errors = []
        suggestions = []
        
        # NUEVA VALIDACIÓN: Permitir consultas sin dimensión principal si son agregaciones globales
        if not structure.main_dimension:
            # Verificar si es una agregación global válida
            if self.is_global_aggregation_query(structure):
                print(f"✅ Agregación global válida detectada - dimensión principal no requerida")
            else:
                # Solo es error si NO es agregación global
                if structure.column_conditions:
                    available_columns = [cvp.column_name for cvp in structure.column_conditions]
                    suggestions.append(f"Columnas detectadas: {', '.join(available_columns)}")
                errors.append("Falta dimensión principal")
                suggestions.append("Agrega una entidad como: partner_code, product_group, cuentas, tienda")
        
        # Validación para contenido significativo
        has_meaningful_content = (
            structure.metrics or 
            structure.operations or 
            structure.column_conditions or
            structure.temporal_filters
        )
        
        if not has_meaningful_content:
            errors.append("Falta métrica, operación o condición")
            suggestions.append("Agrega una métrica como: ventas, sell_out, sales_amount")
        
        # Advertencias para tokens desconocidos
        if structure.unknown_tokens:
            unknown_words = [token.text for token in structure.unknown_tokens]
            suggestions.append(f"Palabras no reconocidas: {', '.join(unknown_words)}")
        
        return {
            'valid': len(errors) == 0,
            'error': '; '.join(errors) if errors else None,
            'suggestions': suggestions
        }


# ------  "Calculador de nivel de complejidad" -------

    def get_complexity_level(self) -> str:
        """Calculador de Nivel de Complejidad"""
        complexity_score = 0

        complexity_score += len(self.column_conditions) * 2
        complexity_score += len(self.temporal_filters) * 3
        complexity_score += len(self.operations) * 1
        complexity_score += len(self.unknown_tokens) * -1
        
        # NUEVA LÓGICA: Complejidad por consultas compuestas
        if self.is_compound_query:
            complexity_score += len(self.compound_criteria) * 2
        
        # Agregar complejidad por patrón
        if self.query_pattern == QueryPattern.REFERENCED:
            complexity_score += 2
        elif self.query_pattern == QueryPattern.LIST_ALL:
            complexity_score += 1
            
        if complexity_score <= 0:
            return "simple"
        elif complexity_score <= 3:
            return "moderada"
        elif complexity_score <= 6:
            return "compleja"
        else:
            return "muy_compleja"


# ------  "Calculador de confianza general" -------

    def calculate_overall_confidence(self, structure: QueryStructure) -> float:
        """Calculador de Confianza General"""
        all_components = []
        
        if structure.main_dimension:
            all_components.append(structure.main_dimension)
        
        all_components.extend(structure.operations)
        all_components.extend(structure.metrics)
        all_components.extend(structure.values)
        all_components.extend(structure.connectors)
        all_components.extend(structure.unknown_tokens)
        
        # Agregar confianza de condiciones de columna
        for condition in structure.column_conditions:
            all_components.append(QueryComponent("dummy", ComponentType.COLUMN_VALUE, condition.confidence))
        
        # Agregar confianza de filtros temporales
        for tf in structure.temporal_filters:
            all_components.append(QueryComponent("dummy", ComponentType.TEMPORAL, tf.confidence))
        
        if not all_components:
            return 0.0
        
        # Calcular promedio ponderado
        total_confidence = sum(comp.confidence for comp in all_components)
        return round(total_confidence / len(all_components), 2)


# ------  "Calculador de confianza referencial" -------

    def calculate_reference_confidence(self, structure: QueryStructure) -> float:
        """Calculador de Confianza Referencial"""
        print(f"   🔍 CALCULANDO CONFIANZA PARA DATOS REFERENCIADOS:")
        
        base_confidence = 0.5  # Confianza base
        factors = []
        
        # Factor 1: Tiene dimensión (+0.15)
        if structure.main_dimension:
            base_confidence += 0.15
            factors.append("tiene_dimensión")
        
        # Factor 2: Operación única (+0.1)
        if len(structure.operations) == 1:
            base_confidence += 0.1
            factors.append("operación_única")
        
        # Factor 3: Métrica única (+0.1)
        if len(structure.metrics) == 1:
            base_confidence += 0.1
            factors.append("métrica_única")
        
        # Factor 4: Sin filtros de columna (+0.1)
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("sin_filtros_columna")
        
        # Factor 5: Operación de comparación (+0.2)
        if structure.operations and structure.operations[0].value in ['máximo', 'mínimo']:
            base_confidence += 0.2
            factors.append("operación_comparación")
        
        # Factor 6: Operación específica de referencia (+0.05)
        if structure.operations:
            op_text = structure.operations[0].text.lower()
            reference_ops = ['mas', 'más', 'mayor', 'mejor', 'menos', 'menor', 'peor']
            if op_text in reference_ops:
                base_confidence += 0.05
                factors.append(f"operación_referencia_{op_text}")
        
        # Penalizaciones
        if len(structure.operations) > 1:
            base_confidence -= 0.2
            factors.append("múltiples_operaciones_-0.2")
        
        if len(structure.metrics) > 1:
            base_confidence -= 0.2
            factors.append("múltiples_métricas_-0.2")
        
        if len(structure.column_conditions) > 0:
            base_confidence -= 0.15
            factors.append("filtros_columna_-0.15")
        
        # Limitar entre 0.0 y 1.0
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 Factores aplicados: {factors}")
        print(f"      ⭐ Confianza final: {final_confidence:.2f}")
        
        return final_confidence


# ------  "Calculador de confianza compuesta" -------

    def calculate_compound_reference_confidence(self, structure: QueryStructure) -> float:
        """Calculador de Confianza Compuesta"""
        print(f"   🔍 CALCULANDO CONFIANZA PARA CONSULTA COMPUESTA REFERENCIADA:")
        
        base_confidence = 0.6  # Confianza base más alta para compuestas
        factors = []
        
        # Factor 1: Tiene dimensión (+0.1)
        if structure.main_dimension:
            base_confidence += 0.1
            factors.append("tiene_dimensión")
        
        # Factor 2: Número de criterios válidos (+0.05 por criterio, max +0.15)
        valid_criteria = len([c for c in structure.compound_criteria if c.confidence >= 0.7])
        criteria_bonus = min(valid_criteria * 0.05, 0.15)
        base_confidence += criteria_bonus
        factors.append(f"criterios_válidos_{valid_criteria}_+{criteria_bonus}")
        
        # Factor 3: Sin filtros de columna (+0.1)
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("sin_filtros_columna")
        
        # Factor 4: Todas las operaciones son de comparación (+0.1)
        reference_operations = ['máximo', 'mínimo', 'mayor', 'menor']
        all_reference = all(
            criteria.operation.value in reference_operations 
            for criteria in structure.compound_criteria
        )
        if all_reference:
            base_confidence += 0.1
            factors.append("todas_operaciones_comparación")
        
        # Factor 5: Calidad promedio de criterios (+0.05)
        if structure.compound_criteria:
            avg_criteria_confidence = sum(c.confidence for c in structure.compound_criteria) / len(structure.compound_criteria)
            if avg_criteria_confidence >= 0.8:
                base_confidence += 0.05
                factors.append(f"alta_calidad_criterios_{avg_criteria_confidence:.2f}")
        
        # Limitar entre 0.0 y 1.0
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 Factores aplicados: {factors}")
        print(f"      ⭐ Confianza final: {final_confidence:.2f}")
        
        return final_confidence


# ------  "Calculador de confianza ranking" -------

    def calculate_ranking_confidence(self, structure: QueryStructure) -> float:
        """Calculador de Confianza de Ranking"""
        print(f"   🔍 CALCULANDO CONFIANZA PARA CONSULTA DE RANKING:")
        
        if not structure.ranking_criteria:
            return 0.0
        
        base_confidence = structure.ranking_criteria.confidence
        factors = ['criterio_base']
        
        # Factor 1: Tiene dimensión principal (+0.1)
        if structure.main_dimension:
            base_confidence += 0.1
            factors.append("tiene_dimensión")
        
        # Factor 2: Tipo de unidad
        if structure.ranking_criteria.unit == RankingUnit.PERCENTAGE:
            base_confidence += 0.05  # Porcentajes son más específicos
            factors.append("usa_porcentaje")
        elif structure.ranking_criteria.unit == RankingUnit.COUNT:
            base_confidence += 0.03
            factors.append("usa_número")
        
        # Factor 3: Tiene métrica específica (+0.05)
        if structure.ranking_criteria.metric:
            base_confidence += 0.05
            factors.append("métrica_específica")
        
        # Factor 4: Tiene filtros de exclusión (+0.02 por filtro, max +0.06)
        if structure.exclusion_filters:
            exclusion_bonus = min(len(structure.exclusion_filters) * 0.02, 0.06)
            base_confidence += exclusion_bonus
            factors.append(f"exclusiones_{len(structure.exclusion_filters)}")
        
        # Factor 5: Valor razonable
        if structure.ranking_criteria.unit == RankingUnit.COUNT and 1 <= structure.ranking_criteria.value <= 50:
            base_confidence += 0.03
            factors.append("valor_razonable_count")
        elif structure.ranking_criteria.unit == RankingUnit.PERCENTAGE and 1 <= structure.ranking_criteria.value <= 100:
            base_confidence += 0.03
            factors.append("valor_razonable_percentage")
        
        # Limitar entre 0.0 y 1.0
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 Factores aplicados: {factors}")
        print(f"      ⭐ Confianza final: {final_confidence:.2f}")
        
        return final_confidence


    def calculate_multi_dimension_confidence(self, structure: QueryStructure) -> float:
        """Calculador de Confianza Multi-Dimensional"""
        print(f"   🔍 CALCULANDO CONFIANZA PARA MÚLTIPLES DIMENSIONES:")
        
        base_confidence = 0.6
        factors = ['base_multi_dimension']
        
        # Factor 1: Número de dimensiones (+0.05 por dimensión extra)
        extra_dims = len(structure.main_dimensions) - 2
        if extra_dims > 0:
            bonus = min(extra_dims * 0.05, 0.15)
            base_confidence += bonus
            factors.append(f"dimensiones_extra_{extra_dims}")
        
        # Factor 2: Tiene operación y métrica (+0.2)
        if structure.operations and structure.metrics:
            base_confidence += 0.2
            factors.append("operacion_metrica")
        
        # Factor 3: Sin filtros complejos (+0.1)
        if len(structure.column_conditions) == 0:
            base_confidence += 0.1
            factors.append("sin_filtros_complejos")
        
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        print(f"      📊 Factores aplicados: {factors}")
        print(f"      ⭐ Confianza final: {final_confidence:.2f}")
        
        return final_confidence



        # =======================================
        # GRUPO 6: GENERACIÓN SQL 
        # Generación de consultas SQL optimizadas
        # =======================================



# ------  "Generador de SQL optimizado" -------



    def generate_optimized_sql(self, structure: QueryStructure) -> str:
        """Generador de SQL Optimizado - VERSIÓN CORREGIDA PARA RANKINGS MULTI-DIMENSIONALES"""
        select_parts = []
        from_clause = "FROM datos"
        where_conditions = []
        group_by_parts = []
        order_by_parts = []
        
        # Identificar columnas temporales para evitar duplicación
        temporal_columns = set()
        temporal_sql_added = False
        
        for tf in structure.temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.add('semana')
                temporal_columns.add('week')
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.add('mes')
                temporal_columns.add('month')
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.add('dia')
                temporal_columns.add('day')
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.add('año')
                temporal_columns.add('year')
        
        print(f"🗄️ Generando SQL optimizado:")
        print(f"   ⏰ Columnas temporales detectadas: {temporal_columns}")
        print(f"   🎯 Patrón de consulta: {structure.query_pattern.value}")
        print(f"   🔗 Es compuesta: {structure.is_compound_query}")
        print(f"   🏆 Es ranking: {structure.is_ranking_query}")
        print(f"   🔗 Es multi-dimensional: {structure.is_multi_dimension_query}")
        
        # 🔧 NUEVA LÓGICA: Manejar rankings multi-dimensionales
        if (structure.is_ranking_query and 
            structure.is_multi_dimension_query and 
            len(structure.main_dimensions) >= 2):
            print(f"🏆🔗 DETECTADO: Ranking multi-dimensional → usando generador especializado")
            return self.generate_multi_dimension_sql(structure, temporal_columns)
        
        # NUEVA LÓGICA: Manejar consultas multi-dimensionales sin ranking
        if (structure.is_multi_dimension_query and 
            structure.query_pattern == QueryPattern.MULTI_DIMENSION):
            print(f"🔗 DETECTADO: Multi-dimensional sin ranking → usando generador especializado")
            return self.generate_multi_dimension_sql(structure, temporal_columns)
        
        # Verificar si es agregación global
        is_global_aggregation = not structure.main_dimension and structure.operations and structure.metrics
        
        if is_global_aggregation:
            print(f"🌐 Generando SQL para agregación global")
            
            # Para agregaciones globales: solo la función de agregación
            if structure.operations and structure.metrics:
                operation = structure.operations[0]
                metric = structure.metrics[0]

                # OPERACIONES SQL DISPONIBLES
                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                        
        else:
            # Lógica para consultas con dimensión principal
            if structure.main_dimension:
                dim_name = structure.main_dimension.text
                select_parts.append(dim_name)
                group_by_parts.append(dim_name)
            
            # CONSULTAS COMPUESTAS
            if structure.is_compound_query and structure.compound_criteria:
                print(f"🔗 Procesando consulta compuesta con {len(structure.compound_criteria)} criterios:")
                
                # Agregar funciones de agregación para cada criterio
                for i, criteria in enumerate(structure.compound_criteria):
                    operation_value = criteria.operation.value
                    metric_text = criteria.metric.text
                    
                    # Usar _get_contextual_aggregation para 'máximo'
                    if operation_value == 'máximo':
                        agg_function = self._get_contextual_aggregation(structure, metric_text, operation_value)
                    else:
                        sql_operations = {
                            'mínimo': f'MIN({metric_text})',
                            'suma': f'SUM({metric_text})',
                            'promedio': f'AVG({metric_text})',
                            'conteo': f'COUNT({metric_text})'
                        }
                        agg_function = sql_operations.get(operation_value, f'SUM({metric_text})')
                    
                    if agg_function:
                        select_parts.append(agg_function)
                        
                        # Construir ORDER BY para múltiples criterios
                        if operation_value in ['máximo', 'mayor']:
                            order_direction = "DESC"
                        elif operation_value in ['mínimo', 'menor']:
                            order_direction = "ASC"
                        else:
                            order_direction = "DESC"
                        
                        order_by_parts.append(f"{agg_function} {order_direction}")
                        
                        print(f"   🔗 Criterio {i+1}: {operation_value} {metric_text} → {agg_function} {order_direction}")
                    else:
                        # Si no hay operación SQL específica, usar la métrica directamente
                        select_parts.append(metric_text)
                        order_by_parts.append(f"{metric_text} DESC")
                        print(f"   🔗 Criterio {i+1}: {metric_text} → {metric_text} DESC")
                
            # LÓGICA TRADICIONAL
            elif structure.operations and structure.metrics:
                operation = structure.operations[0]
                metric = structure.metrics[0]
                
                # Usar _get_contextual_aggregation para 'máximo'
                if operation.value == 'máximo':
                    agg_function = self._get_contextual_aggregation(structure, metric.text, operation.value)
                else:
                    sql_operations = {
                        'mínimo': f'MIN({metric.text})',
                        'suma': f'SUM({metric.text})',
                        'promedio': f'AVG({metric.text})',
                        'conteo': f'COUNT({metric.text})'
                    }
                    agg_function = sql_operations.get(operation.value, f'SUM({metric.text})')
                
                if agg_function:
                    select_parts.append(agg_function)
                    
                    # Para REFERENCED, ordenar por la métrica agregada
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        if operation.value in ['máximo', 'mayor']:
                            order_by_parts.append(f"{agg_function} DESC")
                        elif operation.value in ['mínimo', 'menor']:
                            order_by_parts.append(f"{agg_function} ASC")
                        else:
                            order_by_parts.append(f"{agg_function} DESC")
                    else:
                        order_by_parts.append(f"{agg_function} DESC")
                else:
                    # Si no hay operación SQL específica, usar la métrica directamente
                    select_parts.append(metric.text)
                    if structure.query_pattern == QueryPattern.REFERENCED:
                        order_by_parts.append(f"{metric.text} DESC")
        
        # WHERE para condiciones de columna (excluyendo temporales duplicadas)
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
                print(f"   ✅ Condición WHERE: {condition.column_name} = '{condition.value}'")
            else:
                print(f"   ⏰ Excluyendo condición temporal duplicada: {condition.column_name} = '{condition.value}'")
        
        # FILTROS TEMPORALES - CORREGIDO
        # Intentar filtros temporales avanzados
        advanced_conditions = self.get_advanced_temporal_sql_conditions(structure)
        if advanced_conditions:
            where_conditions.extend(advanced_conditions)
            temporal_sql_added = True
            print(f"   ✅ Usando filtros temporales avanzados: {advanced_conditions}")

        # CONSTRUCCIÓN DEL SQL FINAL
        sql_parts = []
        
        if select_parts:
            sql_parts.append(f"SELECT {', '.join(select_parts)}")
        else:
            sql_parts.append("SELECT *")
        
        sql_parts.append(from_clause)
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # LIMITAR LA DATA SEGUN EL USUARIO
        if structure.query_pattern == QueryPattern.REFERENCED:
            sql_parts.append("LIMIT 1")
            print(f"   🎯 Agregando LIMIT 1 para patrón REFERENCED")
            
        elif structure.query_pattern == QueryPattern.TOP_N and structure.limit_value:
            sql_parts.append(f"LIMIT {structure.limit_value}")
            print(f"   🏆 Agregando LIMIT {structure.limit_value} para patrón TOP_N")
        
        elif structure.is_ranking_query and structure.ranking_criteria and structure.ranking_criteria.value:
            limit_value = int(structure.ranking_criteria.value)
            sql_parts.append(f"LIMIT {limit_value}")
            print(f"   🏆 FORZANDO LIMIT {limit_value} para ranking (patrón: {structure.query_pattern.value})")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 SQL final: {final_sql}")
        
        return final_sql
            


# ------  "Generador de SQL ranking" -------

    def generate_ranking_sql(self, structure: QueryStructure, temporal_columns: set) -> str:
        """🔧 Generador de SQL para Rankings Multi-Criterio - VERSIÓN CORREGIDA"""
        print(f"🏆 GENERANDO SQL PARA RANKING MULTI-CRITERIO:")
        
        ranking = structure.ranking_criteria
        if not ranking:
            print(f"❌ Error: No hay criterios de ranking")
            return "SELECT * FROM datos;"
        
        # CONSTRUIR SELECT - ✅ INCLUIR TODAS LAS MÉTRICAS
        select_parts = []
        if structure.main_dimension:
            select_parts.append(structure.main_dimension.text)
        
        order_by_parts = []
        
        # 🔧 NUEVA LÓGICA: Procesar TODAS las métricas detectadas
        if len(structure.metrics) > 1:
            print(f"   🔗 DETECTANDO RANKING MULTI-CRITERIO con {len(structure.metrics)} métricas")
            
            # Mapear operaciones a métricas
            operations_available = [op.text.lower() for op in structure.operations if op.text.lower() in ['mas', 'más', 'mayor', 'menor', 'top']]
            metrics_available = [m.text for m in structure.metrics]
            
            print(f"   📊 Métricas: {metrics_available}")
            print(f"   ⚡ Operaciones: {operations_available}")
            
            # Asumir que las operaciones se aplican en orden a las métricas
            for i, metric in enumerate(metrics_available):
                # Determinar operación para esta métrica
                if i < len(operations_available):
                    op = operations_available[i]
                else:
                    op = operations_available[0] if operations_available else 'mas'  # Default
                
                # Mapear operación a función SQL
                if op in ['mas', 'más', 'mayor', 'top']:
                    agg_function = f'SUM({metric})'
                    order_direction = 'DESC'
                elif op in ['menor', 'minimo', 'mínimo']:
                    agg_function = f'SUM({metric})'  # Usando SUM, pero ordenando ASC
                    order_direction = 'ASC'
                else:
                    agg_function = f'SUM({metric})'
                    order_direction = 'DESC'
                
                select_parts.append(agg_function)
                order_by_parts.append(f"{agg_function} {order_direction}")
                
                print(f"   {i+1}. {metric} → {agg_function} {order_direction} (operación: {op})")
                
        else:
            # 🔧 LÓGICA ORIGINAL: Una sola métrica
            if ranking.metric:
                if ranking.operation and ranking.operation.text.lower() in ['mas', 'más', 'mayor']:
                    agg_function = f'SUM({ranking.metric.text})'
                    print(f"   🏆 Ranking: 'mas' interpretado como SUM")
                elif ranking.operation:
                    sql_operations = {
                        'máximo': f'MAX({ranking.metric.text})',
                        'mínimo': f'MIN({ranking.metric.text})',
                        'suma': f'SUM({ranking.metric.text})',
                        'promedio': f'AVG({ranking.metric.text})',
                        'conteo': f'COUNT({ranking.metric.text})'
                    }
                    agg_function = sql_operations.get(ranking.operation.value, f'SUM({ranking.metric.text})')
                else:
                    agg_function = f'SUM({ranking.metric.text})'
                
                if agg_function:
                    select_parts.append(agg_function)
                    
                    # Determinar dirección basada en el ranking
                    if ranking.direction == RankingDirection.TOP:
                        order_direction = "DESC"
                    else:
                        order_direction = "ASC"
                        
                    order_by_parts.append(f"{agg_function} {order_direction}")
                    print(f"   ✅ Función agregada al SELECT: {agg_function}")
        
        # CONSTRUIR WHERE (usando lógica existente)
        where_conditions = []
        
        # Condiciones regulares
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
        
        # Exclusiones
        for exclusion in structure.exclusion_filters:
            if exclusion.exclusion_type == ExclusionType.NOT_EQUALS:
                where_conditions.append(f"{exclusion.column_name} != '{exclusion.value}'")
        
        # Filtros temporales avanzados
        advanced_conditions = self.get_advanced_temporal_sql_conditions(structure)
        where_conditions.extend(advanced_conditions)
        
        # CONSTRUIR SQL FINAL
        sql_parts = [
            f"SELECT {', '.join(select_parts)}",
            "FROM datos"
        ]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if structure.main_dimension:
            sql_parts.append(f"GROUP BY {structure.main_dimension.text}")
        
        # 🔧 ORDER BY multi-criterio
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        sql_parts.append(f"LIMIT {int(ranking.value)}")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 SQL final multi-criterio: {final_sql}")
        
        return final_sql


# --- GENERACION DE RANKING SQL ---

    def generate_multi_dimension_sql(self, structure: QueryStructure, temporal_columns: set) -> str:
        """🔧 GENERADOR SQL PARA MÚLTIPLES DIMENSIONES - VERSIÓN CORREGIDA"""
        print(f"🔗 GENERANDO SQL PARA MÚLTIPLES DIMENSIONES:")
        
        select_parts = []
        group_by_parts = []
        order_by_parts = []
        where_conditions = []
        
        # PASO 1: Agregar todas las dimensiones principales
        for dimension in structure.main_dimensions:
            select_parts.append(dimension.text)
            group_by_parts.append(dimension.text)
            print(f"   📍 Dimensión agregada: {dimension.text}")
        
        # PASO 2: 🔧 BUSCAR LA MÉTRICA CORRECTA PARA EL RANKING
        ranking_metric = None
        operation_value = None
        
        # Prioridad 1: Métrica especificada en ranking_criteria
        if structure.ranking_criteria and structure.ranking_criteria.metric:
            ranking_metric = structure.ranking_criteria.metric
            print(f"   📊 Métrica del ranking: {ranking_metric.text}")
        
        # Prioridad 2: Buscar métricas reales (NO convertidas de dimensiones)
        else:
            real_metrics = [
                m for m in structure.metrics 
                if not m.linguistic_info.get('converted_from') == 'dimension'
            ]
            
            if real_metrics:
                ranking_metric = real_metrics[0]
                print(f"   📊 Métrica real encontrada: {ranking_metric.text}")
            else:
                # Fallback: usar la primera métrica disponible
                if structure.metrics:
                    ranking_metric = structure.metrics[0]
                    print(f"   📊 Métrica fallback: {ranking_metric.text}")
        
        # PASO 3: Determinar operación
        if structure.operations:
            # Buscar operación relevante (no ranking indicators)
            relevant_operations = [
                op for op in structure.operations 
                if op.value not in ['top', 'bottom'] and op.subtype != 'ranking_indicator'
            ]
            
            if relevant_operations:
                operation = relevant_operations[0]
                operation_value = operation.value
                print(f"   ⚡ Operación relevante: {operation.text} → {operation_value}")
            else:
                # Si solo hay indicadores de ranking, usar operación por defecto
                operation_value = 'suma'  # Por defecto para rankings
                print(f"   ⚡ Usando operación por defecto: suma")
        else:
            operation_value = 'suma'
            print(f"   ⚡ Sin operaciones, usando por defecto: suma")
        
        # PASO 4: Construir función de agregación
        if ranking_metric:
            if operation_value == 'máximo':
                agg_function = self._get_contextual_aggregation(structure, ranking_metric.text, operation_value)
            else:
                sql_operations = {
                    'mínimo': f'MIN({ranking_metric.text})',
                    'suma': f'SUM({ranking_metric.text})',
                    'promedio': f'AVG({ranking_metric.text})',
                    'conteo': f'COUNT({ranking_metric.text})'
                }
                agg_function = sql_operations.get(operation_value, f'SUM({ranking_metric.text})')
            
            select_parts.append(agg_function)
            
            # Determinar orden basado en ranking
            if structure.ranking_criteria:
                if structure.ranking_criteria.direction == RankingDirection.TOP:
                    order_direction = "DESC"
                else:
                    order_direction = "ASC"
            else:
                # Determinar orden basado en operación
                if operation_value in ['máximo', 'mayor']:
                    order_direction = "DESC"
                elif operation_value in ['mínimo', 'menor']:
                    order_direction = "ASC"
                else:
                    order_direction = "DESC"
            
            order_by_parts.append(f"{agg_function} {order_direction}")
            print(f"   📊 Agregación: {agg_function} {order_direction}")
        else:
            print(f"   ❌ No se encontró métrica válida para el ranking")
            return "SELECT * FROM datos;"
        
        # PASO 5: WHERE conditions
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                where_conditions.append(f"{condition.column_name} = '{condition.value}'")
        
        # PASO 6: Filtros temporales
        advanced_conditions = self.get_advanced_temporal_sql_conditions(structure)
        where_conditions.extend(advanced_conditions)
        
        # PASO 7: Construir SQL final
        sql_parts = [f"SELECT {', '.join(select_parts)}", "FROM datos"]
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        if order_by_parts:
            sql_parts.append(f"ORDER BY {', '.join(order_by_parts)}")
        
        # PASO 8: Aplicar límite
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking_value = int(structure.ranking_criteria.value)
            sql_parts.append(f"LIMIT {ranking_value}")
            print(f"   🏆 APLICANDO LIMIT de ranking: {ranking_value}")
        else:
            sql_parts.append("LIMIT 10")  # Límite por defecto más razonable
            print(f"   📍 APLICANDO LIMIT por defecto: 10")
        
        final_sql = " ".join(sql_parts) + ";"
        print(f"   🎯 SQL multi-dimensional: {final_sql}")
        
        return final_sql




    # Generar SQL con múltiples valores temporales
    def get_advanced_temporal_sql_conditions(self, structure: QueryStructure) -> List[str]:
        """Obtiene condiciones SQL avanzadas para filtros temporales - VERSIÓN MÚLTIPLES VALORES"""
        sql_conditions = []
        
        if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
            for advanced_info in self.advanced_temporal_info:
                
                # 🆕 NUEVO: Manejar múltiples valores específicos
                if (advanced_info.original_filter.filter_type == "multiple_values" and
                    advanced_info.start_value and advanced_info.end_value):
                    
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        sql_condition = f"week IN ({advanced_info.start_value}, {advanced_info.end_value})"
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        sql_condition = f"month IN ({advanced_info.start_value}, {advanced_info.end_value})"
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        sql_condition = f"day IN ({advanced_info.start_value}, {advanced_info.end_value})"
                    else:
                        continue
                    
                    sql_conditions.append(sql_condition)
                    print(f"   ✅ Condición SQL múltiples valores: {sql_condition}")
                    continue
                
                # LÓGICA EXISTENTE para otros tipos de filtros temporales
                sql_condition = advanced_info.to_sql_condition()
                if sql_condition and sql_condition != "1=1":
                    sql_conditions.append(sql_condition)
                    print(f"   ⏰ Condición SQL avanzada: {sql_condition}")
        
        return sql_conditions
    
    

    def _get_contextual_aggregation(self, structure: QueryStructure, metric_text: str, operation: str) -> str:
        """Usar intent semántico original (pre-mapeo) para decidir SUM vs MAX"""
        
        if operation == 'máximo':
            # 🎯 USAR INTENT ORIGINAL (analizado ANTES del mapeo)
            original_intent = getattr(structure, 'original_semantic_intent', 'DEFAULT')
            
            if original_intent == 'MAX':
                print(f"   🎯 INTENT ORIGINAL: MAX → MAX({metric_text}) [palabras originales singulares]")
                return f'MAX({metric_text})'
            elif original_intent == 'SUM':
                print(f"   🎯 INTENT ORIGINAL: SUM → SUM({metric_text}) [palabras originales plurales]")
                return f'SUM({metric_text})'
            else:
                print(f"   🎯 INTENT ORIGINAL: DEFAULT → SUM({metric_text}) [configuración por defecto]")
                return f'SUM({metric_text})'  # Tu configuración por defecto
        
        return f'SUM({metric_text})'   
        


        # =============================================
        # GRUPO 7: FORMATEO Y RESULTADO 
        # Formateo de salida y conversión de resultados
        # =============================================



# ------  "Generador de estructura jerarquica" -------

    def generate_hierarchical_structure(self, structure: QueryStructure) -> str:
        """🔧 Generador de Estructura Jerárquica - VERSIÓN MULTI-CRITERIO"""
        
        # CASO ESPECIAL: Rankings - VERSIÓN MULTI-CRITERIO
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking = structure.ranking_criteria
            direction_text = "top" if ranking.direction == RankingDirection.TOP else "worst"
            
            if ranking.unit == RankingUnit.COUNT:
                result = f"{direction_text} {int(ranking.value)} ({structure.main_dimension.text})"
            else:  # PERCENTAGE
                result = f"{direction_text} {ranking.value}% ({structure.main_dimension.text})"
            
            # 🔧 NUEVA LÓGICA: Incluir múltiples criterios
            if len(structure.metrics) > 1:
                operations_available = [op.text.lower() for op in structure.operations if op.text.lower() in ['mas', 'más', 'mayor', 'menor']]
                metrics_available = [m.text for m in structure.metrics]
                
                criteria_parts = []
                for i, metric in enumerate(metrics_available):
                    if i < len(operations_available):
                        op = operations_available[i]
                    else:
                        op = operations_available[0] if operations_available else 'mas'
                    
                    criteria_parts.append(f"({op} {metric})")
                
                # Combinar con " y "
                combined_criteria = " y ".join(criteria_parts)
                result += f" por {combined_criteria}"
                
            else:
                # LÓGICA ORIGINAL: Un criterio
                result += f" por ({ranking.metric.text})"
            
            # NUEVA LÓGICA: Agregar filtros temporales avanzados
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                result += f" {temporal_description}"
            
            # NUEVA LÓGICA: Agregar filtros de columna si existen
            if structure.column_conditions:
                filter_parts = []
                for condition in structure.column_conditions:
                    filter_parts.append(f"con {condition.column_name} = '{condition.value}'")
                
                if filter_parts:
                    result += f" {' y '.join(filter_parts)}"
            
            # NUEVA LÓGICA: Agregar exclusiones si existen
            if structure.exclusion_filters:
                exclusion_parts = []
                for exclusion in structure.exclusion_filters:
                    exclusion_parts.append(f"excluyendo {exclusion.column_name} = '{exclusion.value}'")
                
                if exclusion_parts:
                    result += f" {' y '.join(exclusion_parts)}"
            
            print(f"   🏆 Resultado ranking completo: {result}")
            return result
        
        # RESTO DE LA LÓGICA ORIGINAL PARA CONSULTAS NO-RANKING
        parts = []
        
        # PASO 1: Identificar columnas temporales
        temporal_columns = set()
        for tf in structure.temporal_filters:
            if tf.unit == TemporalUnit.WEEKS:
                temporal_columns.add('semana')
                temporal_columns.add('week')
            elif tf.unit == TemporalUnit.MONTHS:
                temporal_columns.add('mes')
                temporal_columns.add('month')
            elif tf.unit == TemporalUnit.DAYS:
                temporal_columns.add('dia')
                temporal_columns.add('day')
            elif tf.unit == TemporalUnit.YEARS:
                temporal_columns.add('año')
                temporal_columns.add('year')
        
        print(f"🔍 Generando estructura jerárquica para consulta compuesta:")
        print(f"   📍 Dimensión: {structure.main_dimension.text if structure.main_dimension else 'N/A'}")
        print(f"   🔗 Es compuesta: {structure.is_compound_query}")
        print(f"   🔗 Criterios compuestos: {len(structure.compound_criteria)}")
        print(f"   ⏰ Columnas temporales: {temporal_columns}")
        
        # PASO 2: Verificar si dimensión está en filtros
        dimension_in_filter = False
        if structure.main_dimension and structure.column_conditions:
            main_dim_name = structure.main_dimension.text
            for condition in structure.column_conditions:
                if condition.column_name == main_dim_name:
                    dimension_in_filter = True
                    break
        
        print(f"   🔄 ¿Dimensión en filtros? {dimension_in_filter}")
        
        # PASO 3: FILTRAR condiciones temporales duplicadas
        non_temporal_conditions = []
        for condition in structure.column_conditions:
            if condition.column_name not in temporal_columns:
                non_temporal_conditions.append(condition)
                print(f"   ✅ Conservando filtro: {condition.column_name} = {condition.value}")
            else:
                print(f"   ⏰ EXCLUYENDO filtro temporal duplicado: {condition.column_name} = {condition.value}")
        
        # PASO 4: Construir dimensión principal
        if structure.main_dimension and not dimension_in_filter:
            main_part = f"({structure.main_dimension.text})"
            
            # CRÍTICO: Solo agregar filtros NO temporales
            if non_temporal_conditions:
                conditions = []
                for condition in non_temporal_conditions:
                    conditions.append(f"({condition.column_name} = '{condition.value}')")
                main_part += f" con {' y '.join(conditions)}"
            
            parts.append(main_part)
            print(f"   ✅ Parte principal: {main_part}")
        
        # PASO 5: Filtros directos (solo NO temporales)
        elif non_temporal_conditions:
            filter_parts = []
            for condition in non_temporal_conditions:
                filter_parts.append(f"({condition.column_name} = '{condition.value}')")
            
            if len(filter_parts) == 1:
                parts.append(filter_parts[0])
            else:
                parts.append(f"({' Y '.join(filter_parts)})")
            
            print(f"   ✅ Filtros directos (no temporales): {filter_parts}")
        
        # PASO 6 NUEVA LÓGICA: Operación y métrica COMPUESTA
        if structure.is_compound_query and structure.compound_criteria:
            print(f"🔗 PROCESANDO ESTRUCTURA JERÁRQUICA COMPUESTA:")
            
            # Construir cada criterio como ((operación) (métrica))
            criteria_parts = []
            for i, criteria in enumerate(structure.compound_criteria):
                criteria_part = f"(({criteria.operation.text}) ({criteria.metric.text}))"
                criteria_parts.append(criteria_part)
                print(f"   {i+1}. Criterio: {criteria_part}")
            
            # Unir criterios con " y "
            if len(criteria_parts) == 1:
                operation_part = criteria_parts[0]
            else:
                operation_part = " y ".join(criteria_parts)
            
            # NUEVA LÓGICA: Agregar información temporal avanzada para compuestas
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                operation_part += f" {temporal_description}"
            
            parts.append(operation_part)
            print(f"   ✅ Operación compuesta: {operation_part}")
        
        # PASO 6 LÓGICA TRADICIONAL: Para consultas NO compuestas
        elif structure.operations and structure.metrics:
            op = structure.operations[0]
            metric = structure.metrics[0]
            operation_part = f"(({op.text}) ({metric.text}))"
            
            # NUEVA LÓGICA: Agregar información temporal avanzada
            temporal_description = self.generate_hierarchical_structure_temporal_description(structure)
            if temporal_description:
                operation_part += f" {temporal_description}"
            
            parts.append(operation_part)
            print(f"   ✅ Operación+Métrica tradicional: {operation_part}")
        
        elif structure.operations:
            op = structure.operations[0]
            parts.append(f"({op.text})")
            print(f"   ✅ Solo operación: ({op.text})")
            
        elif structure.metrics:
            # 🔧 Solo agregar métricas que NO están en filtros
            metrics_not_in_filters = []
            for metric in structure.metrics:
                used_in_filter = any(
                    cvp.column_name == metric.text 
                    for cvp in structure.column_conditions
                )
                if not used_in_filter:
                    metrics_not_in_filters.append(metric)
            
            if metrics_not_in_filters:
                metric = metrics_not_in_filters[0]
                parts.append(f"({metric.text})")
        
        # PASO 7: Combinar partes con lógica correcta
        if len(parts) == 1:
            result = parts[0]
        elif len(parts) == 2:
            # Verificar si TODAS las condiciones son temporales
            all_conditions_are_temporal = all(
                condition.column_name in temporal_columns 
                for condition in structure.column_conditions
            )
            
            if all_conditions_are_temporal and structure.main_dimension:
                # Caso: dimensión + operación temporal (sin filtros adicionales)
                result = f"{parts[0]} con {parts[1]}"
                print(f"   🔧 Combinación especial (dimensión con operación temporal): {result}")
            else:
                # Caso: múltiples condiciones independientes
                result = f"{' Y '.join(parts)}"
                print(f"   🔧 Combinación estándar (múltiples condiciones): {result}")
        elif len(parts) > 2:
            result = f"{' Y '.join(parts)}"
        else:
            result = "estructura_incompleta"
        
        print(f"   🎯 Resultado final COMPUESTO: {result}")
        return result



    def generate_hierarchical_structure_temporal_description(self, structure: QueryStructure) -> str:
        """Genera descripción temporal avanzada para estructura jerárquica"""
        temporal_parts = []
        
        # NUEVA LÓGICA: Usar información temporal avanzada si está disponible
        if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
            for advanced_info in self.advanced_temporal_info:
                if advanced_info.is_range_from:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"desde semana {advanced_info.start_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"desde mes {advanced_info.start_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"desde día {advanced_info.start_value}")
                elif advanced_info.is_range_between:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"de semana {advanced_info.start_value} a {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"de mes {advanced_info.start_value} a {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"de día {advanced_info.start_value} a {advanced_info.end_value}")
                elif advanced_info.is_range_to:
                    if advanced_info.original_filter.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"hasta semana {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"hasta mes {advanced_info.end_value}")
                    elif advanced_info.original_filter.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"hasta día {advanced_info.end_value}")
                else:
                    # Filtros tradicionales existentes
                    tf = advanced_info.original_filter
                    if tf.filter_type == "specific":
                        if tf.unit == TemporalUnit.WEEKS:
                            temporal_parts.append(f"en semana {tf.quantity}")
                        elif tf.unit == TemporalUnit.MONTHS:
                            temporal_parts.append(f"en mes {tf.quantity}")
                        elif tf.unit == TemporalUnit.DAYS:
                            temporal_parts.append(f"en día {tf.quantity}")
                    else:
                        temporal_parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
        else:
            # FALLBACK: Usar filtros temporales tradicionales (para compatibilidad)
            for tf in structure.temporal_filters:
                if tf.filter_type == "specific":
                    if tf.unit == TemporalUnit.WEEKS:
                        temporal_parts.append(f"en semana {tf.quantity}")
                    elif tf.unit == TemporalUnit.MONTHS:
                        temporal_parts.append(f"en mes {tf.quantity}")
                    elif tf.unit == TemporalUnit.DAYS:
                        temporal_parts.append(f"en día {tf.quantity}")
                else:
                    temporal_parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
        
        return ' y '.join(temporal_parts) if temporal_parts else ""

# ------  "Debugger de estructura jerarquica" -------

    def debug_hierarchical_structure(self, structure: QueryStructure) -> Dict:
        """Debugger de Estructura Jerárquica"""
        debug_info = {
            'main_dimension': structure.main_dimension.text if structure.main_dimension else None,
            'column_conditions': [f"{cvp.column_name} = '{cvp.value}'" for cvp in structure.column_conditions],
            'operations': [op.text for op in structure.operations],
            'metrics': [m.text for m in structure.metrics],
            'temporal_filters': [f"{tf.indicator} {tf.quantity} {tf.unit.value}" for tf in structure.temporal_filters]
        }
        
        # Verificar si dimensión está en filtros
        dimension_in_filter = False
        if structure.main_dimension and structure.column_conditions:
            main_dim_name = structure.main_dimension.text
            for condition in structure.column_conditions:
                if condition.column_name == main_dim_name:
                    dimension_in_filter = True
                    break
        
        debug_info['dimension_in_filter'] = dimension_in_filter
        
        # Construir paso a paso
        construction_steps = []
        
        if structure.main_dimension and not dimension_in_filter:
            construction_steps.append(f"PASO 1: Dimensión principal → ({structure.main_dimension.text})")
            if structure.column_conditions:
                conditions = [f"({cvp.column_name} = '{cvp.value}')" for cvp in structure.column_conditions]
                construction_steps.append(f"PASO 2: Agregar filtros → con {' y '.join(conditions)}")
        elif structure.column_conditions:
            filters = [f"({cvp.column_name} = '{cvp.value}')" for cvp in structure.column_conditions]
            construction_steps.append(f"PASO 1: Filtros directos → {' Y '.join(filters)}")
        
        if structure.operations and structure.metrics:
            op = structure.operations[0]
            metric = structure.metrics[0]
            construction_steps.append(f"PASO FINAL: Operación + Métrica → (({op.text}) ({metric.text}))")
        
        debug_info['construction_steps'] = construction_steps
        
        return debug_info


# ------  "Generador de interpretacion natural" -------

    def generate_natural_interpretation(self, structure: QueryStructure) -> str:
        """🔧 Generador de Interpretación Natural - VERSIÓN MULTI-CRITERIO"""
        
        # CASO ESPECIAL: Rankings - LÓGICA MULTI-CRITERIO CORREGIDA
        if structure.is_ranking_query and structure.ranking_criteria:
            ranking = structure.ranking_criteria
            parts = []
            
            # Construir interpretación específica para rankings
            direction_text = "los mejores" if ranking.direction == RankingDirection.TOP else "los peores"
            
            if ranking.unit == RankingUnit.COUNT:
                parts.append(f"Encontrar {direction_text} {int(ranking.value)} {structure.main_dimension.text}")
            else:  # PERCENTAGE
                parts.append(f"Encontrar {direction_text} {ranking.value}% de {structure.main_dimension.text}")
            
            # 🔧 NUEVA LÓGICA: Describir TODOS los criterios
            if len(structure.metrics) > 1:
                print(f"   🔗 Generando interpretación multi-criterio")
                
                # Obtener operaciones disponibles
                operations_available = [op.text.lower() for op in structure.operations if op.text.lower() in ['mas', 'más', 'mayor', 'menor']]
                metrics_available = [m.text for m in structure.metrics]
                
                criteria_descriptions = []
                for i, metric in enumerate(metrics_available):
                    if i < len(operations_available):
                        op = operations_available[i]
                    else:
                        op = operations_available[0] if operations_available else 'mas'
                    
                    if op in ['mas', 'más', 'mayor']:
                        criteria_descriptions.append(f"mayor {metric}")
                    elif op in ['menor', 'minimo', 'mínimo']:
                        criteria_descriptions.append(f"menor {metric}")
                    else:
                        criteria_descriptions.append(f"{op} {metric}")
                
                # Combinar criterios con "y"
                if len(criteria_descriptions) == 2:
                    combined_criteria = f" y ".join(criteria_descriptions)
                else:
                    combined_criteria = ", ".join(criteria_descriptions[:-1]) + f" y {criteria_descriptions[-1]}"
                
                parts.append(f"basado en {combined_criteria}")
                
            else:
                # LÓGICA ORIGINAL: Un solo criterio
                if ranking.metric:
                    if ranking.operation and ranking.operation.text.lower() in ['mas', 'más', 'mayor']:
                        parts.append(f"con mayor volumen total de {ranking.metric.text}")
                    elif ranking.operation and ranking.operation.text.lower() in ['menos', 'menor']:
                        parts.append(f"con menor volumen total de {ranking.metric.text}")
                    else:
                        parts.append(f"basado en {ranking.metric.text}")
            
            # Agregar filtros temporales (lógica existente)
            if structure.temporal_filters:
                for tf in structure.temporal_filters:
                    if tf.filter_type == "range_between":
                        # Usar información temporal avanzada si está disponible
                        if hasattr(self, 'advanced_temporal_info') and self.advanced_temporal_info:
                            for advanced_info in self.advanced_temporal_info:
                                if advanced_info.is_range_between:
                                    if tf.unit == TemporalUnit.WEEKS:
                                        parts.append(f"entre semana {advanced_info.start_value} y {advanced_info.end_value}")
                                    elif tf.unit == TemporalUnit.MONTHS:
                                        parts.append(f"entre mes {advanced_info.start_value} y {advanced_info.end_value}")
                        else:
                            parts.append(f"en rango temporal")
                    elif tf.filter_type == "specific":
                        if tf.unit == TemporalUnit.WEEKS:
                            parts.append(f"en la semana número {tf.quantity}")
                        elif tf.unit == TemporalUnit.MONTHS:
                            parts.append(f"en el mes número {tf.quantity}")
                    else:
                        parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
            
            # Agregar otros filtros (lógica existente)
            if structure.column_conditions:
                conditions = []
                for condition in structure.column_conditions:
                    conditions.append(f"donde {condition.column_name} = '{condition.value}'")
                parts.extend(conditions)
            
            interpretation = ", ".join(parts)
            return interpretation.capitalize() if interpretation else "Consulta de ranking sin interpretación clara"
        
        
        # LÓGICA ORIGINAL PARA CONSULTAS NO-RANKING
        parts = []
        
        # Parte principal
        if structure.main_dimension:
            parts.append(f"Encontrar {structure.main_dimension.text}")
        
        # Condiciones de columna
        if structure.column_conditions:
            conditions = []
            for condition in structure.column_conditions:
                conditions.append(f"donde {condition.column_name} = '{condition.value}'")
            parts.append(", ".join(conditions))
        
        # Operación y métrica
        if structure.operations and structure.metrics:
            operation = structure.operations[0]
            metric = structure.metrics[0]
            
            if operation.value == 'máximo':
                parts.append(f"con el mayor valor en {metric.text}")
            elif operation.value == 'mínimo':
                parts.append(f"con el menor valor en {metric.text}")
            else:
                parts.append(f"calculando {operation.value} de {metric.text}")
        elif structure.operations:
            operation = structure.operations[0]
            parts.append(f"con {operation.value}")
        elif structure.metrics:
            metric = structure.metrics[0]
            parts.append(f"relacionado con {metric.text}")
        
        # Filtros temporales
        if structure.temporal_filters:
            for tf in structure.temporal_filters:
                if tf.filter_type == "specific":
                    if tf.unit == TemporalUnit.WEEKS:
                        parts.append(f"en la semana número {tf.quantity}")
                    elif tf.unit == TemporalUnit.MONTHS:
                        parts.append(f"en el mes número {tf.quantity}")
                    elif tf.unit == TemporalUnit.DAYS:
                        parts.append(f"en el día número {tf.quantity}")
                else:
                    parts.append(f"en las {tf.indicator} {tf.quantity} {tf.unit.value}")
        
        interpretation = ", ".join(parts)
        return interpretation.capitalize() if interpretation else "Consulta sin interpretación clara"


# ------  "Convertidor de estructura a diccionario" -------

    def structure_to_dict(self, structure: QueryStructure) -> Dict:
        """Convertidor de Estructura a Diccionario"""
        return {
            'main_dimension': self.component_to_dict(structure.main_dimension) if structure.main_dimension else None,
            'operations': [self.component_to_dict(op) for op in structure.operations],
            'metrics': [self.component_to_dict(m) for m in structure.metrics],
            'column_conditions': [self.cvp_to_dict(cvp) for cvp in structure.column_conditions],
            'temporal_filters': [self.temporal_to_dict(tf) for tf in structure.temporal_filters],
            'values': [self.component_to_dict(v) for v in structure.values],
            'connectors': [self.component_to_dict(c) for c in structure.connectors],
            'unknown_tokens': [self.component_to_dict(u) for u in structure.unknown_tokens],
            'complexity_level': structure.get_complexity_level()
        }


# ------  "Convertidor de componente a diccionario" -------

    def component_to_dict(self, component: QueryComponent) -> Dict:
        """Convertidor de Componente a Diccionario"""
        if not component:
            return None
        
        return {
            'text': component.text,
            'type': component.type.value,
            'confidence': component.confidence,
            'subtype': component.subtype,
            'value': component.value,
            'column_name': component.column_name,
            'linguistic_info': component.linguistic_info
        }


# ------  "Convertidor de par columna-valor" -------

    def cvp_to_dict(self, cvp: ColumnValuePair) -> Dict:
        """Convertidor de Par Columna-Valor"""
        return {
            'column_name': cvp.column_name,
            'value': cvp.value,
            'confidence': cvp.confidence,
            'raw_text': cvp.raw_text
        }


# ------  "Convertidor de filtro temporal" -------

    def temporal_to_dict(self, tf: TemporalFilter) -> Dict:
        """Convertidor de Filtro Temporal"""
        return {
            'indicator': tf.indicator,
            'quantity': tf.quantity,
            'unit': tf.unit.value,
            'confidence': tf.confidence,
            'filter_type': tf.filter_type
        }


# ------  "Inferidor de dimension por defecto" -------

    def infer_default_dimension_for_ranking(self, ranking_criteria: RankingCriteria) -> Optional[QueryComponent]:
        """Inferidor de Dimensión por Defecto"""
        # Dimensiones comunes por métrica
        metric_to_dimension = {
            'ventas': 'account',
            'venta': 'account', 
            'inventario': 'product',
            'margen': 'product',
            'revenue': 'account',
            'sales': 'account'
        }
        
        if ranking_criteria and ranking_criteria.metric:
            metric_text = ranking_criteria.metric.text.lower()
            if metric_text in metric_to_dimension:
                inferred_dim = metric_to_dimension[metric_text]
                
                return QueryComponent(
                    text=inferred_dim,
                    type=ComponentType.DIMENSION,
                    confidence=0.75,  # Confianza media por ser inferida
                    subtype='inferred',
                    linguistic_info={'source': 'inferred_for_ranking'}
                )
        
        return None




        # ========================================
        # GRUPO 8: INTERFAZ DE USUARIO 
        # Interfaz de usuario y sesión interactiva
        # ========================================



# ------  "Mostrador de resultados unificados" -------

    def display_unified_result(self, result: Dict):
        """Mostrar resultado unificado con información de schema mapping"""
        
        if result['success']:
            print("✅ CONSULTA PROCESADA EXITOSAMENTE")
            print("="*80)
            print(f"📝 Input Original: '{result['original_input']}'")
            print(f"🔄 Consulta Normalizada: '{result['normalized_query']}'")
            print(f"⭐ Confianza General: {result['confidence']}")
            print(f"📊 Complejidad: {result['complexity_level'].upper()}")
            
    # 🆕 MOSTRAR AMBOS SQLS
            if 'conceptual_sql' in result:
                print(f"\n🔧 SQL CONCEPTUAL:")
                print(f"   {result['conceptual_sql']}")
            
            print(f"\n🗄️ SQL NORMALIZADO:")
            print(f"   {result['sql_query']}")
            
    # 🆕 MOSTRAR ESTADÍSTICAS DE MAPEO
            if 'schema_mapping_stats' in result:
                stats = result['schema_mapping_stats']
                print(f"\n📊 SCHEMA MAPPING STATS:")
                print(f"   📍 Dimension anchors: {stats['total_dimension_anchors']}")
                print(f"   📈 Metric anchors: {stats['total_metric_anchors']}")
                print(f"   🔄 Total mappings: {stats['total_reverse_mappings']}")
            if not result.get('success', False):
                print("\n❌ ERROR EN LA CONSULTA")
                print("="*70)
                print(f"📝 Input: '{result.get('original_input', 'N/A')}'")
                print(f"❌ Error: {result.get('error', 'Error desconocido')}")
                
    # 🆕 MOSTRAR INFORMACIÓN DE PALABRAS DESCONOCIDAS
                if result.get('error_type') == 'unknown_words':
                    self._display_unknown_words_error(result)
                
                elif result.get('suggestions'):
                    print("\n💡 SUGERENCIAS:")
                    for i, suggestion in enumerate(result['suggestions'], 1):
                        print(f"  {i}. {suggestion}")
                
                return
            
    # 🆕 MOSTRAR ADVERTENCIAS DE PALABRAS SOSPECHOSAS
            if result.get('unknown_words_detected', 0) > 0:
                print(f"\n⚠️ ADVERTENCIA: {result['unknown_words_detected']} palabras con baja confianza detectadas")
            
            # TU CÓDIGO EXISTENTE PARA MOSTRAR RESULTADOS EXITOSOS...
            print("\n✅ CONSULTA PROCESADA EXITOSAMENTE")
            print("="*80)
            print(f"📝 Input Original: '{result['original_input']}'")
            print(f"🔄 Consulta Normalizada: '{result['normalized_query']}'")
            print(f"⭐ Confianza General: {result.get('confidence', 0):.2f}")
            print(f"📊 Complejidad: {result.get('complexity_level', 'desconocida').upper()}")
            
    # ESTRUCTURA JERÁRQUICA
            print(f"\n🏗️  ESTRUCTURA JERÁRQUICA:")
            print(f"   {result.get('hierarchical_structure', 'N/A')}")
            
    # DESGLOSE DETALLADO DE COMPONENTES
            self.show_detailed_component_breakdown(result)
            
    # SQL Y INTERPRETACIÓN
            print(f"\n🗄️  SQL GENERADO:")
            print(f"   {result.get('sql_query', 'N/A')}")
            
            print(f"\n💡 INTERPRETACIÓN NATURAL:")
            print(f"   {result.get('interpretation', 'N/A')}")
            
            print("="*80)
        
    
    
# ------  "Mostrador de palabras desconocidas encontradas"--------
    
    def _display_unknown_words_error(self, result: Dict):
        """Mostrar detalles de error por palabras desconocidas"""
        feedback = result.get('unknown_words_feedback', {})
        
        print(f"\n🚨 PALABRAS NO RECONOCIDAS DETECTADAS:")
        print(f"   📊 Total: {result.get('unknown_words_count', 0)}")
        
        if feedback.get('unknown_words'):
            print(f"\n📋 DETALLES:")
            for word_info in feedback['unknown_words']:
                severity_icon = "🚨" if word_info['severity'] == 'critical' else "⚠️"
                print(f"   {severity_icon} '{word_info['word']}' en posición {word_info['position']}")
                print(f"      Contexto: {word_info['context']}")
                print(f"      Confianza: {word_info['confidence']:.2f}")
        
        if feedback.get('similar_words'):
            print(f"\n💡 PALABRAS SIMILARES ENCONTRADAS:")
            for similar in feedback['similar_words']:
                print(f"   🔄 '{similar['original']}' → ¿'{similar['suggested']}'?")
        
        print(f"\n💡 SUGERENCIAS:")
        for suggestion in feedback.get('suggestions', []):
            print(f"   • {suggestion}")


# ------  "Mostrador de estadisticas de palabras desconocidas" -----

    def show_unknown_words_statistics(self):
        """📊 MOSTRAR ESTADÍSTICAS DE PALABRAS DESCONOCIDAS"""
        stats = self.unknown_words_log['statistics']
        
        print(f"\n📊 ESTADÍSTICAS DE PALABRAS DESCONOCIDAS")
        print("="*60)
        print(f"📈 Total consultas fallidas: {stats.get('total_failures', 0)}")
        print(f"📋 Consultas registradas: {len(self.unknown_words_log['failures'])}")
        
        common_words = stats.get('most_common_unknown_words', {})
        if common_words:
            print(f"\n🔝 TOP PALABRAS DESCONOCIDAS:")
            sorted_words = sorted(common_words.items(), key=lambda x: x[1]['count'], reverse=True)
            for i, (word, info) in enumerate(sorted_words[:10], 1):
                print(f"  {i:2d}. '{word}' → {info['count']} veces")
        
        print("="*60)


# ------  "Mostrador de desglose detallado" -------

    def show_detailed_component_breakdown(self, result: Dict):
        """Mostrador de Desglose Detallado"""
        print(f"\n🔍 DESGLOSE DETALLADO DE COMPONENTES:")
        print("-" * 60)
        
        structure = result.get('query_structure', {})
        
        # Dimensión principal
        main_dim = structure.get('main_dimension')
        if main_dim:
            print(f"\n🎯 DIMENSIÓN PRINCIPAL:")
            print(f"  ✅ '{main_dim['text']}' → {main_dim['type']} (confianza: {main_dim['confidence']:.2f})")
        
        # Operaciones
        operations = structure.get('operations', [])
        if operations:
            print(f"\n⚡ OPERACIONES ({len(operations)}):")
            for op in operations:
                print(f"  ✅ '{op['text']}' → {op['value']} (confianza: {op['confidence']:.2f})")
        
        # Métricas
        metrics = structure.get('metrics', [])
        if metrics:
            print(f"\n📊 MÉTRICAS ({len(metrics)}):")
            for metric in metrics:
                print(f"  ✅ '{metric['text']}' → medida a analizar (confianza: {metric['confidence']:.2f})")
        
        # Condiciones de columna
        column_conditions = structure.get('column_conditions', [])
        if column_conditions:
            print(f"\n🎛️  CONDICIONES DE COLUMNA ({len(column_conditions)}):")
            for condition in column_conditions:
                print(f"  ✅ '{condition['raw_text']}' → WHERE {condition['column_name']} = '{condition['value']}' (confianza: {condition['confidence']:.2f})")
        
        # Filtros temporales
        temporal_filters = structure.get('temporal_filters', [])
        if temporal_filters:
            print(f"\n⏰ FILTROS TEMPORALES ({len(temporal_filters)}):")
            for tf in temporal_filters:
                filter_type_desc = "específico" if tf['filter_type'] == "specific" else "rango"
                print(f"  ✅ '{tf['indicator']} {tf['quantity']} {tf['unit']}' → {filter_type_desc} (confianza: {tf['confidence']:.2f})")
        
        # Tokens no reconocidos
        unknown = structure.get('unknown_tokens', [])
        if unknown:
            print(f"\n❓ TOKENS NO RECONOCIDOS ({len(unknown)}):")
            for token in unknown:
                print(f"  ⚠️  '{token['text']}' (confianza: {token['confidence']:.2f})")


# ------  "Mostrador de estadisticas de sesion" -------

    def show_session_stats(self):
        """Mostrador de Estadísticas de Sesión"""
        print("\n📊 ESTADÍSTICAS DE LA SESIÓN")
        print("="*50)
        
        duration = datetime.now() - self.session_stats['session_start']
        success_rate = 0
        if self.session_stats['total_queries'] > 0:
            success_rate = (self.session_stats['successful_queries'] / self.session_stats['total_queries']) * 100
        
        print(f"⏱️  Duración: {duration}")
        print(f"📈 Total consultas: {self.session_stats['total_queries']}")
        print(f"✅ Exitosas: {self.session_stats['successful_queries']}")
        print(f"❌ Fallidas: {self.session_stats['failed_queries']}")
        print(f"🎯 Tasa de éxito: {success_rate:.1f}%")
        print(f"📝 Consultas simples: {self.session_stats['simple_queries']}")
        print(f"🔧 Consultas complejas: {self.session_stats['complex_queries']}")
        
        # Información adicional si hay historial
        if self.query_history:
            print(f"\n📋 HISTORIAL RECIENTE:")
            recent_queries = self.query_history[-5:]  # Últimas 5 consultas
            for i, entry in enumerate(recent_queries, 1):
                status = "✅" if entry.get('processed', False) else "❌"
                print(f"  {i}. {status} [{entry['timestamp']}] '{entry['input'][:50]}{'...' if len(entry['input']) > 50 else ''}'")
        
        print("="*50)


# ------  "Ejecutor de sesion interactiva" -------

    def run_interactive_session(self):
        """Ejecutor de Sesión Interactiva - VERSIÓN MEJORADA"""
        print("\n🤖 PARSER NLP UNIFICADO - SESIÓN INTERACTIVA")
        print("="*60)
        print("✅ Sistema de detección de palabras desconocidas ACTIVADO")
        print("🚨 Las consultas problemáticas se detendrán automáticamente")
        print("="*60)
        
        while True:
            try:
                print(f"\n[Consultas: {self.session_stats['total_queries']}] ", end="")
                user_input = input("🔍 Ingresa tu consulta: ").strip()
                
                if not user_input:
                    continue
                
                command = user_input.lower()
                
                # COMANDOS ESPECIALES EXISTENTES...
                if command in ['salir', 'exit', 'quit']:
                    print("\n👋 ¡Gracias por usar el Parser NLP Unificado!")
                    self.show_session_stats()
                    self.show_unknown_words_statistics()  # 🆕 MOSTRAR ESTADÍSTICAS ADICIONALES
                    break
                
                elif command in ['unknown', 'desconocidas', 'stats_unknown']:
                    self.show_unknown_words_statistics()
                    continue
            
                
                elif command in ['ayuda', 'help']:
                    self.show_help()
                    continue
                
                elif command in ['stats', 'estadisticas']:
                    self.show_session_stats()
                    continue
                
                elif command in ['diccionarios', 'dict']:
                    try:
                        self.dictionaries.show_dictionary_info()
                    except AttributeError:
                        print("📚 Información de diccionarios no disponible")
                    continue
                
                elif command in ['historial', 'history']:
                    self._show_query_history()
                    continue
                
                elif command in ['limpiar', 'clear']:
                    self._clear_session()
                    continue
                
                elif command in ['test', 'prueba']:
                    self._run_test_queries()
                    continue
                
                # PROCESAR CONSULTA NORMAL
                print("\n🔍 Procesando consulta unificada...")
                result = self.process_user_input(user_input)
                self.display_unified_result(result)
                
            except KeyboardInterrupt:
                print("\n\n👋 Sesión interrumpida por el usuario")
                break
            except Exception as e:
                print(f"\n❌ Error inesperado: {e}")


# ------  "Ayuda del sistema" -------

    def show_help(self):
        """Mostrador de Ayuda del Sistema"""
        print("\n🤖 PARSER NLP UNIFICADO - AYUDA")
        print("="*50)
        print("Procesamiento automático de consultas simples y complejas")
        
        print("\n📋 COMANDOS DISPONIBLES:")
        print("  • Escribe cualquier consulta en lenguaje natural")
        print("  • 'stats' - Ver estadísticas de la sesión")
        print("  • 'diccionarios' - Ver información de diccionarios")
        print("  • 'historial' - Ver historial de consultas")
        print("  • 'limpiar' - Limpiar sesión y estadísticas")
        print("  • 'test' - Ejecutar consultas de prueba")
        print("  • 'ayuda' - Mostrar esta ayuda")
        print("  • 'salir' - Terminar sesión")
        
        print("\n🎯 TIPOS DE CONSULTAS SOPORTADOS:")
        print("  📝 SIMPLES: 'partner code con mayor ventas'")
        print("  🔧 COMPLEJAS: 'customer id con sell out mayor sales amount'")
        print("  📊 CON VALORES: 'product group con estado A mayor precio'")
        print("  ⏰ CON TIEMPO ESPECÍFICO: 'vendor code mayor venta semana 8'")
        print("  ⏰ CON RANGO TEMPORAL: 'account code suma ventas ultimas 3 semanas'")
        print("  🏆 RANKINGS: 'top 5 accounts por ventas'")
        print("  🔗 COMPUESTAS: 'account con mas inventario y menor venta'")
        
        print("\n✅ FRASES COMPUESTAS SOPORTADAS:")
        print("  🏷️  partner code, customer code, vendor code")
        print("  🆔 partner id, customer id, user id")
        print("  📊 sales amount, total amount, sell out")
        print("  🏢 product group, cost center, sales area")
        
        print("\n💡 EJEMPLOS DE CONSULTAS:")
        print("  🔹 'partner code con mas ventas'")
        print("  🔹 'top 5 products por sales amount'")
        print("  🔹 'account region A con mayor inventario'")
        print("  🔹 'customer con mas revenue y menor costo'")
        print("  🔹 'suma ventas ultimas 8 semanas'")
        
        print("\n🚨 CONSEJOS:")
        print("  • Usa frases compuestas (partner_code, no partner code)")
        print("  • Sé específico con operaciones (mas, mayor, suma)")
        print("  • Para rankings usa: top, mejores, primeros + número")
        print("  • Para filtros: entidad + valor (region A, estado ACTIVO)")
        
        print("="*50)


    def _show_query_history(self):
        """Mostrador de Historial de Consultas"""
        print("\n📋 HISTORIAL DE CONSULTAS")
        print("-" * 60)
        
        if not self.query_history:
            print("📝 No hay consultas en el historial")
            return
        
        for i, entry in enumerate(self.query_history, 1):
            status = "✅ EXITOSA" if entry.get('processed', False) else "❌ FALLIDA"
            print(f"\n{i}. [{entry['timestamp']}] {status}")
            print(f"   📝 Input: '{entry['input']}'")
            
            if entry.get('processed', False) and entry.get('result'):
                result = entry['result']
                print(f"   🏗️ Estructura: {result.get('hierarchical_structure', 'N/A')}")
                print(f"   📊 Complejidad: {result.get('complexity_level', 'N/A')}")
                print(f"   ⭐ Confianza: {result.get('confidence', 0):.2f}")
            elif entry.get('error'):
                print(f"   ❌ Error: {entry['error']}")
        
        print(f"\n📊 Total: {len(self.query_history)} consultas")


    def _clear_session(self):
        """Limpiador de Sesión"""
        print("\n🧹 LIMPIANDO SESIÓN...")
        
        # Reiniciar estadísticas
        self.session_stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'simple_queries': 0,
            'complex_queries': 0,
            'session_start': datetime.now()
        }
        
        # Limpiar historial
        self.query_history = []
        
        print("✅ Sesión limpiada exitosamente")
        print("📊 Estadísticas reiniciadas")
        print("📋 Historial borrado")


    def _run_test_queries(self):
        """Ejecutor de Consultas de Prueba"""
        print("\n🧪 EJECUTANDO CONSULTAS DE PRUEBA")
        print("="*50)
        
        test_queries = [
            "partner code con mas ventas",
            "top 5 accounts por revenue",
            "product group con estado A mayor precio",
            "customer con mas inventario y menor costo",
            "suma sales amount ultimas 4 semanas",
            "account region norte con mayor margen",
            "mejores 10% vendors por sell out"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n🔬 PRUEBA {i}: '{query}'")
            print("-" * 40)
            
            try:
                result = self.process_user_input(query)
                
                if result.get('success'):
                    print(f"✅ EXITOSA")
                    print(f"📊 Complejidad: {result.get('complexity_level', 'N/A')}")
                    print(f"⭐ Confianza: {result.get('confidence', 0):.2f}")
                    print(f"🗄️ SQL: {result.get('sql_query', 'N/A')}")
                else:
                    print(f"❌ FALLIDA: {result.get('error', 'Error desconocido')}")
                    
            except Exception as e:
                print(f"🚨 ERROR: {str(e)}")
        
        print(f"\n📊 PRUEBAS COMPLETADAS")
        print("="*50)




    # =============================================
    # MAPEADOR DE TOKENS CON DICCIONARIOS COMPLEJOS
    # =============================================


class SQLSchemaMapper:
    """
    Mapea SQL conceptual a SQL con nombres reales de columnas usando diccionarios anchor.
    Último paso del pipeline NLP para normalizar consultas SQL.
    """
    
    def __init__(self):
        """Inicializa el mapeador cargando diccionarios de anchor"""
        self.dimension_anchors = {}
        self.metric_anchors = {}
        self.reverse_mapping = {}  # Para búsqueda rápida: palabra → anchor
        
        # Rutas de los diccionarios anchor
        self.dimension_path = Path("diccionarios/complejos/anchors/dimension_anchors.json")
        self.metric_path = Path("diccionarios/complejos/anchors/metric_anchors.json")
        
        # Cargar diccionarios
        self._load_anchor_dictionaries()
        
        print(f"🔗 SQLSchemaMapper inicializado:")
        print(f"   📍 Dimensiones: {len(self.dimension_anchors)} anchors")
        print(f"   📊 Métricas: {len(self.metric_anchors)} anchors")
        print(f"   🔄 Mapeos reversos: {len(self.reverse_mapping)} palabras")
    
    
    def _load_anchor_dictionaries(self):
        """Carga los diccionarios de anchor desde archivos JSON"""
        try:
            # Cargar dimension anchors
            if self.dimension_path.exists():
                with open(self.dimension_path, 'r', encoding='utf-8') as f:
                    self.dimension_anchors = json.load(f)
                    print(f"✅ Cargado dimension_anchors.json: {len(self.dimension_anchors)} entradas")
            else:
                print(f"⚠️ No encontrado: {self.dimension_path}")
            
            # Cargar metric anchors
            if self.metric_path.exists():
                with open(self.metric_path, 'r', encoding='utf-8') as f:
                    self.metric_anchors = json.load(f)
                    print(f"✅ Cargado metric_anchors.json: {len(self.metric_anchors)} entradas")
            else:
                print(f"⚠️ No encontrado: {self.metric_path}")
            
            # Construir mapeo reverso para búsqueda rápida
            self._build_reverse_mapping()
            
        except Exception as e:
            print(f"❌ Error cargando diccionarios anchor: {e}")
            print("🔄 Continuando con diccionarios vacíos")
    
    
    def _build_reverse_mapping(self):
        """Construye mapeo reverso: palabra → nombre_anchor para búsqueda rápida"""
        self.reverse_mapping = {}
        
        # Procesar dimension anchors
        for anchor_name, synonyms in self.dimension_anchors.items():
            for synonym in synonyms:
                synonym_lower = synonym.lower().strip()
                if synonym_lower:
                    self.reverse_mapping[synonym_lower] = {
                        'anchor': anchor_name,
                        'type': 'dimension'
                    }
        
        # Procesar metric anchors
        for anchor_name, synonyms in self.metric_anchors.items():
            for synonym in synonyms:
                synonym_lower = synonym.lower().strip()
                if synonym_lower:
                    self.reverse_mapping[synonym_lower] = {
                        'anchor': anchor_name,
                        'type': 'metric'
                    }
        
        print(f"🔄 Mapeo reverso construido: {len(self.reverse_mapping)} palabras mapeadas")
    
    
    def normalize_sql(self, conceptual_sql: str) -> str:
        """
        🔧 NORMALIZADOR SQL CON MANEJO DE ERRORES ROBUSTO
        """
        
        print(f"🔗 NORMALIZANDO SQL (Enhanced - Robust):")
        print(f"   📥 Input: {conceptual_sql}")
        
        try:
            sql = conceptual_sql
            replacements_made = 0
            
            # PASO 1: NORMALIZAR COLUMNAS DENTRO DE FUNCIONES SQL
            import re
            
            function_pattern = r'(\w+)\s*\(\s*(DISTINCT\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\)'
            
            def replace_function_column(match):
                nonlocal replacements_made
                try:
                    function_name = match.group(1)
                    distinct_part = match.group(2) or ""
                    column_name = match.group(3)
                    
                    print(f"   🔍 Function found: {function_name}({distinct_part}{column_name})")
                    
                    normalized_column = self._find_column_mapping_anchors_only(column_name)
                    
                    if normalized_column:
                        new_function = f'{function_name}({distinct_part}{normalized_column})'
                        print(f"   🔄 Function mapping: {function_name}({distinct_part}{column_name}) → {new_function}")
                        replacements_made += 1
                        return new_function
                    else:
                        print(f"   ❓ Function column '{column_name}' no mapping found")
                        return match.group(0)
                        
                except Exception as e:
                    print(f"   ❌ Error in replace_function_column: {e}")
                    return match.group(0)
            
            # Aplicar reemplazos en funciones
            sql = re.sub(function_pattern, replace_function_column, sql, flags=re.IGNORECASE)
            
            # PASO 2: NORMALIZAR COLUMNAS INDEPENDIENTES
            standalone_columns = self._find_standalone_columns(sql)
            
            for column in standalone_columns:
                try:
                    normalized_column = self._find_column_mapping_anchors_only(column)
                    if normalized_column and column != normalized_column:
                        pattern = r'\b' + re.escape(column) + r'\b(?!\s*\))'
                        sql = re.sub(pattern, normalized_column, sql, flags=re.IGNORECASE)
                        replacements_made += 1
                        print(f"   🔄 Standalone mapping: '{column}' → {normalized_column}")
                except Exception as e:
                    print(f"   ❌ Error mapping column '{column}': {e}")
            
            # PASO 3: AGREGAR COMILLAS
            try:
                sql = self._add_quotes_to_columns_enhanced(sql)
            except Exception as e:
                print(f"   ❌ Error adding quotes: {e}")
            
            print(f"   📤 Output: {sql}")
            print(f"   📊 Reemplazos realizados: {replacements_made}")
            
            return sql
            
        except Exception as e:
            print(f"   ❌ CRITICAL ERROR in normalize_sql: {e}")
            print(f"   📋 Returning original SQL as fallback")
            return conceptual_sql


    def _find_column_mapping_anchors_only(self, column_name: str) -> Optional[str]:
        """
        🔍 BUSCADOR ROBUSTO - MANEJA DIFERENTES ESTRUCTURAS DE ANCHORS
        """
        
        column_lower = column_name.lower()
        
        print(f"      🔍 Searching for '{column_name}' in anchors...")
        
        # PASO 1: Buscar en dimension anchors
        if hasattr(self, 'dimension_anchors'):
            print(f"      📍 Checking dimension_anchors (type: {type(self.dimension_anchors)})")
            
            try:
                if isinstance(self.dimension_anchors, dict):
                    for anchor_key, anchor_data in self.dimension_anchors.items():
                        print(f"         🔍 Checking anchor_key: '{anchor_key}' (data type: {type(anchor_data)})")
                        
                        # Manejar diferentes estructuras de anchor_data
                        if isinstance(anchor_data, dict):
                            # Estructura esperada: {"normalized_name": "Store", "variants": [...]}
                            variants = anchor_data.get('variants', [])
                            normalized = anchor_data.get('normalized_name', anchor_key)
                            
                            if isinstance(variants, list):
                                if column_lower in [v.lower() for v in variants]:
                                    print(f"      ✅ Dimension match: '{column_name}' → '{normalized}' (via variants)")
                                    return normalized
                            
                            # También verificar si la clave coincide directamente
                            if anchor_key.lower() == column_lower:
                                print(f"      ✅ Dimension match: '{column_name}' → '{normalized}' (direct key)")
                                return normalized
                                
                        elif isinstance(anchor_data, list):
                            # Estructura: [variant1, variant2, ...]
                            if column_lower in [v.lower() for v in anchor_data]:
                                normalized = anchor_key.title()  # Capitalizar clave como normalized
                                print(f"      ✅ Dimension match: '{column_name}' → '{normalized}' (list structure)")
                                return normalized
                                
                        elif isinstance(anchor_data, str):
                            # Estructura: "normalized_name"
                            if anchor_key.lower() == column_lower:
                                print(f"      ✅ Dimension match: '{column_name}' → '{anchor_data}' (string structure)")
                                return anchor_data
                                
                else:
                    print(f"      ⚠️ dimension_anchors is not a dict: {type(self.dimension_anchors)}")
                    
            except Exception as e:
                print(f"      ❌ Error processing dimension_anchors: {e}")
        
        # PASO 2: Buscar en metric anchors (misma lógica robusta)
        if hasattr(self, 'metric_anchors'):
            print(f"      📊 Checking metric_anchors (type: {type(self.metric_anchors)})")
            
            try:
                if isinstance(self.metric_anchors, dict):
                    for anchor_key, anchor_data in self.metric_anchors.items():
                        print(f"         🔍 Checking metric anchor_key: '{anchor_key}' (data type: {type(anchor_data)})")
                        
                        # Manejar diferentes estructuras de anchor_data
                        if isinstance(anchor_data, dict):
                            variants = anchor_data.get('variants', [])
                            normalized = anchor_data.get('normalized_name', anchor_key)
                            
                            if isinstance(variants, list):
                                if column_lower in [v.lower() for v in variants]:
                                    print(f"      ✅ Metric match: '{column_name}' → '{normalized}' (via variants)")
                                    return normalized
                            
                            if anchor_key.lower() == column_lower:
                                print(f"      ✅ Metric match: '{column_name}' → '{normalized}' (direct key)")
                                return normalized
                                
                        elif isinstance(anchor_data, list):
                            if column_lower in [v.lower() for v in anchor_data]:
                                normalized = anchor_key.title()
                                print(f"      ✅ Metric match: '{column_name}' → '{normalized}' (list structure)")
                                return normalized
                                
                        elif isinstance(anchor_data, str):
                            if anchor_key.lower() == column_lower:
                                print(f"      ✅ Metric match: '{column_name}' → '{anchor_data}' (string structure)")
                                return anchor_data
                                
                else:
                    print(f"      ⚠️ metric_anchors is not a dict: {type(self.metric_anchors)}")
                    
            except Exception as e:
                print(f"      ❌ Error processing metric_anchors: {e}")
        
        # PASO 3: Debug - mostrar contenido de anchors para diagnóstico
        print(f"      ❌ No mapping found for '{column_name}'")
        
        # Debug info para diagnosticar estructura
        if hasattr(self, 'dimension_anchors') and self.dimension_anchors:
            print(f"      🔍 DEBUG - Sample dimension_anchors structure:")
            sample_keys = list(self.dimension_anchors.keys())[:3]  # Primeras 3 claves
            for key in sample_keys:
                print(f"         '{key}': {type(self.dimension_anchors[key])} = {self.dimension_anchors[key]}")
        
        return None


    def debug_anchors_structure(self):
        """
        🔍 MÉTODO PARA DEBUGGEAR LA ESTRUCTURA DE ANCHORS
        Llamar este método para ver cómo están estructurados tus anchors
        """
        
        print(f"\n🔍 DEBUGGING ANCHORS STRUCTURE:")
        
        if hasattr(self, 'dimension_anchors'):
            print(f"📍 DIMENSION_ANCHORS (type: {type(self.dimension_anchors)}):")
            if isinstance(self.dimension_anchors, dict):
                for i, (key, value) in enumerate(self.dimension_anchors.items()):
                    if i < 5:  # Solo mostrar primeros 5
                        print(f"   '{key}': {type(value)} = {value}")
                    elif i == 5:
                        print(f"   ... and {len(self.dimension_anchors) - 5} more")
                        break
            else:
                print(f"   ⚠️ Not a dict: {self.dimension_anchors}")
        
        if hasattr(self, 'metric_anchors'):
            print(f"📊 METRIC_ANCHORS (type: {type(self.metric_anchors)}):")
            if isinstance(self.metric_anchors, dict):
                for i, (key, value) in enumerate(self.metric_anchors.items()):
                    if i < 5:  # Solo mostrar primeros 5
                        print(f"   '{key}': {type(value)} = {value}")
                    elif i == 5:
                        print(f"   ... and {len(self.metric_anchors) - 5} more")
                        break
            else:
                print(f"   ⚠️ Not a dict: {self.metric_anchors}")


    def _find_standalone_columns(self, sql: str) -> List[str]:
        """
        🔍 ENCUENTRA COLUMNAS QUE NO ESTÁN DENTRO DE FUNCIONES
        """
        import re
        
        # Encontrar todas las palabras que podrían ser columnas
        # Excluir palabras SQL reservadas y funciones
        sql_keywords = {
            'select', 'from', 'where', 'group', 'by', 'order', 'limit',
            'and', 'or', 'not', 'in', 'exists', 'between', 'like',
            'count', 'sum', 'max', 'min', 'avg', 'distinct',
            'datos', 'desc', 'asc'
        }
        
        # Encontrar palabras alfanuméricas que no están dentro de funciones o comillas
        word_pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
        words = re.findall(word_pattern, sql)
        
        standalone_columns = []
        for word in words:
            if (word.lower() not in sql_keywords and 
                not word.startswith('"') and 
                not self._is_inside_function(word, sql)):
                standalone_columns.append(word)
        
        # Remover duplicados manteniendo orden
        seen = set()
        unique_columns = []
        for col in standalone_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        return unique_columns


    def _is_inside_function(self, word: str, sql: str) -> bool:
        """
        🔍 VERIFICA SI UNA PALABRA ESTÁ DENTRO DE UNA FUNCIÓN
        """
        import re
        
        # Buscar si la palabra está dentro de paréntesis de función
        function_pattern = r'\w+\s*\([^)]*\b' + re.escape(word) + r'\b[^)]*\)'
        return bool(re.search(function_pattern, sql, re.IGNORECASE))


    def _add_quotes_to_columns_enhanced(self, sql: str) -> str:
        """
        🔧 AGREGADOR DE COMILLAS DINÁMICO - VERSIÓN CORREGIDA
        Usa word boundaries para evitar reemplazos parciales
        """
        
        print(f"🔧 AGREGANDO COMILLAS (Dynamic - Using Anchors):")
        print(f"   📥 Input: {sql}")
        
        import re
        result = sql
        replacements_made = 0
        
        # PASO 1: Obtener todas las columnas normalizadas de los anchors
        normalized_columns = set()
        
        # Extraer de dimension_anchors
        if hasattr(self, 'dimension_anchors') and isinstance(self.dimension_anchors, dict):
            for anchor_key, anchor_data in self.dimension_anchors.items():
                if isinstance(anchor_data, dict):
                    normalized_name = anchor_data.get('normalized_name', anchor_key)
                    normalized_columns.add(normalized_name)
                elif isinstance(anchor_data, str):
                    normalized_columns.add(anchor_data)
                else:
                    normalized_columns.add(anchor_key.title())
        
        # Extraer de metric_anchors
        if hasattr(self, 'metric_anchors') and isinstance(self.metric_anchors, dict):
            for anchor_key, anchor_data in self.metric_anchors.items():
                if isinstance(anchor_data, dict):
                    normalized_name = anchor_data.get('normalized_name', anchor_key)
                    normalized_columns.add(normalized_name)
                elif isinstance(anchor_data, str):
                    normalized_columns.add(anchor_data)
                else:
                    normalized_columns.add(anchor_key.title())
        
        print(f"   📊 Normalized columns from anchors: {sorted(normalized_columns)}")
        
        # PASO 2: Ordenar columnas por longitud (más largas primero)
        # Esto evita que "Inventory" se procese antes que "Dead_Inventory"
        sorted_columns = sorted(normalized_columns, key=len, reverse=True)
        
        # PASO 3: Agregar comillas usando regex con word boundaries
        for column in sorted_columns:
            # Skip si ya tiene comillas
            if f'"{column}"' in result or f"'{column}'" in result:
                continue
            
            # Crear patrón regex que busque la columna como palabra completa
            # \b funciona con letras/números pero no con underscore al final
            # Por eso usamos lookahead/lookbehind más complejos
            pattern = r'(?<!["\w])' + re.escape(column) + r'(?!["\w])'
            
            # Buscar todas las coincidencias
            matches = list(re.finditer(pattern, result))
            
            if matches:
                # Reemplazar de atrás hacia adelante para no afectar las posiciones
                for match in reversed(matches):
                    start, end = match.span()
                    # Verificar contexto para decidir si agregar comillas
                    context_before = result[max(0, start-10):start]
                    context_after = result[end:min(len(result), end+10)]
                    
                    # No agregar comillas si ya está entre comillas
                    if '"' in context_before[-1:] or '"' in context_after[:1]:
                        continue
                    
                    # No agregar comillas si está dentro de comillas simples (valores)
                    if "'" in context_before[-1:] or "'" in context_after[:1]:
                        continue
                    
                    # Reemplazar
                    result = result[:start] + f'"{column}"' + result[end:]
                    replacements_made += 1
                    print(f"      📝 Added quotes: {column} → \"{column}\" at position {start}")
        
        # PASO 4: Verificación final - asegurar que no haya patrones rotos
        broken_pattern = r'(\w+)_"(\w+)"'
        broken_matches = re.findall(broken_pattern, result)
        
        if broken_matches:
            print(f"   ⚠️ WARNING: Found broken patterns that need fixing:")
            for match in broken_matches:
                broken = f'{match[0]}_"{match[1]}"'
                fixed = f'"{match[0]}_{match[1]}"'
                result = result.replace(broken, fixed)
                print(f"      🔧 Fixed: {broken} → {fixed}")
                replacements_made += 1
        
        print(f"   📤 Output: {result}")
        print(f"   📊 Reemplazos realizados: {replacements_made}")
        
        return result
                
        
    def extract_columns_from_sql(self, sql: str) -> List[str]:
        """
        Extrae nombres de columnas del SQL usando expresiones regulares
        Busca en SELECT, GROUP BY, ORDER BY, WHERE
        """
        columns = set()
        
        # Normalizar SQL para análisis
        sql_clean = sql.replace('\n', ' ').replace('\t', ' ')
        sql_clean = re.sub(r'\s+', ' ', sql_clean).strip()
        
# PATRÓN 1: SELECT columns (incluyendo funciones)
        # SELECT tienda, MAX(ventas), SUM(inventario) FROM...
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, sql_clean, re.IGNORECASE)
        if select_match:
            select_part = select_match.group(1)
            # Extraer columnas dentro de funciones y directas
            select_columns = self._extract_columns_from_select(select_part)
            columns.update(select_columns)
        
# PATRÓN 2: GROUP BY columns
        # GROUP BY tienda, region
        group_by_pattern = r'GROUP\s+BY\s+(.*?)(?:\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|\s*;|\s*$)'
        group_by_match = re.search(group_by_pattern, sql_clean, re.IGNORECASE)
        if group_by_match:
            group_by_part = group_by_match.group(1).strip()
            group_by_columns = [col.strip() for col in group_by_part.split(',')]
            columns.update(group_by_columns)
        
# PATRÓN 3: ORDER BY columns (incluyendo funciones)
        # ORDER BY MAX(ventas) DESC, tienda ASC
        order_by_pattern = r'ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*;|\s*$)'
        order_by_match = re.search(order_by_pattern, sql_clean, re.IGNORECASE)
        if order_by_match:
            order_by_part = order_by_match.group(1).strip()
            order_by_columns = self._extract_columns_from_order_by(order_by_part)
            columns.update(order_by_columns)
        
# PATRÓN 4: WHERE conditions
        # WHERE tienda = 'valor' AND ventas > 100
        where_pattern = r'WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s*;|\s*$)'
        where_match = re.search(where_pattern, sql_clean, re.IGNORECASE)
        if where_match:
            where_part = where_match.group(1).strip()
            where_columns = self._extract_columns_from_where(where_part)
            columns.update(where_columns)
        
        # Limpiar y filtrar columnas
        cleaned_columns = []
        for col in columns:
            col_clean = col.strip().strip(',').strip()
            if col_clean and col_clean.lower() not in ['desc', 'asc', 'and', 'or']:
                cleaned_columns.append(col_clean)
        
        return list(set(cleaned_columns))  # Eliminar duplicados
    
    
    def _extract_columns_from_select(self, select_part: str) -> Set[str]:
        """Extrae columnas de la parte SELECT, incluyendo funciones"""
        columns = set()
        
        # Dividir por comas, pero respetando paréntesis
        items = self._split_respecting_parentheses(select_part, ',')
        
        for item in items:
            item = item.strip()
            
            # Si contiene función: MAX(ventas) → extraer 'ventas'
            function_match = re.search(r'\w+\s*\(\s*([^)]+)\s*\)', item)
            if function_match:
                column_in_function = function_match.group(1)
                columns.add(column_in_function)
            else:
                # Columna simple: tienda
                if re.match(r'^\w+$', item):
                    columns.add(item)
        
        return columns
    
    
    def _extract_columns_from_order_by(self, order_by_part: str) -> Set[str]:
        """Extrae columnas de ORDER BY, incluyendo funciones"""
        columns = set()
        
        # Dividir por comas
        items = order_by_part.split(',')
        
        for item in items:
            item = item.strip()
            # Remover DESC/ASC
            item = re.sub(r'\s+(DESC|ASC)\s*$', '', item, flags=re.IGNORECASE).strip()
            
            # Si contiene función: MAX(ventas) → extraer 'ventas'
            function_match = re.search(r'\w+\s*\(\s*(\w+)\s*\)', item)
            if function_match:
                column_in_function = function_match.group(1)
                columns.add(column_in_function)
            else:
                # Columna simple
                if re.match(r'^\w+$', item):
                    columns.add(item)
        
        return columns
    
    
    def _extract_columns_from_where(self, where_part: str) -> Set[str]:
        """Extrae columnas de condiciones WHERE"""
        columns = set()
        
        # Buscar patrones: columna = valor, columna > valor, etc.
        column_patterns = [
            r'(\w+)\s*=\s*[\'"]?[\w\s]+[\'"]?',
            r'(\w+)\s*!=\s*[\'"]?[\w\s]+[\'"]?',
            r'(\w+)\s*>\s*[\d\.]+',
            r'(\w+)\s*<\s*[\d\.]+',
            r'(\w+)\s*>=\s*[\d\.]+',
            r'(\w+)\s*<=\s*[\d\.]+',
            r'(\w+)\s+BETWEEN\s+',
            r'(\w+)\s+IN\s*\(',
        ]
        
        for pattern in column_patterns:
            matches = re.findall(pattern, where_part, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    columns.add(match[0])
                else:
                    columns.add(match)
        
        return columns
    
    
    def _split_respecting_parentheses(self, text: str, delimiter: str) -> List[str]:
        """Divide texto por delimitador respetando paréntesis"""
        parts = []
        current_part = ""
        paren_count = 0
        
        for char in text:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == delimiter and paren_count == 0:
                parts.append(current_part.strip())
                current_part = ""
                continue
            
            current_part += char
        
        if current_part.strip():
            parts.append(current_part.strip())
        
        return parts
    
    
    def find_column_mapping(self, conceptual_word: str) -> Optional[str]:
        """
        Busca el mapeo de una palabra conceptual a su anchor correspondiente
        
        Args:
            conceptual_word: Palabra conceptual (ej: 'tienda', 'ventas')
            
        Returns:
            Nombre del anchor (ej: 'store_name', 'Sell-Out') o None si no se encuentra
        """
        word_lower = conceptual_word.lower().strip()
        
        # Buscar en mapeo reverso
        if word_lower in self.reverse_mapping:
            mapping = self.reverse_mapping[word_lower]
            anchor_name = mapping['anchor']
            mapping_type = mapping['type']
            
            print(f"      🎯 '{conceptual_word}' encontrado en {mapping_type}: '{anchor_name}'")
            return anchor_name
        
        # Si no se encuentra, retornar la palabra original
        print(f"      ❓ '{conceptual_word}' no encontrado en anchors")
        return None
    
    
    def _replace_column_in_sql(self, sql: str, old_column: str, new_column: str) -> str:
        """
        Reemplaza todas las ocurrencias de una columna en el SQL
        Usa regex para evitar reemplazos parciales incorrectos
        """
        # Patrón que busca la columna como palabra completa
        pattern = r'\b' + re.escape(old_column) + r'\b'
        
        # Reemplazar todas las ocurrencias
        new_sql = re.sub(pattern, new_column, sql, flags=re.IGNORECASE)
        
        return new_sql
    
    
    def get_mapping_statistics(self) -> Dict:
        """Devuelve estadísticas de los mapeos cargados"""
        return {
            'total_dimension_anchors': len(self.dimension_anchors),
            'total_metric_anchors': len(self.metric_anchors),
            'total_reverse_mappings': len(self.reverse_mapping),
            'dimension_anchors': list(self.dimension_anchors.keys()),
            'metric_anchors': list(self.metric_anchors.keys())
        }
    
    
    def debug_column_extraction(self, sql: str):
        """Método de debug para ver cómo se extraen las columnas"""
        print(f"\n🔍 DEBUG: Extracción de columnas")
        print(f"SQL: {sql}")
        
        columns = self.extract_columns_from_sql(sql)
        print(f"Columnas extraídas: {columns}")
        
        for column in columns:
            mapping = self.find_column_mapping(column)
            print(f"  {column} → {mapping if mapping else 'Sin mapeo'}")



    def add_quotes_to_all_columns(regex_columns, sql: str) -> str:
        result_sql = sql
        quoted_columns = []

        for column in regex_columns:
            if column.startswith('"') and column.endswith('"'):
                continue

            quoted = f'"{column}"'
            esc = re.escape(column)

            pattern = rf'''
                (?<![\w"'])           # no precedido por letra, número, _ o comilla
                {esc}                 # nombre completo de columna
                (?![\w"'])            # no seguido por letra, número, _ o comilla
            '''

            if re.search(pattern, result_sql, re.IGNORECASE | re.VERBOSE):
                new_sql = re.sub(pattern, quoted, result_sql, flags=re.IGNORECASE | re.VERBOSE)
                if new_sql != result_sql:
                    quoted_columns.append(f"{column} → {quoted}")
                    result_sql = new_sql

        return result_sql, quoted_columns




# ===================================================================
# -------------- ANALIZADOR SEMÁNTICO PRE-MAPEO ---------------------
# ===================================================================


class PreMappingSemanticAnalyzer:
    """
    Analiza semántica en palabras ORIGINALES del usuario
    ANTES de que se mapeen a palabras anchor
    """
    
    def __init__(self):
        # Sin diccionarios - solo reglas generales del español
        pass
    
    def analyze_original_intent(self, original_tokens: List[str]) -> str:
        """
        🎯 ANÁLISIS PRINCIPAL: Detectar intención en tokens ORIGINALES
        
        Args:
            original_tokens: Palabras exactas del usuario ANTES del mapeo
            
        Returns:
            'SUM' - para volumen total (plural context)
            'MAX' - para transacción individual (singular + magnitud)
            'DEFAULT' - usar configuración por defecto
        """
        if not original_tokens:
            return 'DEFAULT'
        
        print(f"🔍 ANÁLISIS PRE-MAPEO de tokens originales: {original_tokens}")
        
        # REGLA 1: Detectar contexto plural (indica volumen total)
        if self._has_plural_context(original_tokens):
            return 'SUM'
        
        # REGLA 2: Detectar contexto individual (indica transacción específica)
        if self._has_individual_context(original_tokens):
            return 'MAX'
        
        return 'DEFAULT'
    
    def _has_plural_context(self, tokens: List[str]) -> bool:
        """Detectar contexto plural en palabras ORIGINALES"""
        
        for token in tokens:
            # REGLA MORFOLÓGICA: Detectar plurales del español
            if self._is_spanish_plural(token):
                print(f"   📊 PLURAL ORIGINAL detectado: '{token}' → SUM")
                return True
        
        # REGLA CONTEXTUAL: Detectar cuantificadores de volumen
        volume_indicators = {'total', 'suma', 'conjunto', 'todos', 'todas', 'cantidad'}
        for token in tokens:
            if token.lower() in volume_indicators:
                print(f"   📊 CUANTIFICADOR de volumen: '{token}' → SUM")
                return True
        
        return False
    
    def _has_individual_context(self, tokens: List[str]) -> bool:
        """Detectar contexto individual en palabras ORIGINALES"""
        
        # REGLA 1: Buscar palabras de magnitud
        magnitude_words = self._find_magnitude_words(tokens)
        if magnitude_words:
            
            # REGLA 2: Verificar que haya sustantivos singulares cerca
            singular_nouns = self._find_singular_nouns(tokens)
            if singular_nouns:
                print(f"   🎯 MAGNITUD + SINGULAR: {magnitude_words} + {singular_nouns} → MAX")
                return True
            
            # REGLA 3: Si hay magnitud sin plural explícito, asumir individual
            if not self._has_explicit_plural(tokens):
                print(f"   🎯 MAGNITUD sin plural explícito: {magnitude_words} → MAX")
                return True
        
        return False
    
    def _is_spanish_plural(self, word: str) -> bool:
        """Detectar plurales usando reglas morfológicas del español"""
        if len(word) <= 2:
            return False
        
        word_lower = word.lower()
        
        # Excepciones comunes (palabras que terminan en 's' pero no son plurales)
        exceptions = {
            'mas', 'más', 'menos', 'entonces', 'además', 'antes', 'después',
            'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'análisis',
            'crisis', 'tesis', 'dosis', 'oasis'
        }
        
        if word_lower in exceptions:
            return False
        
        # REGLAS MORFOLÓGICAS DEL ESPAÑOL:
        
        # Regla 1: Terminación en -s (no acentuada en la última sílaba)
        if word_lower.endswith('s') and not word_lower.endswith('ás'):
            return True
        
        # Regla 2: Terminación en -es  
        if word_lower.endswith('es'):
            return True
        
        return False
    
    def _find_magnitude_words(self, tokens: List[str]) -> List[str]:
        """Encontrar palabras de magnitud en tokens originales"""
        magnitude_words = []
        
        for token in tokens:
            token_lower = token.lower()
            
            # DETECTAR POR MORFOLOGÍA (sufijos comunes)
            magnitude_suffixes = ['imo', 'ima', 'or', 'nde', 'to', 'ta']
            for suffix in magnitude_suffixes:
                if token_lower.endswith(suffix) and len(token) > 3:
                    magnitude_words.append(token)
                    break
            
            # DETECTAR PALABRAS ESPECÍFICAS DE MAGNITUD
            magnitude_specific = {
                'grande', 'pequeño', 'enorme', 'gigante', 'masivo',
                'alto', 'bajo', 'elevado', 'superior', 'inferior'
            }
            if token_lower in magnitude_specific:
                magnitude_words.append(token)
        
        return magnitude_words
    
    def _find_singular_nouns(self, tokens: List[str]) -> List[str]:
        """Encontrar sustantivos singulares en tokens originales"""
        singular_nouns = []
        
        for token in tokens:
            # HEURÍSTICA: Palabras que NO son plurales y podrían ser sustantivos
            if (not self._is_spanish_plural(token) and 
                len(token) > 3 and 
                token.lower() not in {'con', 'mas', 'más', 'menor', 'mayor', 'para', 'por'}):
                singular_nouns.append(token)
        
        return singular_nouns
    
    def _has_explicit_plural(self, tokens: List[str]) -> bool:
        """Verificar si hay plurales explícitos en los tokens"""
        return any(self._is_spanish_plural(token) for token in tokens)
        


        # ===============================
        # FUNCIÓN PRINCIPAL DE EJECUCIÓN 
        # ===============================f


# ------  "Funcion de ejecucion principal" -------

def main():
    """Función Principal de Ejecución"""
    try:
        parser = UnifiedNLPParser()
        
        # Prueba específica del problema
        print("🚨 PRUEBA ESPECÍFICA: partner code Y")
        print("="*50)
        
        query = "cual es el partner code Y con mas ventas"
        result = parser.process_user_input(query)
        parser.display_unified_result(result)
        
        print("\n🚀 Iniciando sesión interactiva...")
        parser.run_interactive_session()
        
    except Exception as e:
        print(f"❌ Error al inicializar: {e}")
        print("\n🔧 POSIBLES SOLUCIONES:")
        print("1. Verifica que diccionario_sinonimos_2.py esté en el mismo directorio")
        print("2. Revisa que todas las dependencias estén instaladas")
        print("3. Verifica la sintaxis del código")

if __name__ == "__main__":
    main()



