# Estrategia: Validación de Rutinas JavaScript en BigQuery

## 1. Contexto: Qué persiste bq-sync de una rutina JS

```
routines/my_func.sql              ← cuerpo JS con header
routines/models/my_func.yaml      ← metadata: arguments, return_type, language
```

**Header del .sql:**
```
-- Routine: my_func
-- Language: JAVASCRIPT

function transform(x) {
  return x * 2;
}
```

**YAML del modelo:**
```yaml
name: my_func
language: JAVASCRIPT
return_type: FLOAT64
arguments:
  - name: x  type: FLOAT64  mode: IN
```

Esto significa que tenemos disponible:
- El **cuerpo JS** puro (después de strip del header)
- Los **argumentos** y **return_type** esperados (desde el YAML)
- El **lenguaje** (desde el header)

---

## 2. Qué se puede validar de una rutina JS

| Nivel | Qué valida | Requiere |
|---|---|---|
| **L1: Syntax** | El cuerpo JS es JavaScript válido | Parser JS |
| **L2: Contrato** | Los parámetros del YAML coinciden con el uso en el JS body | Parser JS + YAML |
| **L3: Semántica BQ** | La UDF funciona end-to-end en BigQuery | BQ dry-run API |

---

## 3. Opciones de herramienta para L1 (Syntax)

### Opción A: `tree-sitter` + `tree-sitter-javascript`

```python
pip install tree-sitter tree-sitter-javascript
```

| Aspecto | Detalle |
|---|---|
| **Tipo** | Parser incremental (C core + Python bindings) |
| **Velocidad** | Extremadamente rápido (~μs por archivo) |
| **Error handling** | No falla — produce nodos `ERROR` en el CST |
| **ES version** | Soporte moderno (ES2024+, actualizado regularmente) |
| **Deps** | `tree-sitter` (C ext) + `tree-sitter-javascript` (grammar) |
| **Tamaño** | ~500 KB total |
| **API** | `parser.parse(code)` → `Tree` → walk y buscar `ERROR` nodes |

```python
from tree_sitter import Parser, Language
import tree_sitter_javascript as tsjs

LANG = Language(tsjs.language())
parser = Parser(LANG)

tree = parser.parse(b"function f(x) { return x * 2; }")
# Buscar nodos ERROR
def find_errors(node):
    errors = []
    if node.type == "ERROR":
        errors.append((node.start_point, node.end_point))
    for child in node.children:
        errors.extend(find_errors(child))
    return errors
```

**Ventaja clave:** No falla en código parcialmente válido — siempre produce un árbol, y los errores son nodos localizados con posición exacta.

### Opción B: `esprima` (Python)

```python
pip install esprima
```

| Aspecto | Detalle |
|---|---|
| **Tipo** | Parser ECMAScript completo (Python puro, port de Esprima) |
| **Velocidad** | Lento comparado con tree-sitter (~10-50x) |
| **Error handling** | Lanza `esprima.Error` con línea/columna |
| **ES version** | ES6/ES2015 (puede faltar features ES2020+) |
| **Deps** | Zero (Python puro) |
| **API** | `esprima.parseScript(code)` → AST o excepción |

```python
import esprima

try:
    esprima.parseScript(js_code)
except esprima.Error as e:
    print(f"Error: {e}")
```

**Desventaja:** Cobertura ES limitada y proyecto con poco mantenimiento reciente.

### Opción C: Subprocess Node.js (`acorn` / `eslint`)

```
node -e "require('acorn').parse(code, {ecmaVersion: 2025})"
```

| Aspecto | Detalle |
|---|---|
| **Tipo** | Parser nativo JS via subprocess |
| **Velocidad** | Lento (startup de Node.js ~100ms por invocación) |
| **Error handling** | Parse error con línea/columna |
| **ES version** | Completo y actualizado |
| **Deps** | Requiere Node.js + npm install |
| **Integración** | subprocess — frágil, requiere Node.js instalado |

**Descartado:** Agregar Node.js como dependencia de un proyecto Python es un anti-patrón.

### Opción D: BigQuery Dry-Run API

```python
from google.cloud import bigquery

client = bigquery.Client()
job_config = bigquery.QueryJobConfig(dry_run=True)
query = f"""
CREATE TEMP FUNCTION my_func(x FLOAT64)
RETURNS FLOAT64
LANGUAGE js
AS r\"\"\"
  {js_body}
\"\"\";
SELECT my_func(1.0);
"""
job = client.query(query, job_config=job_config)
# Si no lanza excepción → válido
```

| Aspecto | Detalle |
|---|---|
| **Tipo** | Validación end-to-end en BigQuery |
| **Velocidad** | Lento (~500ms-2s por query, requiere red) |
| **Cobertura** | **Completa** — verifica syntax JS, tipos BQ, argumentos |
| **Deps** | `google-cloud-bigquery` (ya existe) |
| **Requiere** | Credenciales GCP activas, proyecto BQ |

---

## 4. Matriz de decisión

| Criterio | tree-sitter (A) | esprima (B) | Node.js (C) | Dry-Run (D) |
|---|---|---|---|---|
| **Syntax check JS** | ✅ Robusto | ✅ Básico | ✅ Completo | ✅ Completo |
| **Sin deps externas pesadas** | ✅ ~500KB | ✅ Puro Python | ❌ Requiere Node | ✅ Ya existe |
| **Offline** | ✅ | ✅ | ✅ | ❌ Requiere red+auth |
| **Velocidad** | ✅ μs | 🟡 ms | ❌ ~100ms | ❌ ~1s |
| **ES moderno** | ✅ ES2024+ | 🟡 ES2015 | ✅ | ✅ |
| **Valida tipos BQ** | ❌ | ❌ | ❌ | ✅ |
| **Valida contrato args** | 🟡 Heurístico | 🟡 Heurístico | 🟡 | ✅ |

---

## 5. Recomendación: Estrategia en 2 capas

### Capa 1: `tree-sitter-javascript` (local, siempre)

Validación de **syntax JS** del cuerpo de la rutina. Siempre se ejecuta, sin red, sin auth.

**Qué cubre:**
- Errores de sintaxis JS (paréntesis faltantes, keywords mal usados, etc.)
- Posición exacta del error (línea:columna)
- No falsos positivos — tree-sitter produce árbol incluso con errores parciales

**Qué NO cubre:**
- Tipos BigQuery
- Validez del contrato (¿los args del YAML son los que usa el JS?)
- Funciones BQ JS no estándar

### Capa 2: `BigQuery dry-run` (opcional, con `--online`)

Validación **semántica completa** reconstruyendo el `CREATE TEMP FUNCTION` y ejecutando dry-run.

**Qué cubre:**
- Todo: syntax, tipos, argumentos, return type, runtime errors potenciales
- Es la fuente de verdad definitiva

**Cuándo se usa:**
- Solo con flag explícito: `bq-sync check --online`
- Requiere credenciales GCP activas
- Más lento pero definitivo

---

## 6. Diseño de implementación

### 6.1 Nuevo flag CLI

```
bq-sync check [FILES] [--since HOURS] [--online] [--config PATH]
```

`--online` activa la capa 2 (dry-run BQ) para rutinas JS. Sin `--online`, solo capa 1 (tree-sitter local).

### 6.2 Flujo para rutinas JS

```
1. Detectar language == JAVASCRIPT/JS (desde header)
2. Strip header → obtener cuerpo JS puro
3. [Capa 1] tree-sitter: parsear body → buscar ERROR nodes
   ├─ Errores encontrados → reportar como "error"
   └─ Sin errores → continue
4. [Capa 1.5] Validación de contrato (heurística):
   ├─ Leer YAML del modelo (si existe)
   ├─ Extraer arguments y return_type
   ├─ Verificar que el body tiene `return` statement
   └─ Warning si arg count en YAML ≠ parámetros detectados en JS
5. [Capa 2, solo --online] Dry-run BQ:
   ├─ Reconstruir CREATE TEMP FUNCTION completo
   ├─ Ejecutar dry-run
   └─ Reportar errores BQ
```

### 6.3 Validación de contrato (L2 local)

Usando tree-sitter podemos extraer información estructural del JS:

```python
# Detectar si hay return statement
has_return = any(
    node.type == "return_statement"
    for node in walk_tree(tree.root_node)
)

# Detectar parámetros de función (si el body es una función)
# function transform(x, y) { ... }
func_nodes = [
    n for n in walk_tree(tree.root_node)
    if n.type == "function_declaration"
]
```

Si el YAML dice que hay 2 argumentos pero la función en el body tiene 0 o 3 parámetros → **warning** (no error, porque BQ pasa args posicionalmente al cuerpo JS, no necesariamente como parámetros de función).

> [!NOTE]
> En BQ JS UDFs, los argumentos SQL se mapean a variables globales accesibles directamente en el cuerpo JS — no necesariamente como parámetros de función. Esto significa que un cuerpo `return x + y;` (sin function wrapper) es perfectamente válido si hay args `x` e `y` definidos en el SQL.

### 6.4 Reconstrucción de CREATE TEMP FUNCTION (para dry-run)

```python
def _build_js_udf_query(
    name: str,
    body: str,
    arguments: list[dict],
    return_type: str,
) -> str:
    args_sql = ", ".join(
        f"{a['name']} {a['type']}" for a in arguments
    )
    return (
        f"CREATE TEMP FUNCTION `{name}`({args_sql})\n"
        f"RETURNS {return_type}\n"
        f"LANGUAGE js\n"
        f"AS r\"\"\"\n{body}\n\"\"\";\n"
        f"SELECT `{name}`({', '.join('NULL' for _ in arguments)});\n"
    )
```

---

## 7. Dependencias

| Capa | Dependencia | Peso | Ya existe |
|---|---|---|---|
| L1 syntax | `tree-sitter` + `tree-sitter-javascript` | ~500 KB | ❌ Nuevo |
| L1.5 contrato | tree-sitter (ya cargado) + YAML reader (existente) | 0 | ✅ |
| L2 dry-run | `google-cloud-bigquery` | 0 | ✅ Ya en deps |

**Impacto:** Solo 2 dependencias nuevas (`tree-sitter`, `tree-sitter-javascript`), ambas lightweight y con wheels prebuilt.

---

## 8. Alternativa minimalista: sin tree-sitter

Si preferimos **no agregar dependencias** para JS:

| Nivel | Estrategia |
|---|---|
| L1 syntax | **Regex heurístico**: detectar errores obvios (brackets desbalanceados, strings no cerrados). Frágil pero zero-dep |
| L1.5 contrato | Regex para detectar `return` statement y parámetros |
| L2 dry-run | Igual — BQ API como fuente de verdad |

**Trade-off:** Más falsos positivos/negativos en L1, pero sin dependencias extra. Viable si el volumen de rutinas JS es bajo.

---

## 9. Decisión solicitada

| Pregunta | Opciones |
|---|---|
| **¿Agregar tree-sitter?** | A) Sí — validación JS robusta local / B) No — solo heurística+dry-run |
| **¿Incluir dry-run?** | A) Sí, con `--online` flag / B) No, solo local |
| **¿Validación de contrato?** | A) Sí, warnings heurísticos / B) Solo si hay tree-sitter |
