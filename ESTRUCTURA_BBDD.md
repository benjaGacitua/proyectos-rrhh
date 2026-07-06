# Estructura de Bases de Datos — Proyecto ETL RRHH
> Proyecto: `/opt/etl-bd/proyectos-rrhh/actualizacion-tablas-2`
> Fecha de relevamiento: 2026-06-03
> Motor: **PostgreSQL** (conexión vía túnel SSH con `psycopg2` + `sshtunnel`)

---

## Resumen de Bases de Datos

| Base de Datos | Variable `.env` | Puerto remoto | Descripción |
|---|---|---|---|
| `rh_cramer` | `PG_DATABASE` | 5432 | Base principal RRHH. Schemas: `rh`, `app`, `calculadora`, `costos`, `public`. |
| `db_lavanderia` | `LAV_PG_DATABASE` | 5433 | Base secundaria. Schema `public`. Recibe datos sincronizados desde `rh_cramer`. |

**Fuente de datos origen:** API BUK Chile (`API_BASE_URL=https://cramer.buk.cl/api/v1/chile/`) y BUK Perú (endpoints equivalentes). Autenticación por header `auth_token`.

---

## Schemas en `rh_cramer`

| Schema | Propósito |
|---|---|
| `rh` | Datos maestros y ETL principal (Chile + Perú). |
| `app` | Aplicación web interna: usuarios, roles, módulos, tracking de alertas, candidatos. |
| `calculadora` | Configuración país para calculadora de sueldos (AFP, UF, tasas, tramos impuesto). |
| `costos` | Vistas de apoyo para reportería de costos (dimensiones, jefes). |
| `public` | Vistas utilitarias ad-hoc. |

---

## Schema `rh` — Vista general

### Relaciones

```
rh.areas ◄────────── rh.employees ──────────► rh.consolidado_incidencias
                          │
                          ├────► rh.vacations / rh.vacaciones
                          ├────► rh.contract_alerts ──► app.contract_alert_tracking ──► app.contract_alert_events
                          ├────► rh.job_history (boss_id → employees.id)
                          ├────► rh.finiquitos
                          ├────► rh.licencias
                          └────► rh.reporte_ausentismo_empleados (via SP)

rh.areas_peru ◄────── rh.employees_peru
rh.historical_settlements      ◄──► rh.historical_settlement_items       (Chile, por liquidacion_id)
rh.historical_settlements_peru ◄──► rh.historical_settlement_items_peru  (Perú,  por liquidacion_id)

rh.reporte_kpi_inasistencias          (mensual, agregado por área)
rh.reporte_kpi_inasistencias_semanal  (semanal, agregado por área)
```

---

### Tablas Chile (ya documentadas previamente)

> Sin cambios estructurales — ver descripciones detalladas históricas:
> `rh.employees`, `rh.areas`, `rh.consolidado_incidencias`, `rh.vacations`,
> `rh.contract_alerts`, `rh.historical_settlements`, `rh.historical_settlement_items`,
> `rh.job_history`, `rh.reporte_ausentismo_empleados`.

---

### Tablas Perú (NUEVAS)

#### `rh.areas_peru`
Estructura organizacional Perú. Carga vía endpoint `/organization/areas/` de la instancia BUK Perú.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | `INT` PK | ID del área en BUK. |
| `name` | `VARCHAR(255)` | Nombre del área. |
| `address` | `TEXT` | Dirección. |
| `first_level_id` / `first_level_name` | `INT` / `VARCHAR(255)` | Empresa (primer nivel). |
| `second_level_id` / `second_level_name` | `INT` / `VARCHAR(255)` | Subárea (segundo nivel). |
| `cost_center` | `VARCHAR(100)` | Centro de costo. |
| `status` | `VARCHAR(50)` | `active` / `inactive`. |
| `city` | `VARCHAR(100)` | Ciudad. |
| `fecha_actualizacion` | `TIMESTAMP` | Default `now()`. |

**Filas actuales:** 19. **Lógica:** equivalente a `rh.areas` Chile.

#### `rh.employees_peru`
Empleados de Perú. PK por `document_number` (DNI/CE peruano).

| Columna | Tipo | Notas |
|---|---|---|
| `document_number` | `VARCHAR(50)` **PK** | Documento de identidad peruano. |
| `person_id`, `id` | `INT` | IDs BUK (nullable). |
| `full_name`, `email`, `personal_email`, `phone`, `address`, `province`, `district` | varchar/text | Datos de contacto. |
| `document_type` | `VARCHAR(50)` | Tipo documento (DNI/CE). |
| `birthday`, `gender`, `nationality`, `civil_status` | mixto | Demográficos. |
| `university`, `education_status`, `degree` | varchar | Educación. |
| `bank`, `account_type`, `account_number`, `payment_method`, `payment_currency` | varchar | Datos bancarios. |
| `health_company`, `pension_regime`, `pension_fund`, `afc`, `retired`, `retirement_regime` | mixto | Previsional. |
| `active_since`, `status`, `start_date`, `end_date`, `active_until`, `termination_reason`, `progressive_vacations_start` | fechas | Estado contractual. |
| `private_role`, `name_role`, `area_id`, `cost_center` | mixto | Rol/área. |
| `id_boss`, `dni_boss` | int/varchar | Jefe directo. |
| `base_wage` | `INT` | Sueldo base. |
| `contract_type` | `VARCHAR(50)` | Tipo de contrato. |
| `updated_at` | `TIMESTAMP` | Default `now()`. |

**Filas actuales:** 116. **Diferencias vs `rh.employees`** (Chile): `document_number` reemplaza `rut`, agrega `province`, `payment_currency`, `progressive_vacations_start`, `education_status`, `private_role`, `id_boss`/`dni_boss` en lugar de `rut_boss`.

#### `rh.historical_settlements_peru` y `rh.historical_settlement_items_peru`
Liquidaciones de sueldo Perú. Estructura idéntica a las versiones Chile, salvo que se reemplaza `rut` por `document_number`. Carga mensual (igual idempotencia).

- **`rh.historical_settlements_peru`** — Filas: 2.275. Índices por `liquidacion_id`, `employee_id`, `document_number`, `(anio, mes)`.
- **`rh.historical_settlement_items_peru`** — Filas: 132.609. Mismos índices que la versión Chile más `document_number`.

---

### Tablas RH adicionales (NUEVAS)

#### `rh.finiquitos`
Registro de finiquitos calculados/registrados manualmente (no proviene de la API BUK).

| Columna | Tipo | Notas |
|---|---|---|
| `id` | `SERIAL` PK | — |
| `nombre_trabajador`, `rut_trabajador` | varchar | Empleado. |
| `cargo`, `fecha_ingreso`, `fecha_salida`, `duracion_empresa` (`FLOAT`) | mixto | Datos del contrato. |
| `estado`, `causal` | varchar | Estado del finiquito y causal de término. |
| `sueldo_base`, `bonificaciones_mensuales`, `movilizacion`, `bono_sala_cuna`, `desgaste_vehiculo` | numéricos | Componentes del cálculo. |
| `nombre_jefe`, `rut_jefe` | varchar | Jefe directo. |
| `vacaciones_pendientes` | `FLOAT` | Días pendientes. |

**Índices:** por `nombre_trabajador`, `rut_trabajador`, `rut_jefe`, `id`. **Filas actuales:** 0 (tabla recién creada).

#### `rh.licencias`
Registro liviano de licencias (campos en español). Aparentemente alternativa/staging a `consolidado_incidencias`.

| Columna | Tipo |
|---|---|
| `id` SERIAL PK, `nombre_trabajador`, `rut_trabajador`, `fecha_inicio`, `fecha_fin`, `motivo` (VARCHAR 500) |

**Índices:** por `rut_trabajador`, `nombre_trabajador`. **Filas actuales:** 0.

#### `rh.vacaciones`
Versión "ES" de `rh.vacations`, ampliada con `full_name`, `rut`, `calendar_days`. PK `id` autoincremental.

| Columna | Tipo |
|---|---|
| `id` SERIAL PK, `employee_id` (NOT NULL), `full_name`, `rut`, `working_days`, `calendar_days`, `workday_stage`, `type`, `status`, `start_date`, `end_date` |

**Índices:** por `rut`, `full_name`, `id`. **Filas actuales:** 0 (la fuente operativa sigue siendo `rh.vacations`).

> **Convivencia:** `rh.vacations` (PK = ID BUK, sin nombre/rut copiados, con `created_at/updated_at`) se mantiene como la tabla cargada por el ETL. `rh.vacaciones` se ve como una tabla nueva pensada para reportería interna o carga manual.

#### `rh.reporte_kpi_inasistencias`
KPI mensual de inasistencias por área. **PK compuesta:** `(mes, area)`.

| Columna | Tipo | Notas |
|---|---|---|
| `mes` | `DATE` | Primer día del mes. |
| `periodo` | `VARCHAR(7)` | `YYYY-MM`. |
| `area`, `cost_center` | varchar | Dimensión. |
| `dotacion_teorica` | `INT` | Empleados activos en el mes. |
| `dias_habiles_mes`, `dias_habiles_dotacion` | `INT` | Capacidad teórica. |
| `personas_con_inasistencia`, `dias_totales_inasistencia` | `INT` | Medidas. |
| `pct_dotacion_afectada`, `pct_dias_perdidos` | `NUMERIC` | Indicadores. |

**Filas actuales:** 10.798. **Vista de cálculo:** `rh.vw_kpi_inasistencias_mensual` (genera desde `employees`, `areas`, `consolidado_incidencias`, calendarios derivados).

#### `rh.reporte_kpi_inasistencias_semanal`
Equivalente semanal. **PK compuesta:** `(semana_inicio, area)`.

| Columnas adicionales |
|---|
| `semana_del_mes`, `semana_inicio`, `semana_fin`, `dias_habiles_semana`, `personas_ausentes`, `dias_ausencia`, `personas_presentes`, `pct_ausente`, `pct_presente`, `pct_dias_perdidos` |

**Filas actuales:** 46.939.

---

### Vistas en schema `rh`

| Vista | Qué entrega |
|---|---|
| `rh.vw_jefes_por_trabajadores` | Por cada empleado activo, jefe directo (full_name + email) obtenido por `rut_boss → employees.rut`. |
| `rh.vw_kpi_inasistencias_mensual` | Cálculo dinámico del KPI mensual (`rh.reporte_kpi_inasistencias` es snapshot). |
| `rh.vw_licencias_postnatal` | Incidencias `%Post Natal%` cuyo `end_date <= CURRENT_DATE + 3` (alerta pronto retorno). |
| `rh.vw_licencias_pre_post_natal` | Incidencias parentales / pre / post natal / "Niño" vigentes hoy. |
| `rh.vw_tabla_bono_buenas_practicas` | Empleados activos con cargos operativos (Operario, Bodega, Peoneta, Chofer, etc.) excluyendo división Sabores. Incluye jefe directo (`em2`) y empresa/área. |
| `rh.vw_tabla_incidencias_trabajador` | Listado de todas las incidencias por trabajador (nombre limpio con `initcap + regexp_replace`), orden DESC por `start_date`. |

---

## Schema `app` — Aplicación web interna (NUEVO)

Soporta una aplicación web con login (contraseña + TOTP / OTP), módulos y candidatos. También aloja el tracking del flujo de respuesta de alertas de contratos (`rh.contract_alerts` → email → endpoint → tracking + events).

### Auth & autorización

#### `app.usuarios`
| Columna | Tipo | Notas |
|---|---|---|
| `id` SERIAL PK, `username` UNIQUE | identificación |
| `email`, `nombre_completo` | varchar | |
| `password_hash` | varchar(255) NOT NULL | hash bcrypt/argon2 |
| `rol_id` | INT → `app.roles.id` | |
| `activo` | BOOLEAN | |
| `created_at`, `last_login` | timestamp | |
| `totp_secret`, `totp_enabled` | varchar / boolean | 2FA TOTP |
| `invite_token`, `invite_token_expires_at` | varchar UNIQUE / ts | Invitación inicial |

**Filas:** 5.

#### `app.roles`
`id` SERIAL PK, `nombre` UNIQUE, `descripcion`, `created_at`. **Filas:** 4.

#### `app.modulos`
Módulos de la app web (menú/secciones).

| Columna | Tipo |
|---|---|
| `id` PK, `codigo` UNIQUE, `nombre`, `descripcion`, `icono`, `ruta`, `orden`, `activo` |

**Filas:** 9.

#### `app.rol_modulos`
Tabla puente `rol_id` × `modulo_id` (PK compuesta). **Filas:** 23.

#### `app.usuario_modulos`
Permisos por usuario que sobreescriben/agregan sobre los del rol. PK compuesta. **Filas:** 21.

#### `app.otp_codes`
Códigos OTP de un solo uso (login/verificación).

| Columna | Tipo |
|---|---|
| `id` PK, `user_id` (NOT NULL), `code_hash`, `expires_at`, `used` BOOLEAN default false, `created_at` |

Índice clave: `(user_id, used, expires_at)`. **Filas:** 5.

### Operación

#### `app.calendariocierres`
Fechas de cierre mensual de RRHH (referencia para liquidaciones / KPIs).

| Columna | Tipo |
|---|---|
| `id` PK, `anio`, `mes`, `fecha_cierre`, `created_at` |

UNIQUE `(anio, mes)`. **Filas:** 2.

#### `app.candidatos_seleccion`
Pipeline de candidatos de selección de personal.

| Columna | Tipo |
|---|---|
| `id` PK, `empresa`, `fecha_cierre`, `rut`, `nombre`, `cargo`, `lugar_de_trabajo`, `jornada_de_trabajo`, `tipo_de_contrato`, `fecha_de_inicio`, `gerencia`, `sueldo_base`, `bono`, `movilizacion`, `correo_analista`, `status` |

**Filas:** 1.

### Tracking de alertas de contrato

> Convive con `rh.contract_alerts` (generador). `rh.contract_alerts` lista las alertas vigentes; `app.contract_alert_tracking` lleva el ciclo de vida (envío → respuesta del jefe → sync a BUK).

#### `app.contract_alert_tracking`
| Columna | Tipo | Notas |
|---|---|---|
| `id` PK | — | — |
| `employee_id`, `rut`, `employee_name`, `employee_role` | — | Snapshot del empleado al momento de la alerta. |
| `boss_name`, `boss_email` | — | Destinatario del correo. |
| `alert_date`, `alert_type`, `alert_reason` | date / varchar | — |
| `response_token` | TEXT UNIQUE | Token firmado del link en el correo. |
| `first_sent_at`, `last_followup_at`, `followup_count` | ts / int | Trazabilidad de envío + recordatorios. |
| `response`, `responded_at`, `responder_ip` | varchar / ts / varchar(45) | Respuesta del jefe (e.g. renovar / no renovar / indefinido). |
| `buk_synced`, `buk_synced_at`, `buk_sync_error` | boolean / ts / text | Estado de sync hacia BUK. |
| `created_at`, `updated_at` | ts | — |

UNIQUE `(employee_id, alert_date)`. **Filas:** 27.

#### `app.contract_alert_events`
Eventos auditados sobre cada tracking (open, click, submit, error, etc.).

| Columna | Tipo |
|---|---|
| `id` PK, `tracking_id` (FK lógico → tracking.id), `event_type`, `answer`, `ip` (45), `user_agent` TEXT, `created_at` TZ |

**Filas:** 13.

---

## Schema `calculadora` (NUEVO)

#### `calculadora.country_config`
Configuración paramétrica por país de la calculadora de sueldos/finiquitos. PK `pais`.

| Columna | Tipo | Notas |
|---|---|---|
| `pais` TEXT PK | — | "Chile", "Peru", etc. |
| `afp_data` JSONB | + `afp_updated_at` | Tasas y datos AFP. |
| `uf_value` NUMERIC | + `uf_updated_at` | Valor UF vigente. |
| `dolar_value` NUMERIC | + `dolar_updated_at` | Valor del dólar. |
| `tasas` JSONB | + `tasas_updated_at` | Tasas varias (salud, AFC, etc.). |
| `tax_brackets` JSONB | + `tax_brackets_updated_at` | Tramos impuesto único. |
| `bonos_anuales_uf` JSONB | default `{"navidad":7,"escolaridad":3,"fiestaPatrias":6}` | Bonos anuales por defecto. |
| `bonos_empresa` JSONB | — | Override por empresa. |
| `updated_at` TIMESTAMPTZ | — | — |

**Filas:** 3.

---

## Schema `costos` (NUEVO, solo vistas)

| Vista | Qué entrega |
|---|---|
| `costos.v_dimensiones` | DISTINCT de `empresa, area, subarea, centro_costo, cargo` cruzando `rh.employees` activos con `rh.areas`. Útil para combos/dropdowns. |
| `costos.v_jefes` | Empleados activos que son jefes (aparecen como `rut_boss` de otros) + recuento de `subordinados_directos`. |

---

## Schema `public` (en `rh_cramer`) — utilitarios

| Vista | Qué entrega |
|---|---|
| `public.vw_correos_por_lista_ruts` | Devuelve `full_name, rut, email, personal_email` para una lista fija de RUTs **hardcodeada** dentro del SQL de la vista. Hay que regenerarla cuando cambia la lista (no recibe parámetros). |
| `public.vw_licencias_pre_post_natal` | Duplicado funcional de `rh.vw_licencias_pre_post_natal`. |

---

## Base de Datos: `db_lavanderia` — Schema `public` (sin cambios)

Sigue conteniendo: `public.areas`, `public.subareas`, `public.empresa`, `public.personal`. Sincronización vía `app/etl/sync_lavanderia.py` (upsert por `rut`).

---

## Endpoints API BUK (Chile + Perú)

Igual que Chile pero apuntando a la instancia Perú (`https://cramer.buk.cl/api/v1/peru/...` o instancia equivalente; revisar `app/config/settings.py`).

| Entidad | Endpoint | Frecuencia |
|---|---|---|
| Empleados Chile | `{API_BASE_URL}employees` | Diaria |
| Áreas Chile | `{API_BASE_URL}organization/areas/?status=both` | Diaria |
| Vacaciones Chile | `{API_BASE_URL}vacations` | Diaria |
| Incidencias Chile | `{API_BASE_URL}absences/permission` + `/licence` + `/absence` | Diaria (últimos 7 días) |
| Liquidaciones Chile | `{URL_SETTLEMENTS_CHILE}?date=DD-MM-YYYY` | Mensual |
| Empleados Perú | endpoint Perú | Diaria |
| Áreas Perú | endpoint Perú | Diaria |
| Liquidaciones Perú | endpoint Perú | Mensual |

---

## Flujo ETL — Orden de ejecución sugerido

```
0. [MENSUAL]  ETL Liquidaciones Chile  → rh.historical_settlements (+ items)
0b.[MENSUAL]  ETL Liquidaciones Perú   → rh.historical_settlements_peru (+ items)
1. [DIARIO]   ETL Empleados Chile      → rh.employees
1b.[DIARIO]   ETL Empleados Perú       → rh.employees_peru
2. [DIARIO]   ETL Áreas Chile          → rh.areas
2b.[DIARIO]   ETL Áreas Perú           → rh.areas_peru
3. [DIARIO]   ETL Vacaciones           → rh.vacations
4. [DIARIO]   ETL Incidencias          → rh.consolidado_incidencias
5. [DIARIO]   Generación alertas       → rh.contract_alerts → app.contract_alert_tracking
6. [DIARIO]   Refresh KPI inasistencias→ rh.reporte_kpi_inasistencias (+ semanal)
7. [DIARIO]   Sync lavandería          → db_lavanderia.public.*
```

---

## Notas para construir soluciones

- **Dimensión empresa/área/subárea:** usar `costos.v_dimensiones` o JOIN directo `rh.employees ⨝ rh.areas` por `area_id`.
- **Jerarquía de jefes:** `rh.employees.rut_boss → rh.employees.rut` (self-join). Atajo: `rh.vw_jefes_por_trabajadores`, `costos.v_jefes`.
- **Incidencias / ausentismo:** fuente diaria es `rh.consolidado_incidencias`; KPIs agregados en `rh.reporte_kpi_inasistencias[_semanal]` (snapshot) o `rh.vw_kpi_inasistencias_mensual` (en vivo).
- **Pre/Post natal:** `rh.vw_licencias_pre_post_natal` (vigentes) y `rh.vw_licencias_postnatal` (cerca de finalizar).
- **Operación de RRHH (app web):** todo lo de login/permisos/módulos vive en `app.*`. El tracking del ciclo de respuesta de alertas de contrato está en `app.contract_alert_tracking` + `app.contract_alert_events`.
- **Calculadora de sueldos/finiquitos:** parámetros vivos en `calculadora.country_config` (JSONB versionado con `*_updated_at`). Resultados de finiquitos persistentes en `rh.finiquitos`.
- **Perú vs Chile:** la clave de empleado en Perú es `document_number` (no RUT). Las tablas siguen el sufijo `_peru`.

---

## Archivos de referencia (proyecto ETL)

| Archivo | Descripción |
|---|---|
| `app/etl/create.py` | DDL del schema `rh` (Chile). |
| `app/etl/extract.py` | Extracción API BUK. |
| `app/etl/load.py` | Cargas upsert. |
| `app/etl/settlements_chile.py` | Liquidaciones Chile. |
| `app/etl/contract_alerts.py` | Generación alertas. |
| `app/etl/sync_lavanderia.py` | Sync hacia `db_lavanderia`. |
| `app/utils/db_client.py` | Conexiones SSH + psycopg2. |
| `app/main.py` | Orquestador. |
| `app/sql/*.sql` | Scripts SQL Server legacy. |
