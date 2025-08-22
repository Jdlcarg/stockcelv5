
-- NUEVA ESTRUCTURA PARA HORARIOS MÚLTIPLES DE CAJA
-- Ejecutar estos scripts en orden en la base de datos Neon

-- 1. CREAR TABLA PARA HORARIOS MÚLTIPLES
CREATE TABLE IF NOT EXISTS cash_schedule_periods (
  id SERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  day_of_week INTEGER NOT NULL, -- 1=Lunes, 7=Domingo
  period_name VARCHAR(100), -- Ej: "Mañana", "Tarde", "Noche"
  open_hour INTEGER NOT NULL CHECK (open_hour >= 0 AND open_hour <= 23),
  open_minute INTEGER NOT NULL CHECK (open_minute >= 0 AND open_minute <= 59),
  close_hour INTEGER NOT NULL CHECK (close_hour >= 0 AND close_hour <= 23),
  close_minute INTEGER NOT NULL CHECK (close_minute >= 0 AND close_minute <= 59),
  auto_open_enabled BOOLEAN DEFAULT true,
  auto_close_enabled BOOLEAN DEFAULT true,
  is_active BOOLEAN DEFAULT true,
  priority_order INTEGER DEFAULT 1, -- Para ordenar periodos del mismo día
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- 2. MIGRAR DATOS EXISTENTES DE HORARIOS SIMPLES
INSERT INTO cash_schedule_periods 
  (client_id, day_of_week, period_name, open_hour, open_minute, close_hour, close_minute, auto_open_enabled, auto_close_enabled, is_active)
SELECT 
  client_id,
  generate_series(1, 7) as day_of_week, -- Crear para todos los días
  'Horario Principal' as period_name,
  open_hour,
  open_minute,
  close_hour,
  close_minute,
  auto_open_enabled,
  auto_close_enabled,
  true
FROM cash_schedule_config
WHERE auto_open_enabled = true OR auto_close_enabled = true;

-- 3. CREAR TABLA DE CONFIGURACIÓN GLOBAL POR CLIENTE
CREATE TABLE IF NOT EXISTS cash_schedule_client_config (
  id SERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  timezone VARCHAR(100) DEFAULT 'America/Argentina/Buenos_Aires',
  auto_schedule_enabled BOOLEAN DEFAULT true,
  notification_enabled BOOLEAN DEFAULT false,
  notification_minutes_before INTEGER DEFAULT 5,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(client_id)
);

-- 4. MIGRAR CONFIGURACIÓN GLOBAL
INSERT INTO cash_schedule_client_config (client_id, timezone, auto_schedule_enabled)
SELECT DISTINCT client_id, timezone, true
FROM cash_schedule_config;

-- 5. ACTUALIZAR TABLA DE LOG PARA INCLUIR PERÍODO
ALTER TABLE cash_auto_operations_log 
ADD COLUMN IF NOT EXISTS schedule_period_id INTEGER REFERENCES cash_schedule_periods(id);

-- 6. CREAR ÍNDICES PARA PERFORMANCE
CREATE INDEX IF NOT EXISTS idx_cash_schedule_periods_client_day ON cash_schedule_periods(client_id, day_of_week);
CREATE INDEX IF NOT EXISTS idx_cash_schedule_periods_client_active ON cash_schedule_periods(client_id, is_active);
CREATE INDEX IF NOT EXISTS idx_cash_schedule_periods_times ON cash_schedule_periods(open_hour, open_minute, close_hour, close_minute);
CREATE INDEX IF NOT EXISTS idx_cash_schedule_client_config_client ON cash_schedule_client_config(client_id);

-- 7. COMENTARIOS DE DOCUMENTACIÓN
COMMENT ON TABLE cash_schedule_periods IS 'Períodos de horarios múltiples de caja por cliente y día de semana';
COMMENT ON TABLE cash_schedule_client_config IS 'Configuración global de horarios automáticos por cliente';
COMMENT ON COLUMN cash_schedule_periods.day_of_week IS '1=Lunes, 2=Martes, ... 7=Domingo';
COMMENT ON COLUMN cash_schedule_periods.priority_order IS 'Orden de ejecución para múltiples períodos del mismo día';

-- 8. OPCIONAL: MANTENER TABLA ANTIGUA POR COMPATIBILIDAD (comentar si no se necesita)
-- DROP TABLE cash_schedule_config; -- Solo ejecutar cuando esté seguro de la migración
