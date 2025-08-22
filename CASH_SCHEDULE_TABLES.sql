
-- Scripts SQL para ejecutar manualmente en la base de datos Neon

-- Tabla para configuración de horarios de caja por cliente
CREATE TABLE IF NOT EXISTS cash_schedule_config (
  id SERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  auto_open_enabled BOOLEAN DEFAULT false,
  auto_close_enabled BOOLEAN DEFAULT false,
  open_hour INTEGER DEFAULT 9,
  open_minute INTEGER DEFAULT 0,
  close_hour INTEGER DEFAULT 18,
  close_minute INTEGER DEFAULT 0,
  active_days TEXT DEFAULT '1,2,3,4,5,6,7', -- Días activos (1=Lunes, 7=Domingo)
  timezone TEXT DEFAULT 'America/Argentina/Buenos_Aires',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(client_id)
);

-- Tabla para log de operaciones automáticas
CREATE TABLE IF NOT EXISTS cash_auto_operations_log (
  id SERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  operation_type TEXT NOT NULL, -- 'auto_open', 'auto_close', 'auto_report'
  cash_register_id INTEGER REFERENCES cash_register(id),
  scheduled_time TIMESTAMP,
  executed_time TIMESTAMP DEFAULT NOW(),
  status TEXT DEFAULT 'success', -- 'success', 'failed', 'skipped'
  error_message TEXT,
  report_id INTEGER REFERENCES daily_reports(id),
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_cash_schedule_config_client_id ON cash_schedule_config(client_id);
CREATE INDEX IF NOT EXISTS idx_cash_auto_operations_log_client_id ON cash_auto_operations_log(client_id);
CREATE INDEX IF NOT EXISTS idx_cash_auto_operations_log_operation_type ON cash_auto_operations_log(operation_type);
CREATE INDEX IF NOT EXISTS idx_cash_auto_operations_log_scheduled_time ON cash_auto_operations_log(scheduled_time);

-- Comentarios para documentación
COMMENT ON TABLE cash_schedule_config IS 'Configuración de horarios automáticos de caja por cliente';
COMMENT ON TABLE cash_auto_operations_log IS 'Log de operaciones automáticas de caja (apertura, cierre, reportes)';
