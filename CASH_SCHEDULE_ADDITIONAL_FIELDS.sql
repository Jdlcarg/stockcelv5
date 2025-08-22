
-- Scripts SQL adicionales para soporte completo de horarios automáticos

-- Agregar campos opcionales a daily_reports para marcar reportes automáticos
ALTER TABLE daily_reports 
ADD COLUMN IF NOT EXISTS auto_generated_type TEXT,
ADD COLUMN IF NOT EXISTS cash_register_closed_id INTEGER REFERENCES cash_register(id);

-- Crear índices para performance en las nuevas tablas
CREATE INDEX IF NOT EXISTS idx_daily_reports_auto_generated ON daily_reports(auto_generated_type) WHERE auto_generated_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_daily_reports_cash_register_closed ON daily_reports(cash_register_closed_id);

-- Comentarios adicionales
COMMENT ON COLUMN daily_reports.auto_generated_type IS 'Tipo de generación automática: auto_close, manual, scheduled';
COMMENT ON COLUMN daily_reports.cash_register_closed_id IS 'ID de la caja registradora que se cerró automáticamente';

-- Agregar configuración por defecto para clientes existentes (opcional)
-- INSERT INTO cash_schedule_config (client_id, auto_open_enabled, auto_close_enabled)
-- SELECT id, false, false FROM clients 
-- WHERE id NOT IN (SELECT client_id FROM cash_schedule_config);
