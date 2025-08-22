
-- Verificar y corregir la tabla stock_control_sessions
-- Ejecutar estos comandos en la consola de Neon

-- 1. Verificar estructura actual
SELECT column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'stock_control_sessions' 
ORDER BY ordinal_position;

-- 2. Si la tabla no existe o tiene problemas, recrearla
DROP TABLE IF EXISTS stock_control_sessions CASCADE;

CREATE TABLE stock_control_sessions (
  id SERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  user_id INTEGER NOT NULL REFERENCES users(id),
  date TIMESTAMP NOT NULL,
  start_time TIMESTAMP DEFAULT NOW(),
  end_time TIMESTAMP,
  total_products INTEGER NOT NULL DEFAULT 0,
  scanned_products INTEGER NOT NULL DEFAULT 0,
  missing_products INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- 3. Verificar que se creó correctamente
SELECT * FROM stock_control_sessions LIMIT 1;
