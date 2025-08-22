import { pgTable, text, serial, integer, boolean, timestamp, decimal, uuid, varchar } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Custom validators
const positiveNumber = z.number().positive('Must be a positive number');
const nonEmptyString = z.string().min(1, 'Field cannot be empty').max(255, 'Field too long');
const email = z.string().email('Invalid email format').max(255);
const phoneRegex = /^\+?[\d\s\-\(\)]+$/;
const imeiRegex = /^\d{15}$/;

// Enhanced validation schemas
export const productValidation = z.object({
  imei: z.string().regex(imeiRegex, 'IMEI must be exactly 15 digits'),
  model: nonEmptyString,
  storage: z.string().min(1),
  color: z.string().min(1),
  costPrice: z.string().refine(val => !isNaN(Number(val)) && Number(val) >= 0, 'Invalid price'),
  quality: z.enum(['Nuevo', 'Excelente', 'Muy Bueno', 'Bueno', 'Regular']),
  battery: z.string().optional(),
  provider: z.string().max(100).optional(),
  observations: z.string().max(500).optional()
});

export const userValidation = z.object({
  username: z.string().min(3, 'Username must be at least 3 characters').max(50),
  email: email,
  password: z.string().min(8, 'Password must be at least 8 characters')
    .regex(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)/, 'Password must contain uppercase, lowercase and number'),
  role: z.enum(['superuser', 'admin', 'vendor']),
  phone: z.string().regex(phoneRegex, 'Invalid phone format').optional()
});

// Clients table for multi-tenancy
export const clients = pgTable("clients", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  email: text("email").notNull().unique(),
  phone: text("phone"),
  address: text("address"),
  isActive: boolean("is_active").default(true),
  trialStartDate: timestamp("trial_start_date"),
  trialEndDate: timestamp("trial_end_date"),
  subscriptionType: text("subscription_type").default("trial"), // trial, premium_monthly, premium_yearly, unlimited, expired
  notes: text("notes"), // Admin notes about subscription status
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
  salesContactNumber: text("sales_contact_number"), // Número de contacto para ventas
});

// Users table with client_id for multi-tenancy
export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  username: text("username").notNull(),
  email: text("email").notNull(),
  password: text("password").notNull(),
  role: text("role").notNull(), // 'superuser', 'admin', 'vendor'
  permissions: text("permissions"), // JSON string with permissions array
  isActive: boolean("is_active").default(true),
  mustChangePassword: boolean("must_change_password").default(true), // Force password change on first login
  passwordResetToken: text("password_reset_token"),
  passwordResetExpires: timestamp("password_reset_expires"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Products table
export const products = pgTable("products", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  imei: text("imei").notNull(),
  model: text("model").notNull(),
  storage: text("storage").notNull(),
  color: text("color").notNull(),
  quality: text("quality"),
  battery: text("battery"),
  costPrice: decimal("cost_price", { precision: 10, scale: 2 }).notNull(),
  status: text("status").notNull().default("disponible"), // 'disponible', 'reservado', 'vendido', 'tecnico_interno', 'tecnico_externo', 'a_reparar', 'extravio'
  provider: text("provider"),
  observations: text("observations"),
  entryDate: timestamp("entry_date").defaultNow(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Orders table
export const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  vendorId: integer("vendor_id").references(() => vendors.id),
  customerId: integer("customer_id").references(() => customers.id),
  orderNumber: text("order_number").notNull(),
  customerName: text("customer_name").notNull(),
  customerPhone: text("customer_phone"),
  customerEmail: text("customer_email"),
  totalUsd: decimal("total_usd", { precision: 10, scale: 2 }).notNull(),
  status: text("status").notNull().default("pendiente"), // 'pendiente', 'completado', 'cancelado'
  paymentStatus: text("payment_status").notNull().default("pendiente"), // 'pendiente', 'pagado', 'parcial'
  observations: text("observations"),
  notes: text("notes"),
  shippingType: text("shipping_type"), // 'oficina', 'direccion'
  shippingAddress: text("shipping_address"), // Only when shippingType is 'direccion'
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Order items table
export const orderItems = pgTable("order_items", {
  id: serial("id").primaryKey(),
  orderId: integer("order_id").references(() => orders.id).notNull(),
  productId: integer("product_id").references(() => products.id).notNull(),
  quantity: integer("quantity").notNull().default(1),
  priceUsd: decimal("price_usd", { precision: 10, scale: 2 }).notNull(),
  paymentMethod: text("payment_method"), // Individual payment method per product
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }),
  amountUsd: decimal("amount_usd", { precision: 10, scale: 2 }),
  createdAt: timestamp("created_at").defaultNow(),
});

// Payments table
export const payments = pgTable("payments", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  orderId: integer("order_id").references(() => orders.id).notNull(),
  paymentMethod: text("payment_method").notNull(), // 'efectivo_dolar', 'efectivo_pesos', 'transferencia_pesos', 'transferencia_usdt', 'transferencia_financiera', 'financiera'
  amount: decimal("amount", { precision: 10, scale: 2 }).notNull(),
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }),
  exchangeRateType: text("exchange_rate_type"), // 'ARS→USD', 'USD→ARS' for display purposes
  amountUsd: decimal("amount_usd", { precision: 10, scale: 2 }).notNull(),
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Currency exchange transactions table
export const currencyExchanges = pgTable("currency_exchanges", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  fromCurrency: text("from_currency").notNull(), // 'usd', 'ars', 'usdt'
  toCurrency: text("to_currency").notNull(), // 'usd', 'ars', 'usdt'
  fromAmount: decimal("from_amount", { precision: 10, scale: 2 }).notNull(),
  toAmount: decimal("to_amount", { precision: 10, scale: 2 }).notNull(),
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }).notNull(),
  category: text("category").notNull().default("conversion"), // 'conversion', 'purchase', 'sale', 'conversion_moneda'
  notes: text("notes"),
  userId: integer("user_id").references(() => users.id).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

// Cash register table - Enhanced for continuous operation
export const cashRegister = pgTable("cash_register", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  date: timestamp("date").notNull(),
  initialUsd: decimal("initial_usd", { precision: 10, scale: 2 }).notNull(),
  initialArs: decimal("initial_ars", { precision: 10, scale: 2 }).notNull(),
  initialUsdt: decimal("initial_usdt", { precision: 10, scale: 2 }).notNull(),
  currentUsd: decimal("current_usd", { precision: 10, scale: 2 }).notNull(),
  currentArs: decimal("current_ars", { precision: 10, scale: 2 }).notNull(),
  currentUsdt: decimal("current_usdt", { precision: 10, scale: 2 }).notNull(),
  dailySales: decimal("daily_sales", { precision: 10, scale: 2 }).notNull(),
  totalDebts: decimal("total_debts", { precision: 10, scale: 2 }).default("0"),
  totalExpenses: decimal("total_expenses", { precision: 10, scale: 2 }).default("0"),
  dailyGlobalExchangeRate: decimal("daily_global_exchange_rate", { precision: 10, scale: 4 }),
  isOpen: boolean("is_open").default(true),
  isActive: boolean("is_active").default(true), // For continuous operation
  closedAt: timestamp("closed_at"),
  autoClosedAt: timestamp("auto_closed_at"), // For automatic daily closure
  createdAt: timestamp("created_at").defaultNow(),
});

// Cash movements table - Enhanced for comprehensive tracking
export const cashMovements = pgTable("cash_movements", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  cashRegisterId: integer("cash_register_id").references(() => cashRegister.id).notNull(),
  type: text("type").notNull(), // 'ingreso', 'egreso', 'pago_deuda', 'retiro', 'gasto', 'venta', 'comision_vendedor'
  subtype: text("subtype"), // 'efectivo_ars', 'efectivo_usd', 'transferencia_ars', etc.
  amount: decimal("amount", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").notNull().default("USD"), // 'USD', 'ARS', 'USDT'
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }),
  amountUsd: decimal("amount_usd", { precision: 10, scale: 2 }).notNull(), // Converted to USD
  description: text("description").notNull(),
  referenceId: integer("reference_id"), // Order ID, Payment ID, etc.
  referenceType: text("reference_type"), // 'order', 'payment', 'debt', 'expense'
  customerId: integer("customer_id").references(() => customers.id),
  vendorId: integer("vendor_id").references(() => vendors.id),
  userId: integer("user_id").references(() => users.id).notNull(),
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Expenses table - For tracking all business expenses
export const expenses = pgTable("expenses", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  cashRegisterId: integer("cash_register_id").references(() => cashRegister.id),
  category: text("category").notNull(), // 'alquiler', 'servicios', 'compras', 'sueldos', 'otros'
  description: text("description").notNull(),
  amount: decimal("amount", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").notNull().default("USD"),
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }),
  amountUsd: decimal("amount_usd", { precision: 10, scale: 2 }).notNull(),
  paymentMethod: text("payment_method").notNull(), // 'efectivo_ars', 'efectivo_usd', etc.
  providerId: integer("provider_id"), // If expense is for a provider
  userId: integer("user_id").references(() => users.id).notNull(),
  receiptNumber: text("receipt_number"),
  notes: text("notes"),
  expenseDate: timestamp("expense_date").defaultNow(),
  createdAt: timestamp("created_at").defaultNow(),
});

// Debt payments table - Enhanced tracking
export const debtPayments = pgTable("debt_payments", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  debtId: integer("debt_id").references(() => customerDebts.id).notNull(),
  cashRegisterId: integer("cash_register_id").references(() => cashRegister.id),
  amount: decimal("amount", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").notNull().default("USD"),
  exchangeRate: decimal("exchange_rate", { precision: 10, scale: 4 }),
  amountUsd: decimal("amount_usd", { precision: 10, scale: 2 }).notNull(),
  paymentMethod: text("payment_method").notNull(),
  userId: integer("user_id").references(() => users.id).notNull(),
  notes: text("notes"),
  paymentDate: timestamp("payment_date").defaultNow(),
  createdAt: timestamp("created_at").defaultNow(),
});

// Daily financial reports table
export const dailyReports = pgTable("daily_reports", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  reportDate: timestamp("report_date").notNull(),
  openingBalance: decimal("opening_balance", { precision: 10, scale: 2 }).notNull(),
  totalIncome: decimal("total_income", { precision: 10, scale: 2 }).notNull(),
  totalExpenses: decimal("total_expenses", { precision: 10, scale: 2 }).notNull(),
  totalDebts: decimal("total_debts", { precision: 10, scale: 2 }).notNull(),
  totalDebtPayments: decimal("total_debt_payments", { precision: 10, scale: 2 }).notNull(),
  netProfit: decimal("net_profit", { precision: 10, scale: 2 }).notNull(),
  vendorCommissions: decimal("vendor_commissions", { precision: 10, scale: 2 }).notNull(),
  exchangeRateUsed: decimal("exchange_rate_used", { precision: 10, scale: 4 }).notNull(),
  closingBalance: decimal("closing_balance", { precision: 10, scale: 2 }).notNull(),
  totalMovements: integer("total_movements").notNull(),
  reportData: text("report_data"), // JSON with detailed movements
  generatedAt: timestamp("generated_at").defaultNow(),
  isAutoGenerated: boolean("is_auto_generated").default(false),
});

// Customer debts table - Enhanced
export const customerDebts = pgTable("customer_debts", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  customerId: integer("customer_id").references(() => customers.id).notNull(),
  orderId: integer("order_id").references(() => orders.id),
  debtAmount: decimal("debt_amount", { precision: 10, scale: 2 }).notNull(),
  paidAmount: decimal("paid_amount", { precision: 10, scale: 2 }).default("0"),
  remainingAmount: decimal("remaining_amount", { precision: 10, scale: 2 }).notNull(),
  currency: text("currency").notNull().default("USD"), // 'USD', 'ARS', 'USDT'
  status: text("status").notNull().default("vigente"), // 'vigente', 'pagada', 'vencida'
  dueDate: timestamp("due_date"),
  paymentHistory: text("payment_history"), // JSON array of payments
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Product history table
export const productHistory = pgTable("product_history", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  productId: integer("product_id").references(() => products.id).notNull(),
  previousStatus: text("previous_status"),
  newStatus: text("new_status").notNull(),
  userId: integer("user_id").references(() => users.id).notNull(),
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Stock control sessions table
export const stockControlSessions = pgTable("stock_control_sessions", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  userId: integer("user_id").references(() => users.id).notNull(),
  date: timestamp("date").notNull(),
  startTime: timestamp("start_time").defaultNow(),
  endTime: timestamp("end_time"),
  totalProducts: integer("total_products").notNull().default(0),
  scannedProducts: integer("scanned_products").notNull().default(0),
  missingProducts: integer("missing_products").notNull().default(0),
  status: text("status").notNull().default("active"), // 'active', 'completed'
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Stock control items table (individual products scanned)
export const stockControlItems = pgTable("stock_control_items", {
  id: serial("id").primaryKey(),
  sessionId: integer("session_id").references(() => stockControlSessions.id).notNull(),
  productId: integer("product_id").references(() => products.id).notNull(),
  imei: text("imei").notNull(),
  scannedAt: timestamp("scanned_at").defaultNow(),
  status: text("status").notNull().default("scanned"), // 'scanned', 'missing'
  actionTaken: text("action_taken"), // 'tecnico_interno', 'tecnico_externo', 'a_reparar', 'extravio'
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// System configuration table (only for SuperUser)
export const systemConfiguration = pgTable("system_configuration", {
  id: serial("id").primaryKey(),
  // Planes de suscripción
  premiumMonthlyPrice: varchar("premium_monthly_price", { length: 50 }).default("$29.99/mes"),
  premiumYearlyPrice: varchar("premium_yearly_price", { length: 50 }).default("$299.99/año"),
  premiumYearlyDiscount: varchar("premium_yearly_discount", { length: 100 }).default("2 meses gratis"),
  premiumYearlyPopular: boolean("premium_yearly_popular").default(true),
  
  // Información de contacto de ventas
  salesPhone: varchar("sales_phone", { length: 50 }).default("1170627214"),
  salesEmail: varchar("sales_email", { length: 255 }).default("ventas@stockcel.com"),
  supportEmail: varchar("support_email", { length: 255 }).default("soporte@stockcel.com"),
  
  // Mensajes personalizables
  expiredTitle: varchar("expired_title", { length: 255 }).default("Suscripción Expirada"),
  expiredMessage: text("expired_message").default("Tu período de prueba ha expirado. Para continuar usando StockCel, necesitas renovar tu suscripción."),
  plansTitle: varchar("plans_title", { length: 255 }).default("Planes disponibles:"),
  contactTitle: varchar("contact_title", { length: 255 }).default("Contacta a ventas:"),
  
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Configuration table
export const configuration = pgTable("configuration", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  key: text("key").notNull(),
  value: text("value").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Company Configuration table
export const companyConfiguration = pgTable("company_configuration", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  companyName: text("company_name").notNull(),
  cuit: text("cuit").notNull(),
  address: text("address").notNull(),
  phone: text("phone"),
  email: text("email"),
  website: text("website"),
  logoUrl: text("logo_url"),
  taxCondition: text("tax_condition"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Vendors table (vendedores)
export const vendors = pgTable("vendors", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  name: text("name").notNull(),
  phone: text("phone"),
  email: text("email"),
  commissionPercentage: decimal("commission_percentage", { precision: 5, scale: 2 }), // Porcentaje de comisión
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Customers table (compradores)
export const customers = pgTable("customers", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  name: text("name").notNull(),
  phone: text("phone"),
  email: text("email"),
  address: text("address"),
  identification: text("identification"), // DNI, CI, etc.
  notes: text("notes"),
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Generated Reports table (archivos Excel/PDF generados automáticamente)
export const generatedReports = pgTable("generated_reports", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  dailyReportId: integer("daily_report_id").references(() => dailyReports.id).notNull(),
  reportDate: timestamp("report_date").notNull(),
  reportType: text("report_type").notNull(), // 'excel', 'pdf'
  fileName: text("file_name").notNull(),
  fileData: text("file_data").notNull(), // Base64 encoded file content
  fileSize: integer("file_size").notNull(), // File size in bytes
  reportContent: text("report_content").notNull(), // JSON with report sections data
  generatedAt: timestamp("generated_at").defaultNow(),
  isAutoGenerated: boolean("is_auto_generated").default(true),
});

// Resellers table - Revendedores que pueden vender cuentas del sistema
export const resellers = pgTable("resellers", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  email: text("email").notNull().unique(),
  phone: text("phone"),
  company: text("company"),
  password: text("password").notNull(),
  isActive: boolean("is_active").default(true),
  accountsQuota: integer("accounts_quota").notNull().default(0), // Cuántas cuentas puede vender
  accountsSold: integer("accounts_sold").notNull().default(0), // Cuántas ha vendido
  costPerAccount: decimal("cost_per_account", { precision: 10, scale: 2 }).default("1000.00"), // Costo fijo por cuenta
  totalPaid: decimal("total_paid", { precision: 10, scale: 2 }).default("0.00"), // Total pagado por las cuentas
  totalEarnings: decimal("total_earnings", { precision: 10, scale: 2 }).default("0.00"), // Ganancia total
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Reseller sales - Ventas realizadas por los revendedores
export const resellerSales = pgTable("reseller_sales", {
  id: serial("id").primaryKey(),
  resellerId: integer("reseller_id").references(() => resellers.id).notNull(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  costPerAccount: decimal("cost_per_account", { precision: 10, scale: 2 }).notNull(), // Costo fijo que pagó al SuperUser por esta cuenta
  salePrice: decimal("sale_price", { precision: 10, scale: 2 }).notNull(), // Precio al que lo vendió al cliente
  profit: decimal("profit", { precision: 10, scale: 2 }).notNull(), // Ganancia del revendedor (salePrice - costPerAccount)
  subscriptionType: text("subscription_type").notNull(), // Tipo de suscripción vendida
  trialDays: integer("trial_days").default(7), // Días de prueba asignados
  saleDate: timestamp("sale_date").defaultNow(),
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Reseller configuration - Configuración personalizada por revendedor
export const resellerConfiguration = pgTable("reseller_configuration", {
  id: serial("id").primaryKey(),
  resellerId: integer("reseller_id").references(() => resellers.id).notNull(),
  premiumMonthlyPrice: varchar("premium_monthly_price", { length: 50 }).default("$39.99/mes"),
  premiumYearlyPrice: varchar("premium_yearly_price", { length: 50 }).default("$399.99/año"),
  premiumYearlyDiscount: varchar("premium_yearly_discount", { length: 100 }).default("3 meses gratis"),
  defaultTrialDays: integer("default_trial_days").default(7),
  contactPhone: varchar("contact_phone", { length: 50 }),
  contactEmail: varchar("contact_email", { length: 100 }),
  companyName: varchar("company_name", { length: 200 }),
  supportMessage: varchar("support_message", { length: 500 }).default("Contacta a nuestro equipo para renovar tu suscripción."),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Password reset tokens table
export const passwordResetTokens = pgTable("password_reset_tokens", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").references(() => users.id).notNull(),
  token: text("token").notNull().unique(),
  expiresAt: timestamp("expires_at").notNull(),
  used: boolean("used").default(false),
  createdAt: timestamp("created_at").defaultNow(),
});

// Cash schedule configuration table
// NUEVA ESTRUCTURA DE HORARIOS MÚLTIPLES
export const cashSchedulePeriods = pgTable("cash_schedule_periods", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  dayOfWeek: integer("day_of_week").notNull(), // 1=Lunes, 7=Domingo
  periodName: varchar("period_name", { length: 100 }),
  openHour: integer("open_hour").notNull(),
  openMinute: integer("open_minute").notNull(),
  closeHour: integer("close_hour").notNull(),
  closeMinute: integer("close_minute").notNull(),
  autoOpenEnabled: boolean("auto_open_enabled").default(true),
  autoCloseEnabled: boolean("auto_close_enabled").default(true),
  isActive: boolean("is_active").default(true),
  priorityOrder: integer("priority_order").default(1),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const cashScheduleClientConfig = pgTable("cash_schedule_client_config", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull().unique(),
  timezone: text("timezone").default("America/Argentina/Buenos_Aires"),
  autoScheduleEnabled: boolean("auto_schedule_enabled").default(true),
  notificationEnabled: boolean("notification_enabled").default(false),
  notificationMinutesBefore: integer("notification_minutes_before").default(5),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// MANTENER TABLA ANTIGUA PARA COMPATIBILIDAD (TEMPORAL)
export const cashScheduleConfig = pgTable("cash_schedule_config", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  autoOpenEnabled: boolean("auto_open_enabled").default(false),
  autoCloseEnabled: boolean("auto_close_enabled").default(false),
  openHour: integer("open_hour").default(9),
  openMinute: integer("open_minute").default(0),
  closeHour: integer("close_hour").default(18),
  closeMinute: integer("close_minute").default(0),
  activeDays: text("active_days").default("1,2,3,4,5,6,7"),
  timezone: text("timezone").default("America/Argentina/Buenos_Aires"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Cash auto operations log table
export const cashAutoOperationsLog = pgTable("cash_auto_operations_log", {
  id: serial("id").primaryKey(),
  clientId: integer("client_id").references(() => clients.id).notNull(),
  operationType: text("operation_type").notNull(),
  cashRegisterId: integer("cash_register_id").references(() => cashRegister.id),
  scheduledTime: timestamp("scheduled_time"),
  executedTime: timestamp("executed_time").defaultNow(),
  status: text("status").default("success"),
  errorMessage: text("error_message"),
  reportId: integer("report_id").references(() => dailyReports.id),
  notes: text("notes"),
  createdAt: timestamp("created_at").defaultNow(),
});

// Insert schemas
export const insertClientSchema = createInsertSchema(clients).omit({ id: true, createdAt: true });
export const insertUserSchema = createInsertSchema(users).omit({ id: true, createdAt: true });
export const insertProductSchema = createInsertSchema(products).omit({ id: true, createdAt: true, updatedAt: true });
export const insertOrderSchema = createInsertSchema(orders).omit({ id: true, createdAt: true, updatedAt: true });
export const insertOrderItemSchema = createInsertSchema(orderItems).omit({ id: true, createdAt: true });
export const insertPaymentSchema = createInsertSchema(payments).omit({ id: true, createdAt: true });
export const insertCurrencyExchangeSchema = createInsertSchema(currencyExchanges).omit({ id: true, createdAt: true });
export const insertCashRegisterSchema = createInsertSchema(cashRegister).omit({ id: true, createdAt: true });
export const insertProductHistorySchema = createInsertSchema(productHistory).omit({ id: true, createdAt: true });
export const insertStockControlSessionSchema = createInsertSchema(stockControlSessions).omit({ id: true, createdAt: true });
export const insertStockControlItemSchema = createInsertSchema(stockControlItems).omit({ id: true, scannedAt: true, createdAt: true });
export const insertConfigurationSchema = createInsertSchema(configuration).omit({ id: true, createdAt: true, updatedAt: true });
export const insertCompanyConfigurationSchema = createInsertSchema(companyConfiguration).omit({ id: true, createdAt: true, updatedAt: true });
export const insertVendorSchema = createInsertSchema(vendors).omit({ id: true, createdAt: true, updatedAt: true });
export const insertCustomerSchema = createInsertSchema(customers).omit({ id: true, createdAt: true, updatedAt: true });
export const insertCashMovementSchema = createInsertSchema(cashMovements).omit({ id: true, createdAt: true });
export const insertExpenseSchema = createInsertSchema(expenses).omit({ id: true, createdAt: true });
export const insertDebtPaymentSchema = createInsertSchema(debtPayments).omit({ id: true, createdAt: true });
export const insertDailyReportSchema = createInsertSchema(dailyReports).omit({ id: true, generatedAt: true });
export const insertCustomerDebtSchema = createInsertSchema(customerDebts).omit({ id: true, createdAt: true, updatedAt: true });
export const insertGeneratedReportSchema = createInsertSchema(generatedReports).omit({ id: true, generatedAt: true });
export const insertSystemConfigurationSchema = createInsertSchema(systemConfiguration).omit({ id: true, createdAt: true, updatedAt: true });
export const insertResellerSchema = createInsertSchema(resellers).omit({ id: true, createdAt: true, updatedAt: true });
export const insertResellerSaleSchema = createInsertSchema(resellerSales).omit({ id: true, createdAt: true });
export const insertResellerConfigurationSchema = createInsertSchema(resellerConfiguration).omit({ id: true, createdAt: true, updatedAt: true });
export const insertPasswordResetTokenSchema = createInsertSchema(passwordResetTokens).omit({ id: true, createdAt: true });
// ESQUEMAS PARA NUEVAS TABLAS DE HORARIOS MÚLTIPLES
export const insertCashSchedulePeriodsSchema = createInsertSchema(cashSchedulePeriods).omit({ id: true, createdAt: true, updatedAt: true });
export const insertCashScheduleClientConfigSchema = createInsertSchema(cashScheduleClientConfig).omit({ id: true, createdAt: true, updatedAt: true });

// MANTENER ESQUEMAS ANTIGUOS PARA COMPATIBILIDAD
export const insertCashScheduleConfigSchema = createInsertSchema(cashScheduleConfig).omit({ id: true, createdAt: true, updatedAt: true });
export const insertCashAutoOperationsLogSchema = createInsertSchema(cashAutoOperationsLog).omit({ id: true, createdAt: true });

// Types
export type Client = typeof clients.$inferSelect;
export type User = typeof users.$inferSelect;
export type Product = typeof products.$inferSelect;
export type Order = typeof orders.$inferSelect;
export type OrderItem = typeof orderItems.$inferSelect;
export type Payment = typeof payments.$inferSelect;
export type CurrencyExchange = typeof currencyExchanges.$inferSelect;
export type CashRegister = typeof cashRegister.$inferSelect;
export type ProductHistory = typeof productHistory.$inferSelect;
export type StockControlSession = typeof stockControlSessions.$inferSelect;
export type StockControlItem = typeof stockControlItems.$inferSelect;
export type Configuration = typeof configuration.$inferSelect;
export type CompanyConfiguration = typeof companyConfiguration.$inferSelect;
export type Vendor = typeof vendors.$inferSelect;
export type Customer = typeof customers.$inferSelect;
export type CashMovement = typeof cashMovements.$inferSelect;
export type Expense = typeof expenses.$inferSelect;
export type DebtPayment = typeof debtPayments.$inferSelect;
export type DailyReport = typeof dailyReports.$inferSelect;
export type CustomerDebt = typeof customerDebts.$inferSelect;
export type GeneratedReport = typeof generatedReports.$inferSelect;
export type SystemConfiguration = typeof systemConfiguration.$inferSelect;
export type Reseller = typeof resellers.$inferSelect;
export type ResellerSale = typeof resellerSales.$inferSelect;
export type ResellerConfiguration = typeof resellerConfiguration.$inferSelect;
export type PasswordResetToken = typeof passwordResetTokens.$inferSelect;
export type CashScheduleConfig = typeof cashScheduleConfig.$inferSelect;
export type CashAutoOperationsLog = typeof cashAutoOperationsLog.$inferSelect;

export type InsertClient = z.infer<typeof insertClientSchema>;
export type InsertUser = z.infer<typeof insertUserSchema>;
export type InsertProduct = z.infer<typeof insertProductSchema>;
export type InsertOrder = z.infer<typeof insertOrderSchema>;
export type InsertOrderItem = z.infer<typeof insertOrderItemSchema>;
export type InsertPayment = z.infer<typeof insertPaymentSchema>;
export type InsertCurrencyExchange = z.infer<typeof insertCurrencyExchangeSchema>;
export type InsertCashRegister = z.infer<typeof insertCashRegisterSchema>;
export type InsertProductHistory = z.infer<typeof insertProductHistorySchema>;
export type InsertStockControlSession = z.infer<typeof insertStockControlSessionSchema>;
export type InsertStockControlItem = z.infer<typeof insertStockControlItemSchema>;
export type InsertConfiguration = z.infer<typeof insertConfigurationSchema>;
export type InsertCompanyConfiguration = z.infer<typeof insertCompanyConfigurationSchema>;
export type InsertVendor = z.infer<typeof insertVendorSchema>;
export type InsertCustomer = z.infer<typeof insertCustomerSchema>;
export type InsertCashMovement = z.infer<typeof insertCashMovementSchema>;
export type InsertExpense = z.infer<typeof insertExpenseSchema>;
export type InsertDebtPayment = z.infer<typeof insertDebtPaymentSchema>;
export type InsertDailyReport = z.infer<typeof insertDailyReportSchema>;
export type InsertCustomerDebt = z.infer<typeof insertCustomerDebtSchema>;
export type InsertGeneratedReport = z.infer<typeof insertGeneratedReportSchema>;
export type InsertSystemConfiguration = z.infer<typeof insertSystemConfigurationSchema>;
export type InsertReseller = z.infer<typeof insertResellerSchema>;
export type InsertResellerSale = z.infer<typeof insertResellerSaleSchema>;
export type InsertResellerConfiguration = z.infer<typeof insertResellerConfigurationSchema>;
export type InsertPasswordResetToken = z.infer<typeof insertPasswordResetTokenSchema>;
export type InsertCashScheduleConfig = z.infer<typeof insertCashScheduleConfigSchema>;
export type InsertCashAutoOperationsLog = z.infer<typeof insertCashAutoOperationsLogSchema>;
