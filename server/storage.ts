import {
  clients,
  users,
  products,
  orders,
  orderItems,
  payments,
  currencyExchanges,
  cashRegister,
  cashMovements,
  expenses,
  debtPayments,
  dailyReports,
  generatedReports,
  customerDebts,
  productHistory,
  stockControlSessions,
  stockControlItems,
  configuration,
  companyConfiguration,
  vendors,
  customers,
  systemConfiguration,
  resellers,
  resellerSales,
  resellerConfiguration,
  type Client,
  type User,
  type Product,
  type Order,
  type OrderItem,
  type Payment,
  type CurrencyExchange,
  type CashRegister,
  type CashMovement,
  type Expense,
  type DebtPayment,
  type DailyReport,
  type GeneratedReport,
  type CustomerDebt,
  type ProductHistory,
  type StockControlSession,
  type StockControlItem,
  type Configuration,
  type CompanyConfiguration,
  type Vendor,
  type Customer,
  type SystemConfiguration,
  type Reseller,
  type ResellerSale,
  type ResellerConfiguration,
  insertClientSchema,
  insertUserSchema,
  insertProductSchema,
  insertOrderSchema,
  insertOrderItemSchema,
  insertPaymentSchema,
  insertCurrencyExchangeSchema,
  insertCashRegisterSchema,
  insertCashMovementSchema,
  insertExpenseSchema,
  insertDebtPaymentSchema,
  insertDailyReportSchema,
  insertGeneratedReportSchema,
  insertCustomerDebtSchema,
  insertProductHistorySchema,
  insertStockControlSessionSchema,
  insertStockControlItemSchema,
  insertConfigurationSchema,
  insertCompanyConfigurationSchema,
  insertVendorSchema,
  insertCustomerSchema,
  insertSystemConfigurationSchema,
  insertResellerSchema,
  insertResellerSaleSchema,
  insertResellerConfigurationSchema,
  passwordResetTokens,
  insertPasswordResetTokenSchema,
} from "@shared/schema";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import bcrypt from "bcryptjs";
import { eq, and, gte, lte, lt, like, ilike, or, desc, inArray, sum, count, not } from "drizzle-orm";
import { sql as sqlOperator } from "drizzle-orm";

type InsertClient = typeof insertClientSchema._type;
type InsertUser = typeof insertUserSchema._type;
type InsertProduct = typeof insertProductSchema._type;
type InsertOrder = typeof insertOrderSchema._type;
type InsertOrderItem = typeof insertOrderItemSchema._type;
type InsertPayment = typeof insertPaymentSchema._type;
type InsertCurrencyExchange = typeof insertCurrencyExchangeSchema._type;
type InsertCashRegister = typeof insertCashRegisterSchema._type;
type InsertCashMovement = typeof insertCashMovementSchema._type;
type InsertExpense = typeof insertExpenseSchema._type;
type InsertDebtPayment = typeof insertDebtPaymentSchema._type;
type InsertDailyReport = typeof insertDailyReportSchema._type;
type InsertGeneratedReport = typeof insertGeneratedReportSchema._type;
type InsertCustomerDebt = typeof insertCustomerDebtSchema._type;
type InsertProductHistory = typeof insertProductHistorySchema._type;
type InsertStockControlSession = typeof insertStockControlSessionSchema._type;
type InsertStockControlItem = typeof insertStockControlItemSchema._type;
type InsertConfiguration = typeof insertConfigurationSchema._type;
type InsertCompanyConfiguration = typeof insertCompanyConfigurationSchema._type;
type InsertVendor = typeof insertVendorSchema._type;
type InsertCustomer = typeof insertCustomerSchema._type;
type InsertReseller = typeof insertResellerSchema._type;
type InsertResellerSale = typeof insertResellerSaleSchema._type;
type InsertResellerConfiguration = typeof insertResellerConfigurationSchema._type;
type InsertPasswordResetToken = typeof insertPasswordResetTokenSchema._type;

export interface IStorage {
  // Clients
  createClient(client: InsertClient): Promise<Client>;
  getClientById(id: number): Promise<Client | undefined>;
  getAllClients(): Promise<Client[]>;
  getAllClientsWithAdmins(): Promise<any[]>;
  updateClient(id: number, client: Partial<InsertClient>): Promise<Client | undefined>;
  deleteClient(id: number): Promise<boolean>;
  getClientMovementsSummary(clientId: number): Promise<any>;

  // Users
  createUser(user: InsertUser): Promise<User>;
  getUserById(id: number): Promise<User | undefined>;
  getUserByEmail(email: string): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  getUsersByClientId(clientId: number): Promise<User[]>;
  getUsersByClientIdAndRole(clientId: number, role: string): Promise<User[]>;
  updateUser(id: number, user: Partial<InsertUser>, skipPasswordHash?: boolean): Promise<User | undefined>;
  deleteUser(id: number): Promise<boolean>;

  // Products
  createProduct(product: InsertProduct): Promise<Product>;
  getProductById(id: number): Promise<Product | undefined>;
  getProductsByClientId(clientId: number): Promise<Product[]>;
  getProductByImei(imei: string, clientId: number): Promise<Product | undefined>;
  updateProduct(id: number, product: Partial<InsertProduct>, userId?: number): Promise<Product | undefined>;
  updateProductsByImeis(imeis: string[], clientId: number, updates: Partial<InsertProduct>, userId: number): Promise<any>;
  deleteProduct(id: number): Promise<boolean>;
  searchProducts(clientId: number, filters: {
    search?: string;
    status?: string;
    storage?: string;
    model?: string;
    quality?: string;
    battery?: string;
    provider?: string;
    priceMin?: number;
    priceMax?: number;
    limit?: number;
    offset?: number;
  }): Promise<Product[]>;

  // Product History
  createProductHistory(history: InsertProductHistory): Promise<ProductHistory>;
  getProductHistoryByProductId(productId: number): Promise<ProductHistory[]>;
  getProductHistoryByClientId(clientId: number): Promise<ProductHistory[]>;
  getProductsWithAlerts(clientId: number): Promise<any[]>;

  // Orders
  createOrder(order: InsertOrder): Promise<Order>;
  getOrderById(id: number): Promise<Order | undefined>;
  getOrdersByClientId(clientId: number): Promise<Order[]>;
  getOrdersByClientIdWithVendor(clientId: number): Promise<any[]>;
  getOrdersWithItemsAndProducts(clientId: number, vendorId?: number | null): Promise<any[]>;
  getOrdersByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Order[]>;
  updateOrder(id: number, order: Partial<InsertOrder>): Promise<Order | undefined>;
  deleteOrder(id: number): Promise<boolean>;

  // Order Items
  createOrderItem(orderItem: InsertOrderItem): Promise<OrderItem>;
  getOrderItemsByOrderId(orderId: number): Promise<OrderItem[]>;
  getOrderItemsWithProductsByOrderId(orderId: number): Promise<any[]>;
  deleteOrderItem(id: number): Promise<boolean>;

  // Payments
  createPayment(payment: InsertPayment): Promise<Payment>;
  getPaymentsByOrderId(orderId: number): Promise<Payment[]>;
  getPaymentsByClientId(clientId: number): Promise<Payment[]>;
  getPaymentsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Payment[]>;

  // Currency Exchanges
  createCurrencyExchange(exchange: InsertCurrencyExchange): Promise<CurrencyExchange>;
  getCurrencyExchangesByClientId(clientId: number): Promise<CurrencyExchange[]>;
  getCurrencyExchangesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CurrencyExchange[]>;

  // Cash Register - Enhanced
  createCashRegister(cashRegister: InsertCashRegister): Promise<CashRegister>;
  getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined>;
  getCashRegisterByDate(clientId: number, dateStr: string): Promise<CashRegister | undefined>;
  updateCashRegister(id: number, cashRegister: Partial<InsertCashRegister>): Promise<CashRegister | undefined>;
  getDailySales(clientId: number, startDate: Date, endDate: Date): Promise<number>;
  getOrCreateTodayCashRegister(clientId: number): Promise<CashRegister>;
  autoCloseCashRegister(clientId: number): Promise<CashRegister | undefined>;
  createAutoDailyReport(clientId: number, cashRegister: any, realTimeState: any): Promise<void>;
  getPreviousDayClosingBalance(clientId: number): Promise<number>;
  isSameDay(date1: Date, date2: Date): boolean;

  // Cash Movements
  createCashMovement(movement: InsertCashMovement): Promise<CashMovement>;
  getCashMovementsByClientId(clientId: number): Promise<CashMovement[]>;
  getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]>;
  getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]>;
  getCashMovementsWithFilters(clientId: number, filters: {
    type?: string;
    dateFrom?: Date;
    dateTo?: Date;
    customer?: string;
    vendor?: string;
    search?: string;
    paymentMethod?: string;
  }): Promise<CashMovement[]>;
  getAllCashMovementsForExport(clientId: number): Promise<CashMovement[]>;
  getAllCashMovementsForCompleteExport(clientId: number): Promise<any[]>;

  // Expenses
  createExpense(expense: InsertExpense): Promise<Expense>;
  getExpensesByClientId(clientId: number): Promise<Expense[]>;
  getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]>;
  getExpensesByCategory(clientId: number, category: string): Promise<Expense[]>;
  updateExpense(id: number, expense: Partial<InsertExpense>): Promise<Expense | undefined>;
  deleteExpense(id: number): Promise<boolean>;

  // Customer Debts
  createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt>;
  getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]>;
  getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]>;
  getActiveDebts(clientId: number): Promise<CustomerDebt[]>;
  getActiveDebtByOrderId(orderId: number): Promise<CustomerDebt | undefined>;
  updateCustomerDebt(id: number, debt: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined>;

  // Debt Payments
  createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment>;
  getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]>;
  getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]>;

  // Daily Reports
  createDailyReport(report: InsertDailyReport): Promise<DailyReport>;
  getDailyReportsByClientId(clientId: number): Promise<DailyReport[]>;
  getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined>;
  generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport>;
  getDailyReports(clientId: number, limit?: number): Promise<any[]>;

  // Generated Reports
  createGeneratedReport(report: InsertGeneratedReport): Promise<GeneratedReport>;
  getGeneratedReportsByClientId(clientId: number): Promise<GeneratedReport[]>;
  getGeneratedReportById(id: number): Promise<GeneratedReport | undefined>;
  generateAutoExcelReport(clientId: number, dailyReportId: number, reportDate: Date): Promise<void>;

  // Real-time Cash State
  getRealTimeCashState(clientId: number): Promise<any>;
  getTotalDebtsAmount(clientId: number): Promise<number>;
  getTotalPendingVendorPayments(clientId: number): Promise<number>;
  getStockValue(clientId: number): Promise<{usd: number, ars: number}>;

  // Configuration
  createConfiguration(config: InsertConfiguration): Promise<Configuration>;
  getConfigurationByKey(clientId: number, key: string): Promise<Configuration | undefined>;
  getConfigurationsByClientId(clientId: number): Promise<Configuration[]>;
  updateConfiguration(clientId: number, key: string, value: string): Promise<Configuration>;

  // Company Configuration
  createCompanyConfiguration(config: InsertCompanyConfiguration): Promise<CompanyConfiguration>;
  getCompanyConfigurationByClientId(clientId: number): Promise<CompanyConfiguration | undefined>;
  updateCompanyConfiguration(id: number, config: Partial<InsertCompanyConfiguration>): Promise<CompanyConfiguration | undefined>;

  // Vendors
  createVendor(vendor: InsertVendor): Promise<Vendor>;
  getVendorById(id: number): Promise<Vendor | undefined>;
  getVendorsByClientId(clientId: number): Promise<Vendor[]>;
  updateVendor(id: number, vendor: Partial<InsertVendor>): Promise<Vendor | undefined>;
  deleteVendor(id: number): Promise<boolean>;

  // Customers
  createCustomer(customer: InsertCustomer): Promise<Customer>;
  getCustomerById(id: number): Promise<Customer | undefined>;
  getCustomersByClientId(clientId: number): Promise<Customer[]>;
  updateCustomer(id: number, customer: Partial<InsertCustomer>): Promise<Customer | undefined>;
  deleteCustomer(id: number): Promise<boolean>;

  // Stock Control
  createStockControlSession(session: InsertStockControlSession): Promise<StockControlSession>;
  getStockControlSessionById(id: number): Promise<StockControlSession | undefined>;
  getActiveStockControlSession(clientId: number): Promise<StockControlSession | undefined>;
  getStockControlSessionsByClientId(clientId: number): Promise<StockControlSession[]>;
  updateStockControlSession(id: number, session: Partial<InsertStockControlSession>): Promise<StockControlSession | undefined>;

  createStockControlItem(item: InsertStockControlItem): Promise<StockControlItem>;
  getStockControlItemsBySessionId(sessionId: number): Promise<StockControlItem[]>;
  getStockControlItemsWithProductsBySessionId(sessionId: number): Promise<any[]>;
  updateStockControlItem(id: number, item: Partial<InsertStockControlItem>): Promise<StockControlItem | undefined>;

  getProductsForStockControl(clientId: number): Promise<Product[]>;
  getMissingProductsFromSession(sessionId: number): Promise<Product[]>;
  getExtraviosProducts(clientId: number): Promise<Product[]>;

  // System Configuration
  getSystemConfiguration(): Promise<SystemConfiguration | undefined>;
  updateSystemConfiguration(config: Partial<SystemConfiguration>): Promise<SystemConfiguration | undefined>;

  // Resellers
  createReseller(reseller: InsertReseller): Promise<Reseller>;
  getResellers(): Promise<Reseller[]>;
  getResellerById(id: number): Promise<Reseller | undefined>;
  getResellerByEmail(email: string): Promise<Reseller | undefined>;
  updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined>;
  deleteReseller(id: number): Promise<boolean>;

  // Reseller Sales
  createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale>;
  getResellerSales(): Promise<ResellerSale[]>;
  getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]>;
  getClientsByResellerId(resellerId: number): Promise<any[]>;
  getResellerStats(resellerId: number): Promise<any>;

  // Reseller Configuration
  getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined>;
  updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined>;

  // Password Reset Tokens
  createPasswordResetToken(token: InsertPasswordResetToken): Promise<any>;
  getPasswordResetToken(token: string): Promise<any>;
  markPasswordResetTokenAsUsed(id: number): Promise<boolean>;

  // Vendor Performance
  getVendorPerformanceRanking(clientId: number): Promise<any[]>;

  // Auto-sync Monitor Methods
  getOrdersByDate(clientId: number, date: string): Promise<Order[]>;
  getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]>;

  // Automatic Cash Scheduling
  scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }>;
  checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }>;
}

// Initialize storage after the class is defined
const storage = new DrizzleStorage();

// Export storage instance
export { storage };
export { db };
export { MemStorage };

// Initialize database connection for PRODUCTION VPS PostgreSQL
const connectionString = process.env.DATABASE_URL || "postgresql://stockcel_software:Kc5bpdfkr@localhost:5432/stockcel_software";
if (!connectionString) {
  throw new Error("DATABASE_URL environment variable is required for production");
}

// Production PostgreSQL pool configuration
const pool = new Pool({
  connectionString,
  ssl: false, // VPS PostgreSQL doesn't require SSL
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const db = drizzle(pool, {
  schema: {
    clients,
    users,
    products,
    orders,
    orderItems,
    payments,
    currencyExchanges,
    cashRegister,
    cashMovements,
    expenses,
    debtPayments,
    dailyReports,
    generatedReports,
    customerDebts,
    productHistory,
    stockControlSessions,
    stockControlItems,
    configuration,
    companyConfiguration,
    vendors,
    customers,
    systemConfiguration,
    resellers,
    resellerSales,
    resellerConfiguration,
    passwordResetTokens,
  }
});

// Test database connection on startup
pool.connect((err, client, release) => {
  if (err) {
    console.error('❌ Error connecting to PostgreSQL database:', err);
    process.exit(1);
  } else {
    console.log('✅ Connected to production PostgreSQL database');
    release();
  }
});

class DrizzleStorage implements IStorage {
  private db = db; // Alias for drizzle instance
  private paymentsTable = payments;
  private customerDebtsTable = customerDebts;
  private debtPaymentsTable = debtPayments;
  private expensesTable = expenses;
  private cashMovementsTable = cashMovements;
  private productsTable = products;
  private usersTable = users;
  private clientsTable = clients;
  private ordersTable = orders;
  private vendorsTable = vendors;
  private customersTable = customers;
  private cashRegisterTable = cashRegister;
  private stockControlSessionsTable = stockControlSessions;
  private stockControlItemsTable = stockControlItems;
  private dailyReportsTable = dailyReports;
  private generatedReportsTable = generatedReports;
  private configurationTable = configuration;
  private companyConfigurationTable = companyConfiguration;
  private resellerConfigurationTable = resellerConfiguration;
  private resellerSalesTable = resellerSales;
  private resellersTable = resellers;
  private passwordResetTokensTable = passwordResetTokens;

  // Clients
  async createClient(client: InsertClient): Promise<Client> {
    const [result] = await this.db.insert(this.clientsTable).values(client).returning();
    return result;
  }

  async getClientById(id: number): Promise<Client | undefined> {
    const [result] = await this.db.select().from(this.clientsTable).where(eq(this.clientsTable.id, id));
    return result;
  }

  async getAllClients(): Promise<Client[]> {
    return await this.db.select().from(this.clientsTable);
  }

  async getAllClientsWithAdmins(): Promise<any[]> {
    const allClients = await this.db.select().from(this.clientsTable);
    const result = [];

    for (const client of allClients) {
      // Get admin user for this client
      const [adminUser] = await this.db.select()
        .from(this.usersTable)
        .where(and(eq(this.usersTable.clientId, client.id), eq(this.usersTable.role, 'admin')))
        .limit(1);

      result.push({
        ...client,
        adminUsername: adminUser?.username || 'No admin',
        adminName: adminUser?.username || 'No admin',
        adminEmail: adminUser?.email || '',
      });
    }

    return result;
  }

  async updateClient(id: number, client: Partial<InsertClient>): Promise<Client | undefined> {
    // If email is being updated, check if it already exists in another client
    if (client.email) {
      // Get current client to compare emails
      const currentClient = await this.db.select().from(this.clientsTable).where(eq(this.clientsTable.id, id)).limit(1);

      // Only check for duplicates if the email is actually being changed
      if (currentClient.length > 0 && currentClient[0].email !== client.email) {
        const existingClient = await this.db.select()
          .from(this.clientsTable)
          .where(eq(this.clientsTable.email, client.email))
          .limit(1);

        if (existingClient.length > 0) {
          throw new Error(`Email ${client.email} already exists for another client`);
        }
      }
    }

    const [result] = await this.db.update(this.clientsTable).set(client).where(eq(this.clientsTable.id, id)).returning();
    return result;
  }

  async getClientMovementsSummary(clientId: number): Promise<any> {
    try {
      // Count different types of data for this client
      const [productsCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.productsTable).where(eq(this.productsTable.clientId, clientId));
      const [ordersCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.ordersTable).where(eq(this.ordersTable.clientId, clientId));
      const [usersCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.usersTable).where(eq(this.usersTable.clientId, clientId));
      const [customersCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.customersTable).where(eq(this.customersTable.clientId, clientId));
      const [cashMovementsCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.cashMovementsTable).where(eq(this.cashMovementsTable.clientId, clientId));
      const [paymentsCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.paymentsTable).where(eq(this.paymentsTable.clientId, clientId));
      const [vendorsCount] = await this.db.select({ count: sqlOperator`count(*)` }).from(this.vendorsTable).where(eq(this.vendorsTable.clientId, clientId));

      return {
        products: parseInt(productsCount.count.toString()),
        orders: parseInt(ordersCount.count.toString()),
        users: parseInt(usersCount.count.toString()),
        customers: parseInt(customersCount.count.toString()),
        cashMovements: parseInt(cashMovementsCount.count.toString()),
        payments: parseInt(paymentsCount.count.toString()),
        vendors: parseInt(vendorsCount.count.toString()),
        hasMovements: (
          parseInt(productsCount.count.toString()) > 0 ||
          parseInt(ordersCount.count.toString()) > 0 ||
          parseInt(cashMovementsCount.count.toString()) > 0 ||
          parseInt(paymentsCount.count.toString()) > 0
        )
      };
    } catch (error) {
      console.error('Error getting client movements summary:', error);
      throw error;
    }
  }

  async deleteClient(id: number): Promise<boolean> {
    try {
      // Delete in proper order respecting foreign key constraints

      // 1. First delete records that reference users via user_id
      // Delete cash movements (they have user_id foreign key)
      await this.db.delete(this.cashMovementsTable).where(eq(this.cashMovementsTable.clientId, id));

      // Delete expenses (they have user_id foreign key)
      await this.db.delete(this.expensesTable).where(eq(this.expensesTable.clientId, id));

      // Delete debt payments (they have user_id foreign key)
      await this.db.delete(this.debtPaymentsTable).where(eq(this.debtPaymentsTable.clientId, id));

      // 2. Delete order-related data
      // Get all order IDs for this client first
      const clientOrders = await this.db.select({ id: this.ordersTable.id }).from(this.ordersTable).where(eq(this.ordersTable.clientId, id));
      const orderIds = clientOrders.map(order => order.id);

      // Delete customer debts (they reference orders)
      await this.db.delete(this.customerDebtsTable).where(eq(this.customerDebtsTable.clientId, id));

      // Delete order items (they reference orders and products)
      if (orderIds.length > 0) {
        await this.db.delete(this.orderItems).where(inArray(this.orderItems.orderId, orderIds));
      }

      // Delete payments (they reference orders)
      await this.db.delete(this.paymentsTable).where(eq(this.paymentsTable.clientId, id));

      // Delete orders
      await this.db.delete(this.ordersTable).where(eq(this.ordersTable.clientId, id));

      // 3. Delete other client data
      await this.db.delete(this.productsTable).where(eq(this.productsTable.clientId, id));
      await this.db.delete(this.cashRegisterTable).where(eq(this.cashRegisterTable.clientId, id));
      await this.db.delete(this.companyConfigurationTable).where(eq(this.companyConfigurationTable.clientId, id));
      await this.db.delete(this.vendorsTable).where(eq(this.vendorsTable.clientId, id));
      await this.db.delete(this.customersTable).where(eq(this.customersTable.clientId, id));
      await this.db.delete(this.customerDebtsTable).where(eq(this.customerDebtsTable.clientId, id));
      await this.db.delete(this.dailyReportsTable).where(eq(this.dailyReportsTable.clientId, id));
      await this.db.delete(this.generatedReportsTable).where(eq(this.generatedReportsTable.clientId, id));

      // 4. Finally delete users (no longer referenced by anything)
      await this.db.delete(this.usersTable).where(eq(this.usersTable.clientId, id));

      // 5. Delete the client itself
      const result = await this.db.delete(this.clientsTable).where(eq(this.clientsTable.id, id));
      return result.rowCount > 0;
    } catch (error) {
      console.error('Error deleting client:', error);
      return false;
    }
  }

  // Users - PRODUCTION SECURITY
  async createUser(user: InsertUser): Promise<User> {
    // Hash password with production security if provided
    const userData = { ...user };
    if (userData.password) {
      userData.password = bcrypt.hashSync(userData.password, 12);
    }

    const [result] = await this.db.insert(this.usersTable).values(userData).returning();
    console.log(`✅ User created: ${user.email} with role: ${user.role}`);
    return result;
  }

  async getUserById(id: number): Promise<User | undefined> {
    const [result] = await this.db.select().from(this.usersTable).where(eq(this.usersTable.id, id));
    return result;
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    try {
      console.log(`🔍 Searching for user by email: ${email}`);
      const [result] = await this.db.select().from(this.usersTable).where(eq(this.usersTable.email, email));
      console.log(`📊 getUserByEmail result:`, result ? `Found user ID ${result.id}` : 'No user found');
      return result;
    } catch (error) {
      console.error(`❌ Error in getUserByEmail for ${email}:`, error);
      return undefined;
    }
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    const [result] = await this.db.select().from(this.usersTable).where(eq(this.usersTable.username, username));
    return result;
  }

  async getUsersByClientId(clientId: number): Promise<User[]> {
    return await this.db.select().from(this.usersTable).where(eq(this.usersTable.clientId, clientId));
  }

  async getUsersByClientIdAndRole(clientId: number, role: string): Promise<User[]> {
    return await this.db.select().from(this.usersTable).where(
      and(eq(this.usersTable.clientId, clientId), eq(this.usersTable.role, role))
    );
  }

  async updateUser(id: number, user: Partial<InsertUser>, skipPasswordHash: boolean = false): Promise<User | undefined> {
    // Validate user ID
    if (!id || id <= 0) {
      throw new Error('Invalid user ID');
    }

    // Hash password with enhanced security
    const userData = { ...user };
    if (userData.password && !skipPasswordHash) {
      // Validate password strength
      if (userData.password.length < 8) {
        throw new Error('Password must be at least 8 characters long');
      }

      // Only hash if password doesn't start with $2b$ (bcrypt hash format)
      if (!userData.password.startsWith('$2b$')) {
        // Increased salt rounds for better security
        userData.password = bcrypt.hashSync(userData.password, 14);
        console.log(`🔐 Password hashed for user ID: ${id}`);
      } else {
        console.log(`⚠️ Password already hashed for user ID: ${id}, skipping hash`);
      }
    }

    // Validate email format if provided
    if (userData.email) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(userData.email)) {
        throw new Error('Invalid email format');
      }
    }

    const [result] = await this.db.update(this.usersTable).set(userData).where(eq(this.usersTable.id, id)).returning();
    console.log(`✅ User updated successfully: ID ${id}`);
    return result;
  }

  async deleteUser(id: number): Promise<boolean> {
    const result = await this.db.delete(this.usersTable).where(eq(this.usersTable.id, id));
    return result.rowCount > 0;
  }

  // Products
  async createProduct(product: InsertProduct): Promise<Product> {
    const [result] = await this.db.insert(this.productsTable).values(product).returning();
    return result;
  }

  async getProductById(id: number): Promise<Product | undefined> {
    const [result] = await this.db.select().from(this.productsTable).where(eq(this.productsTable.id, id));
    return result;
  }

  async getProductsByClientId(clientId: number): Promise<Product[]> {
    return await this.db.select().from(this.productsTable).where(eq(this.productsTable.clientId, clientId));
  }

  async getProductByImei(imei: string, clientId: number): Promise<Product | undefined> {
    const [result] = await this.db.select().from(this.productsTable).where(
      and(eq(this.productsTable.imei, imei), eq(this.productsTable.clientId, clientId))
    );
    return result;
  }

  async updateProduct(id: number, product: Partial<InsertProduct>, userId?: number): Promise<Product | undefined> {
    // Get the existing product first to track changes
    const [existing] = await this.db.select().from(this.productsTable).where(eq(this.productsTable.id, id));
    if (!existing) return undefined;

    // Update the product
    const [result] = await this.db.update(this.productsTable)
      .set({ ...product, updatedAt: new Date() })
      .where(eq(this.productsTable.id, id))
      .returning();

    // Create history record if userId is provided
    if (userId && result) {
      const changes = [];
      const fieldNames = {
        'status': 'Estado',
        'costPrice': 'Precio Costo',
        'quality': 'Calidad',
        'battery': 'Batería',
        'provider': 'Proveedor',
        'observations': 'Observaciones',
        'model': 'Modelo',
        'storage': 'Almacenamiento',
        'color': 'Color',
        'imei': 'IMEI'
      };

      const fieldsToTrack = ['status', 'costPrice', 'quality', 'battery', 'provider', 'observations', 'model', 'storage', 'color', 'imei'];

      for (const field of fieldsToTrack) {
        if (product[field as keyof typeof product] !== undefined && product[field as keyof typeof product] !== existing[field as keyof typeof existing]) {
          const oldValue = existing[field as keyof typeof existing] || 'vacío';
          const newValue = product[field as keyof typeof product] || 'vacío';
          const fieldName = fieldNames[field as keyof typeof fieldNames] || field;
          changes.push(`${fieldName}: ${oldValue} → ${newValue}`);
        }
      }

      if (changes.length > 0) {
        try {
          await this.createProductHistory({
            clientId: existing.clientId,
            productId: id,
            previousStatus: existing.status,
            newStatus: product.status || existing.status,
            userId: userId,
            notes: `Cambios: ${changes.join(', ')}`,
          });
        } catch (error) {
          console.error('Error creating product history:', error);
        }
      }
    }

    return result;
  }

  async updateProductsByImeis(imeis: string[], clientId: number, updates: Partial<InsertProduct>, userId: number): Promise<any> {
    console.log('DrizzleStorage: Updating products by IMEIs:', {imeis, clientId, updates, userId });

    try {
      // Get existing products first to track changes
      const existingProducts = await this.db.select()
        .from(this.productsTable)
        .where(
          and(
            inArray(this.productsTable.imei, imeis),
            eq(this.productsTable.clientId, clientId)
          )
        );

      // Update products matching the IMEIs
      const result = await this.db.update(this.productsTable)
        .set({
          ...updates,
          updatedAt: new Date()
        })
        .where(
          and(
            inArray(this.productsTable.imei, imeis),
            eq(this.productsTable.clientId, clientId)
          )
        )
        .returning();

      // Create history records for each updated product
      for (const existingProduct of existingProducts) {
        const changes = [];
        const fieldNames = {
          'status': 'Estado',
          'costPrice': 'Precio Costo',
          'quality': 'Calidad',
          'battery': 'Batería',
          'provider': 'Proveedor',
          'observations': 'Observaciones',
          'model': 'Modelo',
          'storage': 'Almacenamiento',
          'color': 'Color',
          'imei': 'IMEI'
        };

        const fieldsToTrack = ['status', 'costPrice', 'quality', 'battery', 'provider', 'observations', 'model', 'storage', 'color', 'imei'];

        for (const field of fieldsToTrack) {
          if (updates[field as keyof typeof updates] !== undefined && updates[field as keyof typeof updates] !== existingProduct[field as keyof typeof existingProduct]) {
            const oldValue = existingProduct[field as keyof typeof existingProduct] || 'vacío';
            const newValue = updates[field as keyof typeof updates] || 'vacío';
            const fieldName = fieldNames[field as keyof typeof fieldNames] || field;
            changes.push(`${fieldName}: ${oldValue} → ${newValue}`);
          }
        }

        if (changes.length > 0) {
          try {
            await this.createProductHistory({
              clientId: existingProduct.clientId,
              productId: existingProduct.id,
              previousStatus: existingProduct.status,
              newStatus: updates.status || existingProduct.status,
              userId: userId,
              notes: `Actualización masiva - ${changes.join(', ')}`,
            });
          } catch (error) {
            console.error('Error creating product history for IMEI:', existingProduct.imei, error);
          }
        }
      }

      console.log('DrizzleStorage: Batch update completed:', result.length, 'products updated');
      return {
        success: true,
        updatedCount: result.length,
        updatedProducts: result
      };
    } catch (error) {
      console.error('DrizzleStorage: Error in batch update:', error);
      throw error;
    }
  }

  async deleteProduct(id: number): Promise<boolean> {
    const result = await this.db.delete(this.productsTable).where(eq(this.productsTable.id, id));
    return result.rowCount > 0;
  }

  async searchProducts(clientId: number, filters: {
    search?: string;
    status?: string;
    storage?: string;
    model?: string;
    quality?: string;
    battery?: string;
    provider?: string;
    priceMin?: number;
    priceMax?: number;
    limit?: number;
    offset?: number;
  }): Promise<Product[]> {
    // Input validation
    if (!clientId || clientId <= 0) {
      throw new Error('Invalid client ID');
    }

    const conditions = [eq(this.productsTable.clientId, clientId)];

    // Sanitize and validate search inputs
    if (filters.search) {
      const sanitizedSearch = filters.search.trim().substring(0, 100);
      conditions.push(
        or(
          ilike(this.productsTable.model, `%${sanitizedSearch}%`),
          ilike(this.productsTable.imei, `%${sanitizedSearch}%`),
          ilike(this.productsTable.provider, `%${sanitizedSearch}%`)
        )
      );
    }

    if (filters.status) {
      const validStatuses = ['disponible', 'reservado', 'vendido', 'tecnico_interno', 'tecnico_externo', 'a_reparar', 'extravio'];
      if (validStatuses.includes(filters.status)) {
        conditions.push(eq(this.productsTable.status, filters.status));
      }
    }

    if (filters.storage) {
      conditions.push(eq(this.productsTable.storage, filters.storage));
    }

    if (filters.model) {
      const sanitizedModel = filters.model.trim().substring(0, 100);
      conditions.push(ilike(this.productsTable.model, `%${sanitizedModel}%`));
    }

    if (filters.quality) {
      conditions.push(eq(this.productsTable.quality, filters.quality));
    }

    if (filters.battery) {
      conditions.push(eq(this.productsTable.battery, filters.battery));
    }

    if (filters.provider) {
      const sanitizedProvider = filters.provider.trim().substring(0, 100);
      conditions.push(ilike(this.productsTable.provider, `%${sanitizedProvider}%`));
    }

    if (filters.priceMin !== undefined && filters.priceMin >= 0) {
      conditions.push(gte(this.productsTable.costPrice, filters.priceMin.toString()));
    }

    if (filters.priceMax !== undefined && filters.priceMax >= 0) {
      conditions.push(lte(this.productsTable.costPrice, filters.priceMax.toString()));
    }

    let query = this.db.select().from(this.productsTable).where(and(...conditions));

    // Add pagination with reasonable defaults
    const limit = Math.min(filters.limit || 100, 1000); // Max 1000 records
    const offset = Math.max(filters.offset || 0, 0);

    query = query.limit(limit).offset(offset);

    return await query.orderBy(desc(this.productsTable.updatedAt));
  }

  // Payment methods
  async createPayment(payment: InsertPayment): Promise<Payment> {
    console.log('DrizzleStorage: Creating payment with data:', payment);
    const [result] = await this.db.insert(this.paymentsTable).values(payment).returning();
    console.log('DrizzleStorage: Payment created successfully:', result);

    // CORRECCIÓN CRÍTICA: No crear movimientos automáticos aquí para evitar duplicación
    // Los movimientos de caja se crean desde los endpoints específicos (/api/debt-payments)
    console.log('DrizzleStorage: ✅ Payment created WITHOUT automatic cash movement to prevent duplication');

    return result;
  }

  // Helper function to extract currency from payment method
  private getCurrencyFromPaymentMethod(paymentMethod: string): string {
    // Handle financiera cases specifically
    if (paymentMethod === 'financiera_usd') {
      return 'USD'; // Financiera payment in USD (no conversion needed)
    } else if (paymentMethod === 'financiera_ars') {
      return 'ARS'; // Financiera payment in ARS (needs conversion to USD)
    } else if (paymentMethod.includes('_usd')) {
      return 'USD';
    } else if (paymentMethod.includes('_ars')) {
      return 'ARS';
    } else if (paymentMethod.includes('_usdt')) {
      return 'USDT';
    }
    return 'USD'; // Default
  }

  async getPaymentsByOrderId(orderId: number): Promise<Payment[]> {
    console.log('DrizzleStorage: Getting payments for order ID:', orderId);
    const result = await this.db.select().from(this.paymentsTable).where(eq(this.paymentsTable.orderId, orderId));
    console.log('DrizzleStorage: Found payments:', result);
    return result;
  }

  async getPaymentsByClientId(clientId: number): Promise<Payment[]> {
    return await this.db.select().from(this.paymentsTable).where(eq(this.paymentsTable.clientId, clientId));
  }

  async getPaymentsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Payment[]> {
    try {
      const payments = await this.db
        .select()
        .from(this.paymentsTable)
        .where(and(
          eq(this.paymentsTable.clientId, clientId),
          gte(this.paymentsTable.createdAt, startDate),
          lte(this.paymentsTable.createdAt, endDate)
        ))
        .orderBy(desc(this.paymentsTable.createdAt));

      return payments;
    } catch (error) {
      console.error('Error getting payments by date range:', error);
      return [];
    }
  }

  // Orders
  async createOrder(order: InsertOrder): Promise<Order> {
    const [result] = await this.db.insert(this.ordersTable).values(order).returning();
    return result;
  }

  async getOrderById(id: number): Promise<Order | undefined> {
    const [result] = await this.db.select().from(this.ordersTable).where(eq(this.ordersTable.id, id));
    return result;
  }

  async getOrdersByClientId(clientId: number): Promise<Order[]> {
    try {
      const result = await this.db.select().from(this.ordersTable)
        .where(eq(this.ordersTable.clientId, clientId))
        .orderBy(desc(this.ordersTable.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByClientId:', error);
      return [];
    }
  }

  async updateOrder(id: number, order: Partial<InsertOrder>): Promise<Order | undefined> {
    const [result] = await this.db.update(this.ordersTable).set(order).where(eq(this.ordersTable.id, id)).returning();
    return result;
  }

  async deleteOrder(id: number): Promise<boolean> {
    const result = await this.db.delete(this.ordersTable).where(eq(this.ordersTable.id, id));
    return result.rowCount > 0;
  }

  async getOrdersByClientIdWithVendor(clientId: number): Promise<any[]> {
    try {
      const result = await this.db
        .select({
          id: this.ordersTable.id,
          clientId: this.ordersTable.clientId,
          vendorId: this.ordersTable.vendorId,
          customerId: this.ordersTable.customerId,
          orderNumber: this.ordersTable.orderNumber,
          customerName: this.ordersTable.customerName,
          customerPhone: this.ordersTable.customerPhone,
          customerEmail: this.ordersTable.customerEmail,
          totalUsd: this.ordersTable.totalUsd,
          status: this.ordersTable.status,
          paymentStatus: this.ordersTable.paymentStatus,
          observations: this.ordersTable.observations,
          notes: this.ordersTable.notes,
          shippingType: this.ordersTable.shippingType,
          shippingAddress: this.ordersTable.shippingAddress,
          createdAt: this.ordersTable.createdAt,
          updatedAt: this.ordersTable.updatedAt,
          vendorName: this.vendorsTable.name
        })
        .from(this.ordersTable)
        .leftJoin(this.vendorsTable, eq(this.ordersTable.vendorId, this.vendorsTable.id))
        .where(eq(this.ordersTable.clientId, clientId))
        .orderBy(desc(this.ordersTable.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByClientIdWithVendor:', error);
      return [];
    }
  }

  async getOrdersWithItemsAndProducts(clientId: number, vendorId?: number | null): Promise<any[]> {
    try {
      // Build base query conditions
      const conditions = [eq(this.ordersTable.clientId, clientId)];
      if (vendorId) {
        conditions.push(eq(this.ordersTable.vendorId, vendorId));
      }

      // Get orders with complete vendor information including commission
      const ordersResult = await this.db
        .select({
          id: this.ordersTable.id,
          clientId: this.ordersTable.clientId,
          vendorId: this.ordersTable.vendorId,
          customerId: this.ordersTable.customerId,
          orderNumber: this.ordersTable.orderNumber,
          customerName: this.ordersTable.customerName,
          customerPhone: this.ordersTable.customerPhone,
          customerEmail: this.ordersTable.customerEmail,
          totalUsd: this.ordersTable.totalUsd,
          status: this.ordersTable.status,
          paymentStatus: this.ordersTable.paymentStatus,
          observations: this.ordersTable.observations,
          notes: this.ordersTable.notes,
          shippingType: this.ordersTable.shippingType,
          shippingAddress: this.ordersTable.shippingAddress,
          createdAt: this.ordersTable.createdAt,
          updatedAt: this.ordersTable.updatedAt,
          vendorName: this.vendorsTable.name,
          vendorCommission: this.vendorsTable.commissionPercentage,
          vendorEmail: this.vendorsTable.email,
          vendorPhone: this.vendorsTable.phone,
          vendorIsActive: this.vendorsTable.isActive
        })
        .from(this.ordersTable)
        .leftJoin(this.vendorsTable, eq(this.ordersTable.vendorId, this.vendorsTable.id))
        .where(and(...conditions))
        .orderBy(desc(this.ordersTable.createdAt));

      // For each order, get its items with products
      const result = [];
      for (const order of ordersResult) {
        const orderItemsResult = await this.db
          .select({
            id: this.orderItemsTable.id,
            orderId: this.orderItemsTable.orderId,
            productId: this.orderItemsTable.productId,
            quantity: this.orderItemsTable.quantity,
            priceUsd: this.orderItemsTable.priceUsd,
            paymentMethod: this.orderItemsTable.paymentMethod,
            exchangeRate: this.orderItemsTable.exchangeRate,
            amountUsd: this.orderItemsTable.amountUsd,
            createdAt: this.orderItemsTable.createdAt,
            // Product fields
            productImei: this.productsTable.imei,
            productModel: this.productsTable.model,
            productStorage: this.productsTable.storage,
            productColor: this.productsTable.color,
            productCostPrice: this.productsTable.costPrice,
            productStatus: this.productsTable.status
          })
          .from(this.orderItemsTable)
          .leftJoin(this.productsTable, eq(this.orderItemsTable.productId, this.productsTable.id))
          .where(eq(this.orderItemsTable.orderId, order.id));

        const itemsWithProducts = orderItemsResult.map(item => ({
          id: item.id,
          orderId: item.orderId,
          productId: item.productId,
          quantity: item.quantity,
          priceUsd: item.priceUsd,
          paymentMethod: item.paymentMethod,
          exchangeRate: item.exchangeRate,
          amountUsd: item.amountUsd,
          createdAt: item.createdAt,
          product: item.productImei ? {
            id: item.productId,
            imei: item.productImei,
            model: item.productModel,
            storage: item.productStorage,
            color: item.productColor,
            costPrice: item.productCostPrice,
            status: item.productStatus
          } : null
        }));

        // Create complete vendor object if vendor exists
        const vendor = order.vendorId ? {
          id: order.vendorId,
          name: order.vendorName,
          commissionPercentage: order.vendorCommission,
          email: order.vendorEmail,
          phone: order.vendorPhone,
          isActive: order.vendorIsActive
        } : null;

        result.push({
          ...order,
          vendor: vendor,
          orderItems: itemsWithProducts
        });
      }

      return result;
    } catch (error) {
      console.error('Error in getOrdersWithItemsAndProducts:', error);
      return [];
    }
  }

  async getOrdersByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Order[]> {
    try {
      const result = await this.db
        .select()
        .from(this.ordersTable)
        .where(and(
          eq(this.ordersTable.clientId, clientId),
          gte(this.ordersTable.createdAt, startDate),
          lt(this.ordersTable.createdAt, endDate)
        ))
        .orderBy(desc(this.ordersTable.createdAt));
      return result;
    } catch (error) {
      console.error('Error in getOrdersByDateRange:', error);
      return [];
    }
  }

  // Order Items
  async createOrderItem(orderItem: InsertOrderItem): Promise<OrderItem> {
    const [result] = await this.db.insert(this.orderItemsTable).values(orderItem).returning();
    return result;
  }

  async getOrderItemsByOrderId(orderId: number): Promise<OrderItem[]> {
    return await this.db.select().from(this.orderItemsTable).where(eq(this.orderItemsTable.orderId, orderId));
  }

  async getOrderItemsWithProductsByOrderId(orderId: number): Promise<any[]> {
    console.log('DrizzleStorage: Getting order items with products for order ID:', orderId);

    // Get order items first, then get products
    const orderItemsResult = await this.db.select().from(this.orderItemsTable).where(eq(this.orderItemsTable.orderId, orderId));
    console.log('DrizzleStorage: Found order items:', orderItemsResult);

    const result = [];
    for (const item of orderItemsResult) {
      const productResult = await this.db.select().from(this.productsTable).where(eq(this.productsTable.id, item.productId));
      const product = productResult[0] || null;
      result.push({
        ...item,
        product
      });
    }

    console.log('DrizzleStorage: Final result with products:', result);
    return result;
  }

  async deleteOrderItem(id: number): Promise<boolean> {
    const result = await this.db.delete(this.orderItemsTable).where(eq(this.orderItemsTable.id, id));
    return result.rowCount > 0;
  }

  // Stub implementations for other methods to keep interface compatibility
  async createProductHistory(history: InsertProductHistory): Promise<ProductHistory> {
    const [result] = await this.db.insert(this.productHistoryTable).values(history).returning();
    return result;
  }

  async getProductHistoryByProductId(productId: number): Promise<ProductHistory[]> {
    return await this.db.select().from(this.productHistoryTable).where(eq(this.productHistoryTable.productId, productId));
  }

  async getProductHistoryByClientId(clientId: number): Promise<ProductHistory[]> {
    return await this.db.select().from(this.productHistoryTable).where(eq(this.productHistoryTable.clientId, clientId));
  }

  async getProductsWithAlerts(clientId: number): Promise<any[]> {
    return [];
  }

  async createCurrencyExchange(exchange: InsertCurrencyExchange): Promise<CurrencyExchange> {
    const [result] = await this.db.insert(this.currencyExchangesTable).values(exchange).returning();
    return result;
  }

  async getCurrencyExchangesByClientId(clientId: number): Promise<CurrencyExchange[]> {
    return await this.db.select().from(this.currencyExchangesTable).where(eq(this.currencyExchangesTable.clientId, clientId));
  }

  async getCurrencyExchangesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CurrencyExchange[]> {
    return await this.db.select().from(this.currencyExchangesTable).where(
      and(
        eq(this.currencyExchangesTable.clientId, clientId),
        gte(this.currencyExchangesTable.createdAt, startDate),
        lt(this.currencyExchangesTable.createdAt, endDate)
      )
    );
  }

  async createCashRegister(cashRegisterData: InsertCashRegister): Promise<CashRegister> {
    const [result] = await this.db.insert(this.cashRegisterTable).values(cashRegisterData).returning();
    return result;
  }

  async getCurrentCashRegister(clientId: number): Promise<CashRegister | undefined> {
    const [result] = await this.db.select().from(this.cashRegisterTable).where(
      and(eq(this.cashRegisterTable.clientId, clientId), eq(this.cashRegisterTable.isOpen, true))
    );
    return result;
  }

  async getCashRegisterByDate(clientId: number, dateStr: string): Promise<CashRegister | undefined> {
    const targetDate = new Date(dateStr);
    const startOfDay = new Date(targetDate);
    startOfDay.setHours(0, 0, 0, 0);

    const endOfDay = new Date(targetDate);
    endOfDay.setHours(23, 59, 59, 999);

    const [result] = await this.db.select().from(this.cashRegisterTable).where(
      and(
        eq(this.cashRegisterTable.clientId, clientId),
        gte(this.cashRegisterTable.date, startOfDay),
        lte(this.cashRegisterTable.date, endOfDay)
      )
    );
    return result;
  }

  async updateCashRegister(id: number, cashRegisterData: Partial<InsertCashRegister>): Promise<CashRegister | undefined> {
    const [result] = await this.db.update(this.cashRegisterTable).set(cashRegisterData).where(eq(this.cashRegisterTable.id, id)).returning();
    return result;
  }

  async getDailySales(clientId: number, startDate: Date, endDate: Date): Promise<number> {
    const result = await this.db.select().from(this.ordersTable).where(
      and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lt(this.ordersTable.createdAt, endDate)
      )
    );

    return result.reduce((sum, order) => sum + parseFloat(order.totalUsd), 0);
  }

  async createConfiguration(config: InsertConfiguration): Promise<Configuration> {
    const [result] = await this.db.insert(this.configurationTable).values(config).returning();
    return result;
  }

  async getConfigurationByKey(clientId: number, key: string): Promise<Configuration | undefined> {
    const [result] = await this.db.select().from(this.configurationTable).where(
      and(eq(this.configurationTable.clientId, clientId), eq(this.configurationTable.key, key))
    );
    return result;
  }

  async getConfigurationsByClientId(clientId: number): Promise<Configuration[]> {
    return await this.db.select().from(this.configurationTable).where(eq(this.configurationTable.clientId, clientId));
  }

  async updateConfiguration(clientId: number, key: string, value: string): Promise<Configuration> {
    const existing = await this.getConfigurationByKey(clientId, key);
    if (existing) {
      const [result] = await this.db.update(this.configurationTable)
        .set({ value })
        .where(and(eq(this.configurationTable.clientId, clientId), eq(this.configurationTable.key, key)))
        .returning();
      return result;
    } else {
      return await this.createConfiguration({ clientId, key, value });
    }
  }

  async createCompanyConfiguration(config: InsertCompanyConfiguration): Promise<CompanyConfiguration> {
    const [result] = await this.db.insert(this.companyConfigurationTable).values(config).returning();
    return result;
  }

  async getCompanyConfigurationByClientId(clientId: number): Promise<CompanyConfiguration | undefined> {
    const [result] = await this.db.select().from(this.companyConfigurationTable).where(eq(this.companyConfigurationTable.clientId, clientId));
    return result;
  }

  async updateCompanyConfiguration(id: number, config: Partial<InsertCompanyConfiguration>): Promise<CompanyConfiguration | undefined> {
    const [result] = await this.db.update(this.companyConfigurationTable).set(config).where(eq(this.companyConfigurationTable.id, id)).returning();
    return result;
  }

  async createVendor(vendor: InsertVendor): Promise<Vendor> {
    const [result] = await this.db.insert(this.vendorsTable).values(vendor).returning();
    return result;
  }

  async getVendorById(id: number): Promise<Vendor | undefined> {
    const [result] = await this.db.select().from(this.vendorsTable).where(eq(this.vendorsTable.id, id));
    return result;
  }

  async getVendorsByClientId(clientId: number): Promise<Vendor[]> {
    return await this.db.select().from(this.vendorsTable).where(eq(this.vendorsTable.clientId, clientId));
  }

  async updateVendor(id: number, vendor: Partial<InsertVendor>): Promise<Vendor | undefined> {
    const [result] = await this.db.update(this.vendorsTable).set(vendor).where(eq(this.vendorsTable.id, id)).returning();
    return result;
  }

  async deleteVendor(id: number): Promise<boolean> {
    const result = await this.db.delete(this.vendorsTable).where(eq(this.vendorsTable.id, id));
    return result.rowCount > 0;
  }

  async createCustomer(customer: InsertCustomer): Promise<Customer> {
    const [result] = await this.db.insert(this.customersTable).values(customer).returning();
    return result;
  }

  async getCustomerById(id: number): Promise<Customer | undefined> {
    const [result] = await this.db.select().from(this.customersTable).where(eq(this.customersTable.id, id));
    return result;
  }

  async getCustomersByClientId(clientId: number): Promise<Customer[]> {
    return await this.db.select().from(this.customersTable).where(eq(this.customersTable.clientId, clientId));
  }

  async updateCustomer(id: number, customer: Partial<InsertCustomer>): Promise<Customer | undefined> {
    const [result] = await this.db.update(this.customersTable).set(customer).where(eq(this.customersTable.id, id)).returning();
    return result;
  }

  async deleteCustomer(id: number): Promise<boolean> {
    const result = await this.db.delete(this.customersTable).where(eq(this.customersTable.id, id));
    return result.rowCount > 0;
  }

  // Stock Control Sessions
  async createStockControlSession(sessionData: InsertStockControlSession): Promise<StockControlSession> {
    console.log('📊 Creating stock control session with data:', JSON.stringify(sessionData, null, 2));

    try {
      const [session] = await this.db.insert(this.stockControlSessionsTable).values({
        clientId: sessionData.clientId,
        userId: sessionData.userId,
        date: new Date(sessionData.date),
        startTime: sessionData.startTime || new Date(),
        totalProducts: sessionData.totalProducts || 0,
        scannedProducts: sessionData.scannedProducts || 0,
        missingProducts: sessionData.missingProducts || 0,
        status: sessionData.status || 'active',
        notes: sessionData.notes || null
      }).returning();

      console.log('✅ Stock control session created successfully:', JSON.stringify(session, null, 2));
      return session;
    } catch (error) {
      console.error('❌ Error in createStockControlSession:', error);
      throw error;
    }
  }

  async getStockControlSessionById(id: number): Promise<StockControlSession | undefined> {
    const [session] = await this.db.select().from(this.stockControlSessionsTable).where(eq(this.stockControlSessionsTable.id, id));
    return session;
  }

  async getActiveStockControlSession(clientId: number): Promise<StockControlSession | undefined> {
    const [session] = await this.db.select()
      .from(this.stockControlSessionsTable)
      .where(and(
        eq(this.stockControlSessionsTable.clientId, clientId),
        eq(this.stockControlSessionsTable.status, 'active')
      ));
    return session;
  }

  async getStockControlSessionsByClientId(clientId: number): Promise<StockControlSession[]> {
    const sessions = await this.db.select()
      .from(this.stockControlSessionsTable)
      .where(eq(this.stockControlSessionsTable.clientId, clientId))
      .orderBy(desc(this.stockControlSessionsTable.date));
    return sessions;
  }

  async updateStockControlSession(id: number, session: Partial<InsertStockControlSession>): Promise<StockControlSession | undefined> {
    const [updated] = await this.db.update(this.stockControlSessionsTable)
      .set(session)
      .where(eq(this.stockControlSessionsTable.id, id))
      .returning();
    return updated;
  }

  // Stock Control Items
  async createStockControlItem(item: InsertStockControlItem): Promise<StockControlItem> {
    const [created] = await this.db.insert(this.stockControlItemsTable).values(item).returning();
    return created;
  }

  async getStockControlItemsBySessionId(sessionId: number): Promise<StockControlItem[]> {
    const items = await this.db.select()
      .from(this.stockControlItemsTable)
      .where(eq(this.stockControlItemsTable.sessionId, sessionId));
    return items;
  }

  async getStockControlItemsWithProductsBySessionId(sessionId: number): Promise<any[]> {
    const items = await this.db.select({
      id: this.stockControlItemsTable.id,
      imei: this.stockControlItemsTable.imei,
      scannedAt: this.stockControlItemsTable.scannedAt,
      status: this.stockControlItemsTable.status,
      model: this.productsTable.model,
      storage: this.productsTable.storage,
      color: this.productsTable.color,
    })
    .from(this.stockControlItemsTable)
    .leftJoin(this.productsTable, eq(this.stockControlItemsTable.productId, this.productsTable.id))
    .where(eq(this.stockControlItemsTable.sessionId, sessionId));
    return items;
  }

  async updateStockControlItem(id: number, item: Partial<InsertStockControlItem>): Promise<StockControlItem | undefined> {
    const [updated] = await this.db.update(this.stockControlItemsTable)
      .set(item)
      .where(eq(this.stockControlItemsTable.id, id))
      .returning();
    return updated;
  }

  // Stock Control Helper Methods
  async getProductsForStockControl(clientId: number): Promise<Product[]> {
    const productsForControl = await this.db.select()
      .from(this.productsTable)
      .where(and(
        eq(this.productsTable.clientId, clientId),
        eq(this.productsTable.status, 'disponible')
      ));

    const reservedProducts = await this.db.select()
      .from(this.productsTable)
      .where(and(
        eq(this.productsTable.clientId, clientId),
        eq(this.productsTable.status, 'reservado')
      ));

    return [...productsForControl, ...reservedProducts];
  }

  async getMissingProductsFromSession(sessionId: number): Promise<Product[]> {
    // Get session info
    const session = await this.getStockControlSessionById(sessionId);
    if (!session) return [];

    // Get all products that should be controlled
    const allProducts = await this.getProductsForStockControl(session.clientId);

    // Get scanned product IDs
    const scannedItems = await this.getStockControlItemsBySessionId(sessionId);
    const scannedProductIds = scannedItems.map(item => item.productId);

    // Return products not scanned
    return allProducts.filter(product => !scannedProductIds.includes(product.id));
  }

  async getExtraviosProducts(clientId: number): Promise<Product[]> {
    const extraviosProducts = await this.db.select()
      .from(this.productsTable)
      .where(and(
        eq(this.productsTable.clientId, clientId),
        eq(this.productsTable.status, 'extravio')
      ));
    return extraviosProducts;
  }

  // =======================
  // CASH MOVEMENTS - Enhanced System
  // =======================

  async createCashMovement(movement: InsertCashMovement): Promise<CashMovement> {
    const [result] = await this.db.insert(this.cashMovementsTable).values(movement).returning();
    return result;
  }

  async getCashMovementsByClientId(clientId: number): Promise<CashMovement[]> {
    return await this.db
      .select({
        id: this.cashMovementsTable.id,
        clientId: this.cashMovementsTable.clientId,
        cashRegisterId: this.cashMovementsTable.cashRegisterId,
        type: this.cashMovementsTable.type,
        subtype: this.cashMovementsTable.subtype,
        amount: this.cashMovementsTable.amount,
        currency: this.cashMovementsTable.currency,
        exchangeRate: this.cashMovementsTable.exchangeRate,
        amountUsd: this.cashMovementsTable.amountUsd,
        description: this.cashMovementsTable.description,
        referenceId: this.cashMovementsTable.referenceId,
        referenceType: this.cashMovementsTable.referenceType,
        customerId: this.cashMovementsTable.customerId,
        vendorId: this.cashMovementsTable.vendorId,
        userId: this.cashMovementsTable.userId,
        notes: this.cashMovementsTable.notes,
        createdAt: this.cashMovementsTable.createdAt,
        userName: this.usersTable.username,
        customerName: this.customersTable.name
      })
      .from(this.cashMovementsTable)
      .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
      .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
      .where(eq(this.cashMovementsTable.clientId, clientId))
      .orderBy(desc(this.cashMovementsTable.createdAt));
  }

  async getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]> {
    return await this.db
      .select({
        id: this.cashMovementsTable.id,
        clientId: this.cashMovementsTable.clientId,
        cashRegisterId: this.cashMovementsTable.cashRegisterId,
        type: this.cashMovementsTable.type,
        subtype: this.cashMovementsTable.subtype,
        amount: this.cashMovementsTable.amount,
        currency: this.cashMovementsTable.currency,
        exchangeRate: this.cashMovementsTable.exchangeRate,
        amountUsd: this.cashMovementsTable.amountUsd,
        description: this.cashMovementsTable.description,
        referenceId: this.cashMovementsTable.referenceId,
        referenceType: this.cashMovementsTable.referenceType,
        customerId: this.cashMovementsTable.customerId,
        vendorId: this.cashMovementsTable.vendorId,
        userId: this.cashMovementsTable.userId,
        notes: this.cashMovementsTable.notes,
        createdAt: this.cashMovementsTable.createdAt,
        userName: this.usersTable.username,
        customerName: this.customersTable.name
      })
      .from(this.cashMovementsTable)
      .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
      .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
      .where(and(
        eq(this.cashMovementsTable.clientId, clientId),
        eq(this.cashMovementsTable.type, type)
      ))
      .orderBy(desc(this.cashMovementsTable.createdAt));
  }

  async getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]> {
    return await this.db
      .select({
        id: this.cashMovementsTable.id,
        clientId: this.cashMovementsTable.clientId,
        cashRegisterId: this.cashMovementsTable.cashRegisterId,
        type: this.cashMovementsTable.type,
        subtype: this.cashMovementsTable.subtype,
        amount: this.cashMovementsTable.amount,
        currency: this.cashMovementsTable.currency,
        exchangeRate: this.cashMovementsTable.exchangeRate,
        amountUsd: this.cashMovementsTable.amountUsd,
        description: this.cashMovementsTable.description,
        referenceId: this.cashMovementsTable.referenceId,
        referenceType: this.cashMovementsTable.referenceType,
        customerId: this.cashMovementsTable.customerId,
        vendorId: this.cashMovementsTable.vendorId,
        userId: this.cashMovementsTable.userId,
        notes: this.cashMovementsTable.notes,
        createdAt: this.cashMovementsTable.createdAt,
        userName: this.usersTable.username,
        customerName: this.customersTable.name
      })
      .from(this.cashMovementsTable)
      .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
      .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
      .where(and(
        eq(this.cashMovementsTable.clientId, clientId),
        gte(this.cashMovementsTable.createdAt, startDate),
        lt(this.cashMovementsTable.createdAt, endDate)
      ))
      .orderBy(desc(this.cashMovementsTable.createdAt));
  }

  async getCashMovementsWithFilters(clientId: number, filters: {
    type?: string;
    dateFrom?: Date;
    dateTo?: Date;
    customer?: string;
    vendor?: string;
    search?: string;
    paymentMethod?: string;
  }): Promise<CashMovement[]> {
    let query = this.db
      .select({
        id: this.cashMovementsTable.id,
        clientId: this.cashMovementsTable.clientId,
        cashRegisterId: this.cashMovementsTable.cashRegisterId,
        type: this.cashMovementsTable.type,
        subtype: this.cashMovementsTable.subtype,
        amount: this.cashMovementsTable.amount,
        currency: this.cashMovementsTable.currency,
        exchangeRate: this.cashMovementsTable.exchangeRate,
        amountUsd: this.cashMovementsTable.amountUsd,
        description: this.cashMovementsTable.description,
        referenceId: this.cashMovementsTable.referenceId,
        referenceType: this.cashMovementsTable.referenceType,
        customerId: this.cashMovementsTable.customerId,
        vendorId: this.cashMovementsTable.vendorId,
        userId: this.cashMovementsTable.userId,
        notes: this.cashMovementsTable.notes,
        createdAt: this.cashMovementsTable.createdAt,
        userName: this.usersTable.username,
        customerName: this.customersTable.name,
        vendorName: this.vendorsTable.name
      })
      .from(this.cashMovementsTable)
      .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
      .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
      .leftJoin(this.vendorsTable, eq(this.cashMovementsTable.vendorId, this.vendorsTable.id));

    const conditions = [eq(this.cashMovementsTable.clientId, clientId)];

    // Apply filters
    if (filters.type) {
      conditions.push(eq(this.cashMovementsTable.type, filters.type));
    }

    if (filters.dateFrom && filters.dateTo) {
      conditions.push(gte(this.cashMovementsTable.createdAt, filters.dateFrom));
      conditions.push(lt(this.cashMovementsTable.createdAt, filters.dateTo));
    }

    if (filters.customer) {
      conditions.push(ilike(this.customersTable.name, `%${filters.customer}%`));
    }

    if (filters.vendor) {
      conditions.push(
        or(
          ilike(this.vendorsTable.name, `%${filters.vendor}%`),
          ilike(this.usersTable.username, `%${filters.vendor}%`)
        )
      );
    }

    if (filters.search) {
      conditions.push(
        or(
          ilike(this.cashMovementsTable.description, `%${filters.search}%`),
          ilike(this.cashMovementsTable.notes, `%${filters.search}%`),
          ilike(this.customersTable.name, `%${filters.search}%`),
          ilike(this.usersTable.username, `%${filters.search}%`),
          ilike(this.vendorsTable.name, `%${filters.search}%`)
        )
      );
    }

    if (filters.paymentMethod) {
      conditions.push(eq(this.cashMovementsTable.subtype, filters.paymentMethod));
    }

    return await query
      .where(and(...conditions))
      .orderBy(desc(this.cashMovementsTable.createdAt));
  }

  async getAllCashMovementsForExport(clientId: number): Promise<CashMovement[]> {
    return await this.db
      .select({
        id: this.cashMovementsTable.id,
        clientId: this.cashMovementsTable.clientId,
        cashRegisterId: this.cashMovementsTable.cashRegisterId,
        type: this.cashMovementsTable.type,
        subtype: this.cashMovementsTable.subtype,
        amount: this.cashMovementsTable.amount,
        currency: this.cashMovementsTable.currency,
        exchangeRate: this.cashMovementsTable.exchangeRate,
        amountUsd: this.cashMovementsTable.amountUsd,
        description: this.cashMovementsTable.description,
        referenceId: this.cashMovementsTable.referenceId,
        referenceType: this.cashMovementsTable.referenceType,
        customerId: this.cashMovementsTable.customerId,
        vendorId: this.cashMovementsTable.vendorId,
        userId: this.cashMovementsTable.userId,
        notes: this.cashMovementsTable.notes,
        createdAt: this.cashMovementsTable.createdAt,
        userName: this.usersTable.username,
        customerName: this.customersTable.name,
        vendorName: this.vendorsTable.name
      })
      .from(this.cashMovementsTable)
      .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
      .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
      .leftJoin(this.vendorsTable, eq(this.cashMovementsTable.vendorId, this.vendorsTable.id))
      .where(eq(this.cashMovementsTable.clientId, clientId))
      .orderBy(desc(this.cashMovementsTable.createdAt));
  }

  // =======================
  // EXPENSES
  // =======================

  async createExpense(expense: InsertExpense): Promise<Expense> {
    const [result] = await this.db.insert(this.expensesTable).values(expense).returning();
    return result;
  }

  async getExpensesByClientId(clientId: number): Promise<Expense[]> {
    return await this.db
      .select({
        id: this.expensesTable.id,
        clientId: this.expensesTable.clientId,
        cashRegisterId: this.expensesTable.cashRegisterId,
        category: this.expensesTable.category,
        description: this.expensesTable.description,
        amount: this.expensesTable.amount,
        currency: this.expensesTable.currency,
        exchangeRate: this.expensesTable.exchangeRate,
        amountUsd: this.expensesTable.amountUsd,
        paymentMethod: this.expensesTable.paymentMethod,
        providerId: this.expensesTable.providerId,
        userId: this.expensesTable.userId,
        receiptNumber: this.expensesTable.receiptNumber,
        notes: this.expensesTable.notes,
        expenseDate: this.expensesTable.expenseDate,
        createdAt: this.expensesTable.createdAt,
        userName: this.usersTable.username
      })
      .from(this.expensesTable)
      .leftJoin(this.usersTable, eq(this.expensesTable.userId, this.usersTable.id))
      .where(eq(this.expensesTable.clientId, clientId))
      .orderBy(desc(this.expensesTable.createdAt));
  }

  async getExpensesByCategory(clientId: number, category: string): Promise<Expense[]> {
    return await this.db
      .select({
        id: this.expensesTable.id,
        clientId: this.expensesTable.clientId,
        cashRegisterId: this.expensesTable.cashRegisterId,
        category: this.expensesTable.category,
        description: this.expensesTable.description,
        amount: this.expensesTable.amount,
        currency: this.expensesTable.currency,
        exchangeRate: this.expensesTable.exchangeRate,
        amountUsd: this.expensesTable.amountUsd,
        paymentMethod: this.expensesTable.paymentMethod,
        providerId: this.expensesTable.providerId,
        userId: this.expensesTable.userId,
        receiptNumber: this.expensesTable.receiptNumber,
        notes: this.expensesTable.notes,
        expenseDate: this.expensesTable.expenseDate,
        createdAt: this.expensesTable.createdAt,
        userName: this.usersTable.username
      })
      .from(this.expensesTable)
      .leftJoin(this.usersTable, eq(this.expensesTable.userId, this.usersTable.id))
      .where(and(
        eq(this.expensesTable.clientId, clientId),
        eq(this.expensesTable.category, category)
      ))
      .orderBy(desc(this.expensesTable.createdAt));
  }

  async getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]> {
    return await this.db
      .select({
        id: this.expensesTable.id,
        clientId: this.expensesTable.clientId,
        cashRegisterId: this.expensesTable.cashRegisterId,
        category: this.expensesTable.category,
        description: this.expensesTable.description,
        amount: this.expensesTable.amount,
        currency: this.expensesTable.currency,
        exchangeRate: this.expensesTable.exchangeRate,
        amountUsd: this.expensesTable.amountUsd,
        paymentMethod: this.expensesTable.paymentMethod,
        providerId: this.expensesTable.providerId,
        userId: this.expensesTable.userId,
        receiptNumber: this.expensesTable.receiptNumber,
        notes: this.expensesTable.notes,
        expenseDate: this.expensesTable.expenseDate,
        createdAt: this.expensesTable.createdAt,
        userName: this.usersTable.username
      })
      .from(this.expensesTable)
      .leftJoin(this.usersTable, eq(this.expensesTable.userId, this.usersTable.id))
      .where(and(
        eq(this.expensesTable.clientId, clientId),
        gte(this.expensesTable.expenseDate, startDate),
        lt(this.expensesTable.expenseDate, endDate)
      ))
      .orderBy(desc(this.expensesTable.createdAt));
  }

  async updateExpense(id: number, updates: Partial<InsertExpense>): Promise<Expense | undefined> {
    const [result] = await this.db
      .update(this.expensesTable)
      .set(updates)
      .where(eq(this.expensesTable.id, id))
      .returning();
    return result;
  }

  async deleteExpense(id: number): Promise<boolean> {
    const result = await this.db.delete(this.expensesTable).where(eq(this.expensesTable.id, id));
    return result.rowCount > 0;
  }

  // =======================
  // CUSTOMER DEBTS
  // =======================

  async createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt> {
    const [result] = await this.db.insert(this.customerDebtsTable).values(debt).returning();
    return result;
  }

  async getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]> {
    return await this.db
      .select({
        id: this.customerDebtsTable.id,
        clientId: this.customerDebtsTable.clientId,
        customerId: this.customerDebtsTable.customerId,
        orderId: this.customerDebtsTable.orderId,
        debtAmount: this.customerDebtsTable.debtAmount,
        paidAmount: this.customerDebtsTable.paidAmount,
        remainingAmount: this.customerDebtsTable.remainingAmount,
        currency: this.customerDebtsTable.currency,
        status: this.customerDebtsTable.status,
        dueDate: this.customerDebtsTable.dueDate,
        paymentHistory: this.customerDebtsTable.paymentHistory,
        notes: this.customerDebtsTable.notes,
        createdAt: this.customerDebtsTable.createdAt,
        updatedAt: this.customerDebtsTable.updatedAt,
        customerName: this.customersTable.name
      })
      .from(this.customerDebtsTable)
      .leftJoin(this.customersTable, eq(this.customerDebtsTable.customerId, this.customersTable.id))
      .where(eq(this.customerDebtsTable.clientId, clientId))
      .orderBy(desc(this.customerDebtsTable.createdAt));
  }

  async getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]> {
    return await this.db
      .select({
        id: this.customerDebtsTable.id,
        clientId: this.customerDebtsTable.clientId,
        customerId: this.customerDebtsTable.customerId,
        orderId: this.customerDebtsTable.orderId,
        debtAmount: this.customerDebtsTable.debtAmount,
        paidAmount: this.customerDebtsTable.paidAmount,
        remainingAmount: this.customerDebtsTable.remainingAmount,
        currency: this.customerDebtsTable.currency,
        status: this.customerDebtsTable.status,
        dueDate: this.customerDebtsTable.dueDate,
        paymentHistory: this.customerDebtsTable.paymentHistory,
        notes: this.customerDebtsTable.notes,
        createdAt: this.customerDebtsTable.createdAt,
        updatedAt: this.customerDebtsTable.updatedAt,
        customerName: this.customersTable.name
      })
      .from(this.customerDebtsTable)
      .leftJoin(this.customersTable, eq(this.customerDebtsTable.customerId, this.customersTable.id))
      .where(eq(this.customerDebtsTable.customerId, customerId))
      .orderBy(desc(this.customerDebtsTable.createdAt));
  }

  async getActiveDebts(clientId: number): Promise<CustomerDebt[]> {
    return await this.db
      .select({
        id: this.customerDebtsTable.id,
        clientId: this.customerDebtsTable.clientId,
        customerId: this.customerDebtsTable.customerId,
        orderId: this.customerDebtsTable.orderId,
        debtAmount: this.customerDebtsTable.debtAmount,
        paidAmount: this.customerDebtsTable.paidAmount,
        remainingAmount: this.customerDebtsTable.remainingAmount,
        currency: this.customerDebtsTable.currency,
        status: this.customerDebtsTable.status,
        dueDate: this.customerDebtsTable.dueDate,
        paymentHistory: this.customerDebtsTable.paymentHistory,
        notes: this.customerDebtsTable.notes,
        createdAt: this.customerDebtsTable.createdAt,
        updatedAt: this.customerDebtsTable.updatedAt,
        customerName: this.customersTable.name
      })
      .from(this.customerDebtsTable)
      .leftJoin(this.customersTable, eq(this.customerDebtsTable.customerId, this.customersTable.id))
      .where(and(
        eq(this.customerDebtsTable.clientId, clientId),
        eq(this.customerDebtsTable.status, 'vigente')
      ))
      .orderBy(desc(this.customerDebtsTable.createdAt));
  }

  async getActiveDebtByOrderId(orderId: number): Promise<CustomerDebt | undefined> {
    const [debt] = await this.db.select()
      .from(this.customerDebtsTable)
      .where(and(
        eq(this.customerDebtsTable.orderId, orderId),
        eq(this.customerDebtsTable.status, 'vigente')
      ))
      .limit(1);
    return debt;
  }

  async updateCustomerDebt(id: number, updates: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined> {
    const [result] = await this.db
      .update(this.customerDebtsTable)
      .set(updates)
      .where(eq(this.customerDebtsTable.id, id))
      .returning();
    return result;
  }

  // =======================
  // DEBT PAYMENTS
  // =======================

  async createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment> {
    const [result] = await this.db.insert(this.debtPaymentsTable).values(payment).returning();
    return result;
  }

  async getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]> {
    return await this.db
      .select({
        id: this.debtPaymentsTable.id,
        clientId: this.debtPaymentsTable.clientId,
        debtId: this.debtPaymentsTable.debtId,
        cashRegisterId: this.debtPaymentsTable.cashRegisterId,
        amount: this.debtPaymentsTable.amount,
        currency: this.debtPaymentsTable.currency,
        exchangeRate: this.debtPaymentsTable.exchangeRate,
        amountUsd: this.debtPaymentsTable.amountUsd,
        paymentMethod: this.debtPaymentsTable.paymentMethod,
        userId: this.debtPaymentsTable.userId,
        notes: this.debtPaymentsTable.notes,
        paymentDate: this.debtPaymentsTable.paymentDate,
        createdAt: this.debtPaymentsTable.createdAt,
        userName: this.usersTable.username
      })
      .from(this.debtPaymentsTable)
      .leftJoin(this.usersTable, eq(this.debtPaymentsTable.userId, this.usersTable.id))
      .where(eq(this.debtPaymentsTable.debtId, debtId))
      .orderBy(desc(this.debtPaymentsTable.createdAt));
  }

  async getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]> {
    return await this.db
      .select({
        id: this.debtPaymentsTable.id,
        clientId: this.debtPaymentsTable.clientId,
        debtId: this.debtPaymentsTable.debtId,
        cashRegisterId: this.debtPaymentsTable.cashRegisterId,
        amount: this.debtPaymentsTable.amount,
        currency: this.debtPaymentsTable.currency,
        exchangeRate: this.debtPaymentsTable.exchangeRate,
        amountUsd: this.debtPaymentsTable.amountUsd,
        paymentMethod: this.debtPaymentsTable.paymentMethod,
        userId: this.debtPaymentsTable.userId,
        notes: this.debtPaymentsTable.notes,
        paymentDate: this.debtPaymentsTable.paymentDate,
        createdAt: this.debtPaymentsTable.createdAt,
        userName: this.usersTable.username
      })
      .from(this.debtPaymentsTable)
      .leftJoin(this.usersTable, eq(this.debtPaymentsTable.userId, this.usersTable.id))
      .where(eq(this.debtPaymentsTable.clientId, clientId))
      .orderBy(desc(this.debtPaymentsTable.createdAt));
  }

  // =======================
  // DAILY REPORTS
  // =======================

  async createDailyReport(report: InsertDailyReport): Promise<DailyReport> {
    const [result] = await this.db.insert(this.dailyReportsTable).values(report).returning();
    return result;
  }

  async getDailyReportsByClientId(clientId: number): Promise<DailyReport[]> {
    return await this.db
      .select()
      .from(this.dailyReportsTable)
      .where(eq(this.dailyReportsTable.clientId, clientId))
      .orderBy(desc(this.dailyReportsTable.reportDate));
  }

  async getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined> {
    const [result] = await this.db
      .select()
      .from(this.dailyReportsTable)
      .where(and(
        eq(this.dailyReportsTable.clientId, clientId),
        eq(this.dailyReportsTable.reportDate, date)
      ));
    return result;
  }

  async generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport> {
    // Get all movements for the day
    const startDate = new Date(date);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(date);
    endDate.setHours(23, 59, 59, 999);

    // Calculate totals using raw SQL for better performance
    const totalsQuery = await this.db.execute(sqlOperator`
      SELECT
        COALESCE(SUM(CASE WHEN cm.type IN ('ingreso', 'venta') THEN CAST(cm.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_income,
        COALESCE(SUM(CASE WHEN e.id IS NOT NULL THEN CAST(e.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_expenses,
        COALESCE(SUM(CASE WHEN dp.id IS NOT NULL THEN CAST(dp.amount_usd AS DECIMAL) ELSE 0 END), 0) as total_debt_payments,
        COUNT(cm.id) as total_movements
      FROM cash_movements cm
      LEFT JOIN expenses e ON e.client_id = ${clientId} AND e.expense_date >= ${startDate} AND e.expense_date < ${endDate}
      LEFT JOIN debt_payments dp ON dp.client_id = ${clientId} AND dp.payment_date >= ${startDate} AND dp.payment_date < ${endDate}
      WHERE cm.client_id = ${clientId}
        AND cm.created_at >= ${startDate}
        AND cm.created_at < ${endDate}
    `);

    const totals = totalsQuery.rows[0] as any;
    const totalIncome = parseFloat(totals.total_income || "0");
    const totalExpenses = parseFloat(totals.total_expenses || "0");
    const totalDebtPayments = parseFloat(totals.total_debt_payments || "0");
    const netProfit = totalIncome - totalExpenses;

    const reportData = {
      summary: {
        totalIncome,
        totalExpenses,
        totalDebtPayments,
        netProfit,
        movementsCount: parseInt(totals.total_movements || "0")
      },
      timestamp: new Date().toISOString()
    };

    const report = await this.createDailyReport({
      clientId,
      reportDate: date,
      openingBalance: "1000.00", // Should get from previous day's closing
      totalIncome: totalIncome.toFixed(2),
      totalExpenses: totalExpenses.toFixed(2),
      totalDebts: "0.00", // Calculate active debts
      totalDebtPayments: totalDebtPayments.toFixed(2),
      netProfit: netProfit.toFixed(2),
      vendorCommissions: "0.00", // Calculate vendor commissions
      exchangeRateUsed: "1200.00", // Get current rate
      closingBalance: (1000 + netProfit).toFixed(2),
      totalMovements: parseInt(totals.total_movements || "0"),
      reportData: JSON.stringify(reportData),
      isAutoGenerated: true
    });

    return report;
  }

  async getDailyReports(clientId: number, limit: number = 30): Promise<any[]> {
    return await this.db.select()
      .from(this.dailyReportsTable)
      .where(eq(this.dailyReportsTable.clientId, clientId))
      .orderBy(desc(this.dailyReportsTable.reportDate))
      .limit(limit);
  }

  // =======================
  // GENERATED REPORTS
  // =======================

  async createGeneratedReport(report: InsertGeneratedReport): Promise<GeneratedReport> {
    const [result] = await this.db.insert(this.generatedReportsTable).values(report).returning();
    return result;
  }

  async getGeneratedReportsByClientId(clientId: number): Promise<GeneratedReport[]> {
    return await this.db
      .select()
      .from(this.generatedReportsTable)
      .leftJoin(this.dailyReportsTable, eq(this.generatedReportsTable.dailyReportId, this.dailyReportsTable.id))
      .where(eq(this.generatedReportsTable.clientId, clientId))
      .orderBy(desc(this.generatedReportsTable.generatedAt));
  }

  async getGeneratedReportById(id: number): Promise<GeneratedReport | undefined> {
    const [result] = await this.db
      .select()
      .from(this.generatedReportsTable)
      .where(eq(this.generatedReportsTable.id, id));
    return result;
  }

  async generateAutoExcelReport(clientId: number, dailyReportId: number, reportDate: Date): Promise<void> {
    try {
      // Get comprehensive data for the report
      const [
        cashMovements,
        expenses,
        debtPayments,
        vendorPerformance,
        paymentMethods // This seems to be missing in the query, assuming it's for payment method counts
      ] = await Promise.all([
        this.getAllCashMovementsForExport(clientId),
        this.getExpensesByClientId(clientId),
        this.getDebtPaymentsByClientId(clientId),
        this.getVendorPerformanceRanking(clientId),
        this.getPaymentMethodsSummary(clientId) // Assuming this method exists or needs to be created
      ]);

      // Filter data for the specific date
      const dateStr = reportDate.toISOString().split('T')[0];
      const filteredMovements = cashMovements.filter((mov: any) =>
        mov.createdAt.toISOString().split('T')[0] === dateStr
      );
      const filteredExpenses = expenses.filter((exp: any) =>
        exp.expenseDate.toISOString().split('T')[0] === dateStr
      );
      const filteredDebtPayments = debtPayments.filter((debt: any) =>
        debt.paymentDate.toISOString().split('T')[0] === dateStr
      );

      // Calculate payment method breakdown
      const paymentMethodBreakdown: any = {};
      filteredMovements.forEach((movement: any) => {
        const method = movement.subtype || 'No especificado';
        if (!paymentMethodBreakdown[method]) {
          paymentMethodBreakdown[method] = { count: 0, totalUsd: 0 };
        }
        paymentMethodBreakdown[method].count++;
        paymentMethodBreakdown[method].totalUsd += parseFloat(movement.amountUsd || 0);
      });

      // Prepare comprehensive report content
      const reportContent = {
        date: dateStr,
        paymentMethodBreakdown,
        detailedPayments: filteredMovements,
        detailedExpenses: filteredExpenses,
        cashMovements: filteredMovements,
        vendorPerformance: vendorPerformance,
        summary: {
          totalIncome: filteredMovements
            .filter((m: any) => m.type === 'venta' || m.type === 'ingreso')
            .reduce((sum: number, m: any) => sum + parseFloat(m.amountUsd || 0), 0),
          totalExpenses: filteredExpenses
            .reduce((sum: number, e: any) => sum + parseFloat(e.amountUsd || 0), 0),
          totalDebtPayments: filteredDebtPayments
            .reduce((sum: number, d: any) => sum + parseFloat(d.amountUsd || 0), 0),
          totalMovements: filteredMovements.length
        }
      };

      // Generate Excel content as CSV format
      const excelContent = this.generateExcelCsvContent(reportContent);
      const fileName = `StockCel_Reporte_Diario_${dateStr.replace(/-/g, '_')}.csv`;

      // Store the generated report
      await this.createGeneratedReport({
        clientId,
        dailyReportId,
        reportDate,
        reportType: 'excel',
        fileName,
        fileData: Buffer.from(excelContent).toString('base64'),
        fileSize: Buffer.from(excelContent).length,
        reportContent: JSON.stringify(reportContent),
        isAutoGenerated: true
      });

      console.log(`📊 Excel report generated automatically: ${fileName}`);
    } catch (error) {
      console.error('Error generating auto Excel report:', error);
    }
  }

  // Placeholder for getPaymentMethodsSummary, replace with actual implementation if needed
  private async getPaymentMethodsSummary(clientId: number): Promise<any> {
    console.warn("getPaymentMethodsSummary is not implemented, returning empty object.");
    return {};
  }

  private generateExcelCsvContent(reportContent: any): string {
    const lines = [];

    // Header
    lines.push('STOCKCEL - REPORTE DIARIO AUTOMATICO');
    lines.push(`Fecha: ${reportContent.date}`);
    lines.push('');

    // Summary section
    lines.push('RESUMEN EJECUTIVO');
    lines.push('Concepto,Valor USD');
    lines.push(`Ingresos Totales,${reportContent.summary.totalIncome.toFixed(2)}`);
    lines.push(`Gastos Totales,${reportContent.summary.totalExpenses.toFixed(2)}`);
    lines.push(`Pagos de Deudas,${reportContent.summary.totalDebtPayments.toFixed(2)}`);
    lines.push(`Ganancia Neta,${(reportContent.summary.totalIncome - reportContent.summary.totalExpenses).toFixed(2)}`);
    lines.push(`Total Movimientos,${reportContent.summary.totalMovements}`);
    lines.push('');

    // Payment methods breakdown
    lines.push('DESGLOSE POR METODOS DE PAGO');
    lines.push('Metodo,Cantidad,Total USD');
    Object.entries(reportContent.paymentMethodBreakdown).forEach(([method, data]: any) => {
      lines.push(`${method},${data.count},${data.totalUsd.toFixed(2)}`);
    });
    lines.push('');

    // Detailed movements
    lines.push('MOVIMIENTOS DE CAJA DETALLADOS');
    lines.push('Fecha,Tipo,Descripcion,Metodo,Moneda,Monto Original,Monto USD,Cliente,Vendedor');
    reportContent.detailedPayments.forEach((movement: any) => {
      lines.push([
        new Date(movement.createdAt).toLocaleDateString('es-ES'),
        movement.type,
        movement.description || '-',
        movement.subtype || '-',
        movement.currency,
        movement.amount,
        movement.amountUsd,
        movement.customerName || '-',
        movement.vendorName || '-'
      ].join(','));
    });
    lines.push('');

    // Vendor performance
    lines.push('RENDIMIENTO DE VENDEDORES');
    lines.push('Vendedor,Ventas,Ingresos USD,Ganancia,Comision');
    reportContent.vendorPerformance.forEach((vendor: any) => {
      lines.push([
        vendor.vendorName,
        vendor.totalSales || 0,
        vendor.totalRevenue || 0,
        vendor.totalProfit || 0,
        vendor.commission || 0
      ].join(','));
    });

    return lines.join('\n');
  }

  async getAllCashMovementsForCompleteExport(clientId: number): Promise<any[]> {
    // Get all movements with related data for comprehensive export
    const movements = await this.db.select({
      id: this.cashMovementsTable.id,
      type: this.cashMovementsTable.type,
      subtype: this.cashMovementsTable.subtype,
      amount: this.cashMovementsTable.amount,
      currency: this.cashMovementsTable.currency,
      exchangeRate: this.cashMovementsTable.exchangeRate,
      amountUsd: this.cashMovementsTable.amountUsd,
      description: this.cashMovementsTable.description,
      customerName: this.customersTable.name,
      vendorName: this.vendorsTable.name,
      userId: this.cashMovementsTable.userId,
      referenceId: this.cashMovementsTable.referenceId,
      referenceType: this.cashMovementsTable.referenceType,
      createdAt: this.cashMovementsTable.createdAt,
      userName: this.usersTable.username
    })
    .from(this.cashMovementsTable)
    .leftJoin(this.usersTable, eq(this.cashMovementsTable.userId, this.usersTable.id))
    .leftJoin(this.customersTable, eq(this.cashMovementsTable.customerId, this.customersTable.id))
    .leftJoin(this.vendorsTable, eq(this.cashMovementsTable.vendorId, this.vendorsTable.id))
    .where(eq(this.cashMovementsTable.clientId, clientId))
    .orderBy(desc(this.cashMovementsTable.createdAt));

    return movements;
  }

  // =======================
  // AUTO-SYNC MONITOR METHODS
  // =======================

  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }

  // Resellers
  async createReseller(reseller: InsertReseller): Promise<Reseller> {
    console.log('🔐 Creando revendedor - Email:', reseller.email);
    console.log('🔐 Password original length:', reseller.password.length);
    console.log('🔐 Password original (primeros 10):', reseller.password.substring(0, 10));

    const hashedPassword = bcrypt.hashSync(reseller.password, 10);
    console.log('🔐 Password hasheado length:', hashedPassword.length);
    console.log('🔐 Password hasheado (primeros 20):', hashedPassword.substring(0, 20));

    const [result] = await this.db.insert(this.resellersTable).values({
      ...reseller,
      password: hashedPassword,
    }).returning();

    console.log('✅ Revendedor creado exitosamente - ID:', result.id);
    return result;
  }

  async getResellers(): Promise<Reseller[]> {
    return await this.db.select().from(this.resellersTable).orderBy(desc(this.resellersTable.createdAt));
  }

  async getResellerById(id: number): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result;
  }

  async getResellerByEmail(email: string): Promise<Reseller | undefined> {
    const [result] = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.email, email));
    return result;
  }

  async updateReseller(id: number, reseller: Partial<InsertReseller>): Promise<Reseller | undefined> {
    const updateData: any = { ...reseller, updatedAt: new Date() };

    // Hash password if provided
    if (reseller.password) {
      updateData.password = bcrypt.hashSync(reseller.password, 10);
      console.log('🔐 Actualizando revendedor - Password hasheado correctamente');
    }

    const [result] = await this.db.update(this.resellersTable)
      .set(updateData)
      .where(eq(this.resellersTable.id, id))
      .returning();
    return result;
  }

  async deleteReseller(id: number): Promise<boolean> {
    // Delete related data first
    await this.db.delete(this.resellerSalesTable).where(eq(this.resellerSalesTable.resellerId, id));
    await this.db.delete(this.resellerConfigurationTable).where(eq(this.resellerConfigurationTable.resellerId, id));

    // Delete reseller
    const result = await this.db.delete(this.resellersTable).where(eq(this.resellersTable.id, id));
    return result.rowCount > 0;
  }

  // Reseller Sales
  async createResellerSale(resellerId: number, saleData: any): Promise<ResellerSale> {
    // Create client first
    const newClient = await this.createClient({
      name: saleData.clientName,
      email: saleData.clientEmail,
      subscriptionType: saleData.subscriptionType,
      trialStartDate: new Date(),
      trialEndDate: new Date(Date.now() + (saleData.trialDays * 24 * 60 * 60 * 1000)),
    });

    // Calculate profit (salePrice - costPerAccount)
    const profit = saleData.salePrice - saleData.costPerAccount;

    // Create sale record
    const [result] = await this.db.insert(this.resellerSalesTable).values({
      resellerId,
      clientId: newClient.id,
      costPerAccount: saleData.costPerAccount.toString(),
      salePrice: saleData.salePrice.toString(),
      profit: profit.toString(),
      subscriptionType: saleData.subscriptionType,
      trialDays: saleData.trialDays,
      notes: saleData.notes,
    }).returning();

    // Update reseller stats - get current values first
    const currentReseller = await this.db.select().from(this.resellersTable).where(eq(this.resellersTable.id, resellerId)).limit(1);
    if (currentReseller.length > 0) {
      await this.db.update(this.resellersTable)
        .set({
          accountsSold: currentReseller[0].accountsSold + 1,
          totalEarnings: currentReseller[0].totalEarnings + profit,
          totalPaid: currentReseller[0].totalPaid + saleData.costPerAccount,
          updatedAt: new Date(),
        })
        .where(eq(this.resellersTable.id, resellerId));
    }

    return result;
  }

  async getResellerSales(): Promise<ResellerSale[]> {
    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    return salesWithClients as any[];
  }

  async getResellerSalesByReseller(resellerId: number): Promise<ResellerSale[]> {
    console.log(`🔍 getResellerSalesByReseller called for resellerId: ${resellerId}`);

    const salesWithClients = await this.db.select({
      id: this.resellerSalesTable.id,
      resellerId: this.resellerSalesTable.resellerId,
      clientId: this.resellerSalesTable.clientId,
      costPerAccount: this.resellerSalesTable.costPerAccount,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      subscriptionType: this.resellerSalesTable.subscriptionType,
      trialDays: this.resellerSalesTable.trialDays,
      saleDate: this.resellerSalesTable.saleDate,
      notes: this.resellerSalesTable.notes,
      createdAt: this.resellerSalesTable.createdAt,
      clientName: this.clientsTable.name,
      clientEmail: this.clientsTable.email,
    })
    .from(this.resellerSalesTable)
    .leftJoin(this.clientsTable, eq(this.resellerSalesTable.clientId, this.clientsTable.id))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.resellerSalesTable.saleDate));

    console.log(`📊 getResellerSalesByReseller result: Found ${salesWithClients.length} sales`);
    console.log(`📊 Sales IDs: ${salesWithClients.map(s => s.id).join(', ')}`);

    return salesWithClients as any[];
  }

  async getClientsByResellerId(resellerId: number): Promise<any[]> {
    // Obtener clientes creados por este revendedor a través de las ventas
    const clientsWithSales = await this.db.select({
      id: this.clientsTable.id,
      name: this.clientsTable.name,
      email: this.clientsTable.email,
      subscriptionType: this.clientsTable.subscriptionType,
      trialStartDate: this.clientsTable.trialStartDate,
      trialEndDate: this.clientsTable.trialEndDate,
      isActive: this.clientsTable.isActive,
      createdAt: this.clientsTable.createdAt,
      saleDate: this.resellerSalesTable.saleDate,
      salePrice: this.resellerSalesTable.salePrice,
      profit: this.resellerSalesTable.profit,
      notes: this.resellerSalesTable.notes
    })
    .from(this.clientsTable)
    .innerJoin(this.resellerSalesTable, eq(this.clientsTable.id, this.resellerSalesTable.clientId))
    .where(eq(this.resellerSalesTable.resellerId, resellerId))
    .orderBy(desc(this.clientsTable.createdAt));

    return clientsWithSales;
  }

  async getResellerStats(resellerId: number): Promise<any> {
    const reseller = await this.getResellerById(resellerId);
    if (!reseller) return null;

    const sales = await this.getResellerSalesByReseller(resellerId);

    const currentMonth = new Date();
    currentMonth.setDate(1);
    currentMonth.setHours(0, 0, 0, 0);

    const monthlySales = sales.filter(sale =>
      new Date(sale.saleDate) >= currentMonth
    );

    return {
      totalSales: sales.length,
      monthlyRevenue: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.salePrice), 0),
      monthlyProfit: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.profit), 0),
      monthlyCommission: monthlySales.reduce((sum, sale) => sum + parseFloat(sale.commission), 0),
      accountsQuota: reseller.accountsQuota,
      accountsSold: reseller.accountsSold,
    };
  }

  // Reseller Configuration
  async getResellerConfiguration(resellerId: number): Promise<ResellerConfiguration | undefined> {
    const [result] = await this.db.select()
      .from(this.resellerConfigurationTable)
      .where(eq(this.resellerConfigurationTable.resellerId, resellerId));

    if (!result) {
      // Create default configuration
      const defaultConfig = {
        resellerId,
        premiumMonthlyPrice: "$39.99/mes",
        premiumYearlyPrice: "$399.99/año",
        premiumYearlyDiscount: "3 meses gratis",
        defaultTrialDays: 7,
        supportMessage: "Contacta a nuestro equipo para renovar tu suscripción.",
      };

      const [created] = await this.db.insert(this.resellerConfigurationTable)
        .values(defaultConfig)
        .returning();
      return created;
    }

    return result;
  }

  async updateResellerConfiguration(resellerId: number, config: Partial<InsertResellerConfiguration>): Promise<ResellerConfiguration | undefined> {
    const existing = await this.getResellerConfiguration(resellerId);

    if (existing) {
      const [result] = await this.db.update(this.resellerConfigurationTable)
        .set({ ...config, updatedAt: new Date() })
        .where(eq(this.resellerConfigurationTable.resellerId, resellerId))
        .returning();
      return result;
    }

    return undefined;
  }

  // Password Reset Tokens
  async createPasswordResetToken(token: InsertPasswordResetToken): Promise<any> {
    const [result] = await this.db.insert(this.passwordResetTokensTable).values(token).returning();
    return result;
  }

  async getPasswordResetToken(token: string): Promise<any> {
    const [result] = await this.db.select()
      .from(this.passwordResetTokensTable)
      .where(eq(this.passwordResetTokensTable.token, token));
    return result;
  }

  async markPasswordResetTokenAsUsed(id: number): Promise<boolean> {
    try {
      await this.db.update(this.passwordResetTokensTable)
        .set({ used: true })
        .where(eq(this.passwordResetTokensTable.id, id));
      return true;
    } catch (error) {
      console.error('Error marking token as used:', error);
      return false;
    }
  }

  // Vendor Performance
  async getVendorPerformanceRanking(clientId: number): Promise<any[]> {
    try {
      const result = await this.db.execute(sqlOperator`
        WITH VendorSales AS (
          SELECT
            o.vendorId,
            v.name AS vendorName,
            COUNT(o.id) AS totalSales,
            COALESCE(SUM(o.total_usd), 0) AS totalRevenue,
            COALESCE(SUM(CAST(o.total_usd AS DECIMAL) * CAST(v.commission_percentage AS DECIMAL) / 100), 0) AS commission
          FROM orders o
          JOIN vendors v ON o.vendorId = v.id
          WHERE o.clientId = ${clientId}
          GROUP BY o.vendorId, v.name, v.commission_percentage
        )
        SELECT
          vs.vendorName,
          vs.totalSales,
          vs.totalRevenue,
          (vs.totalRevenue - vs.commission) AS totalProfit,
          vs.commission
        FROM VendorSales vs
        ORDER BY vs.totalSales DESC
      `);

      return result.rows;
    } catch (error) {
      console.error('Error fetching vendor performance ranking:', error);
      return [];
    }
  }

  // Auto-sync Monitor Methods
  async getOrdersByDate(clientId: number, date: string): Promise<Order[]> {
    const startDate = new Date(date + 'T00:00:00.000Z');
    const endDate = new Date(date + 'T23:59:59.999Z');

    return await this.db.select()
      .from(this.ordersTable)
      .where(and(
        eq(this.ordersTable.clientId, clientId),
        gte(this.ordersTable.createdAt, startDate),
        lte(this.ordersTable.createdAt, endDate)
      ))
      .orderBy(desc(this.ordersTable.createdAt));
  }

  async getCashMovementsByOrderId(orderId: number): Promise<CashMovement[]> {
    return await this.db.select()
      .from(this.cashMovementsTable)
      .where(eq(this.cashMovementsTable.referenceId, orderId));
  }

  // =======================
  // AUTOMATIC CASH SCHEDULING
  // =======================

  async scheduleCashOperations(clientId: number): Promise<{
    status: 'open' | 'closed' | 'no_config' | 'error';
    message?: string;
    nextOpen: Date | null;
    nextClose: Date | null;
    currentTime: string;
    actualCashRegister?: any;
    config?: any;
  }> {
    try {
      console.log(`🔍 [DEBUG] scheduleCashOperations called for clientId: ${clientId}`);

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        return {
          status: 'no_config',
          message: 'No hay configuración de horarios automáticos',
          nextOpen: null,
          nextClose: null,
          currentTime: new Date().toISOString()
        };
      }

      // CORRECCIÓN CRÍTICA: Verificar estado REAL de la caja en la base de datos
      const currentCashRegister = await this.getCurrentCashRegister(clientId);
      const today = new Date().toISOString().split('T')[0];

      // Get current Argentina time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      console.log(`🕐 Current Argentina time: ${argentinaTime.toLocaleString('es-AR')}`);

      // Calculate next operations
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();
      const currentTimeInMinutes = currentHour * 60 + currentMinute;

      const openTimeInMinutes = (config.openHour || 0) * 60 + (config.openMinute || 0);
      const closeTimeInMinutes = (config.closeHour || 23) * 60 + (config.closeMinute || 59);

      // CORRECCIÓN: Determinar estado basado en CAJA REAL + HORARIOS
      let status = 'closed';

      if (currentCashRegister && currentCashRegister.isOpen) {
        // Si hay una caja abierta, verificar que sea de hoy
        const registerDate = new Date(currentCashRegister.date).toISOString().split('T')[0];
        if (registerDate === today) {
          status = 'open';
          console.log(`✅ [SCHEDULE] Cash register is OPEN for today ${today} - ID: ${currentCashRegister.id}`);
        } else {
          status = 'closed';
          console.log(`⚠️ [SCHEDULE] Cash register is from different date ${registerDate}, today is ${today}`);
        }
      } else {
        status = 'closed';
        console.log(`❌ [SCHEDULE] No active cash register found for client ${clientId}`);
      }

      // Calculate next open/close times
      const todayDate = new Date(argentinaTime);
      const tomorrow = new Date(argentinaTime);
      tomorrow.setDate(tomorrow.getDate() + 1);

      let nextOpen = null;
      let nextClose = null;

      if (config.autoOpenEnabled) {
        if (status === 'closed' && currentTimeInMinutes < openTimeInMinutes) {
          // Next open is today
          nextOpen = new Date(todayDate);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        } else {
          // Next open is tomorrow
          nextOpen = new Date(tomorrow);
          nextOpen.setHours(config.openHour || 0, config.openMinute || 0, 0, 0);
        }
      }

      if (config.autoCloseEnabled) {
        if (status === 'open' && currentTimeInMinutes < closeTimeInMinutes) {
          // Next close is today
          nextClose = new Date(todayDate);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        } else {
          // Next close is tomorrow
          nextClose = new Date(tomorrow);
          nextClose.setHours(config.closeHour || 23, config.closeMinute || 59, 0, 0);
        }
      }

      const result = {
        status,
        nextOpen: nextOpen?.toISOString() ?? null,
        nextClose: nextClose?.toISOString() ?? null,
        currentTime: argentinaTime.toISOString(),
        actualCashRegister: currentCashRegister ? {
          id: currentCashRegister.id,
          isOpen: currentCashRegister.isOpen,
          date: currentCashRegister.date
        } : null,
        config: {
          openHour: config.openHour,
          openMinute: config.openMinute,
          closeHour: config.closeHour,
          closeMinute: config.closeMinute,
          autoOpenEnabled: config.autoOpenEnabled,
          autoCloseEnabled: config.autoCloseEnabled
        }
      };

      console.log(`📅 Schedule for client ${clientId}: Status=${status}, Cash Register ID=${currentCashRegister?.id}, isOpen=${currentCashRegister?.isOpen}, Current ${argentinaTime.toLocaleDateString()}, ${argentinaTime.toLocaleTimeString()}, Next Open: ${nextOpen?.toLocaleDateString()}, ${nextOpen?.toLocaleTimeString()}, Next Close: ${nextClose?.toLocaleDateString()}, ${nextClose?.toLocaleTimeString()}`);

      return result;
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      return {
        status: 'error',
        message: 'Error al obtener programación de caja',
        nextOpen: null,
        nextClose: null,
        currentTime: new Date().toISOString()
      };
    }
  }

  async checkAndProcessAutomaticOperations(clientId: number): Promise<{
    closed?: CashRegister;
    opened?: CashRegister;
    notification?: string;
  }> {
    const now = new Date();
    const currentRegister = await this.getCurrentCashRegister(clientId);
    const schedule = await this.scheduleCashOperations(clientId);

    let result: any = {};

    // Check if it's time to close (23:59:00)
    const closeTime = new Date();
    closeTime.setHours(23, 59, 0, 0);

    if (now >= closeTime && currentRegister && currentRegister.isOpen) {
      result.closed = await this.autoCloseCashRegister(clientId);
      result.notification = `🕐 Caja cerrada automáticamente a las ${closeTime.toLocaleTimeString()}. Reabrirá mañana a las 00:00:00`;
    }

    // Check if it's time to open (00:00:00)
    const openTime = new Date();
    openTime.setHours(0, 0, 0, 0);

    if (now >= openTime && (!currentRegister || !currentRegister.isOpen)) {
      // Get previous day's closing balance for opening balance
      const previousBalance = result.closed?.currentUsd || "0.00";

      result.opened = await this.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: previousBalance,
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: previousBalance,
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalDebts: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true
      });

      result.notification = `🌅 Caja abierta automáticamente a las ${openTime.toLocaleTimeString()}. Cerrará hoy a las 23:59:00`;
    }

    return result;
  }
}