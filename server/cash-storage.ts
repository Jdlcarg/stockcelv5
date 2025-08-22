import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { eq, and, gte, lt, desc, sql, sum } from "drizzle-orm";
import {
  cashRegister,
  cashMovements,
  expenses,
  debtPayments,
  dailyReports,
  customerDebts,
  orders,
  payments,
  products,
  users,
  customers,
  type CashRegister,
  type CashMovement,
  type Expense,
  type DebtPayment,
  type DailyReport,
  type CustomerDebt,
  insertCashMovementSchema,
  insertExpenseSchema,
  insertDebtPaymentSchema,
  insertDailyReportSchema,
  insertCustomerDebtSchema,
} from "@shared/schema";

type InsertCashMovement = typeof insertCashMovementSchema._type;
type InsertExpense = typeof insertExpenseSchema._type;
type InsertDebtPayment = typeof insertDebtPaymentSchema._type;
type InsertDailyReport = typeof insertDailyReportSchema._type;
type InsertCustomerDebt = typeof insertCustomerDebtSchema._type;

const connectionString = process.env.DATABASE_URL || "postgresql://stockcel_software:Kc5bpdfkr@localhost:5432/stockcel_software";
const client = postgres(connectionString);
const db = drizzle(client);

export class CashStorage {
  
  // =======================
  // CASH MOVEMENTS
  // =======================
  
  async createCashMovement(movement: InsertCashMovement): Promise<CashMovement> {
    const [result] = await db.insert(cashMovements).values(movement).returning();
    return result;
  }

  async getCashMovementsByClientId(clientId: number): Promise<CashMovement[]> {
    return await db
      .select()
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(eq(cashMovements.clientId, clientId))
      .orderBy(desc(cashMovements.createdAt))
      .then(rows => rows.map(row => ({
        ...row.cash_movements,
        userName: row.users?.username || 'Unknown',
        customerName: row.customers?.name || null,
      })));
  }

  async getCashMovementsByType(clientId: number, type: string): Promise<CashMovement[]> {
    return await db
      .select()
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        eq(cashMovements.type, type)
      ))
      .orderBy(desc(cashMovements.createdAt))
      .then(rows => rows.map(row => ({
        ...row.cash_movements,
        userName: row.users?.username || 'Unknown',
        customerName: row.customers?.name || null,
      })));
  }

  async getCashMovementsByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<CashMovement[]> {
    return await db
      .select()
      .from(cashMovements)
      .leftJoin(users, eq(cashMovements.userId, users.id))
      .leftJoin(customers, eq(cashMovements.customerId, customers.id))
      .where(and(
        eq(cashMovements.clientId, clientId),
        gte(cashMovements.createdAt, startDate),
        lt(cashMovements.createdAt, endDate)
      ))
      .orderBy(desc(cashMovements.createdAt))
      .then(rows => rows.map(row => ({
        ...row.cash_movements,
        userName: row.users?.username || 'Unknown',
        customerName: row.customers?.name || null,
      })));
  }

  // =======================
  // EXPENSES
  // =======================

  async createExpense(expense: InsertExpense): Promise<Expense> {
    const [result] = await db.insert(expenses).values(expense).returning();
    return result;
  }

  async getExpensesByClientId(clientId: number): Promise<Expense[]> {
    return await db
      .select()
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(eq(expenses.clientId, clientId))
      .orderBy(desc(expenses.createdAt))
      .then(rows => rows.map(row => ({
        ...row.expenses,
        userName: row.users?.username || 'Unknown',
      })));
  }

  async getExpensesByCategory(clientId: number, category: string): Promise<Expense[]> {
    return await db
      .select()
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        eq(expenses.category, category)
      ))
      .orderBy(desc(expenses.createdAt))
      .then(rows => rows.map(row => ({
        ...row.expenses,
        userName: row.users?.username || 'Unknown',
      })));
  }

  async getExpensesByDateRange(clientId: number, startDate: Date, endDate: Date): Promise<Expense[]> {
    return await db
      .select()
      .from(expenses)
      .leftJoin(users, eq(expenses.userId, users.id))
      .where(and(
        eq(expenses.clientId, clientId),
        gte(expenses.expenseDate, startDate),
        lt(expenses.expenseDate, endDate)
      ))
      .orderBy(desc(expenses.createdAt))
      .then(rows => rows.map(row => ({
        ...row.expenses,
        userName: row.users?.username || 'Unknown',
      })));
  }

  async updateExpense(id: number, updates: Partial<InsertExpense>): Promise<Expense | undefined> {
    const [result] = await db
      .update(expenses)
      .set(updates)
      .where(eq(expenses.id, id))
      .returning();
    return result;
  }

  async deleteExpense(id: number): Promise<boolean> {
    const result = await db.delete(expenses).where(eq(expenses.id, id));
    return result.rowCount > 0;
  }

  // =======================
  // CUSTOMER DEBTS
  // =======================

  async createCustomerDebt(debt: InsertCustomerDebt): Promise<CustomerDebt> {
    const [result] = await db.insert(customerDebts).values(debt).returning();
    return result;
  }

  async getCustomerDebtsByClientId(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select()
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.clientId, clientId))
      .orderBy(desc(customerDebts.createdAt))
      .then(rows => rows.map(row => ({
        ...row.customer_debts,
        customerName: row.customers?.name || 'Unknown Customer',
      })));
  }

  async getCustomerDebtsByCustomerId(customerId: number): Promise<CustomerDebt[]> {
    return await db
      .select()
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(eq(customerDebts.customerId, customerId))
      .orderBy(desc(customerDebts.createdAt))
      .then(rows => rows.map(row => ({
        ...row.customer_debts,
        customerName: row.customers?.name || 'Unknown Customer',
      })));
  }

  async getActiveDebts(clientId: number): Promise<CustomerDebt[]> {
    return await db
      .select()
      .from(customerDebts)
      .leftJoin(customers, eq(customerDebts.customerId, customers.id))
      .where(and(
        eq(customerDebts.clientId, clientId),
        eq(customerDebts.status, 'vigente')
      ))
      .orderBy(desc(customerDebts.createdAt))
      .then(rows => rows.map(row => ({
        ...row.customer_debts,
        customerName: row.customers?.name || 'Unknown Customer',
      })));
  }

  async updateCustomerDebt(id: number, updates: Partial<InsertCustomerDebt>): Promise<CustomerDebt | undefined> {
    const [result] = await db
      .update(customerDebts)
      .set(updates)
      .where(eq(customerDebts.id, id))
      .returning();
    return result;
  }

  // =======================
  // DEBT PAYMENTS
  // =======================

  async createDebtPayment(payment: InsertDebtPayment): Promise<DebtPayment> {
    const [result] = await db.insert(debtPayments).values(payment).returning();
    return result;
  }

  async getDebtPaymentsByDebtId(debtId: number): Promise<DebtPayment[]> {
    return await db
      .select()
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.debtId, debtId))
      .orderBy(desc(debtPayments.createdAt))
      .then(rows => rows.map(row => ({
        ...row.debt_payments,
        userName: row.users?.username || 'Unknown',
      })));
  }

  async getDebtPaymentsByClientId(clientId: number): Promise<DebtPayment[]> {
    return await db
      .select()
      .from(debtPayments)
      .leftJoin(users, eq(debtPayments.userId, users.id))
      .where(eq(debtPayments.clientId, clientId))
      .orderBy(desc(debtPayments.createdAt))
      .then(rows => rows.map(row => ({
        ...row.debt_payments,
        userName: row.users?.username || 'Unknown',
      })));
  }

  // =======================
  // DAILY REPORTS
  // =======================

  async createDailyReport(report: InsertDailyReport): Promise<DailyReport> {
    const [result] = await db.insert(dailyReports).values(report).returning();
    return result;
  }

  async getDailyReportsByClientId(clientId: number): Promise<DailyReport[]> {
    return await db
      .select()
      .from(dailyReports)
      .where(eq(dailyReports.clientId, clientId))
      .orderBy(desc(dailyReports.reportDate));
  }

  async getDailyReportByDate(clientId: number, date: Date): Promise<DailyReport | undefined> {
    const [result] = await db
      .select()
      .from(dailyReports)
      .where(and(
        eq(dailyReports.clientId, clientId),
        eq(dailyReports.reportDate, date)
      ));
    return result;
  }

  async generateAutoDailyReport(clientId: number, date: Date): Promise<DailyReport> {
    // Get all movements for the day
    const startDate = new Date(date);
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(date);
    endDate.setHours(23, 59, 59, 999);

    // Calculate totals
    const incomeResult = await db
      .select({ total: sum(cashMovements.amountUsd) })
      .from(cashMovements)
      .where(and(
        eq(cashMovements.clientId, clientId),
        gte(cashMovements.createdAt, startDate),
        lt(cashMovements.createdAt, endDate),
        sql`${cashMovements.type} IN ('ingreso', 'venta')`
      ));

    const expenseResult = await db
      .select({ total: sum(expenses.amountUsd) })
      .from(expenses)
      .where(and(
        eq(expenses.clientId, clientId),
        gte(expenses.expenseDate, startDate),
        lt(expenses.expenseDate, endDate)
      ));

    const debtPaymentResult = await db
      .select({ total: sum(debtPayments.amountUsd) })
      .from(debtPayments)
      .where(and(
        eq(debtPayments.clientId, clientId),
        gte(debtPayments.paymentDate, startDate),
        lt(debtPayments.paymentDate, endDate)
      ));

    const totalIncome = parseFloat(incomeResult[0]?.total || "0");
    const totalExpenses = parseFloat(expenseResult[0]?.total || "0");
    const totalDebtPayments = parseFloat(debtPaymentResult[0]?.total || "0");
    const netProfit = totalIncome - totalExpenses;

    const reportData = {
      movements: await this.getCashMovementsByDateRange(clientId, startDate, endDate),
      expenses: await this.getExpensesByDateRange(clientId, startDate, endDate),
      debtPayments: await this.getDebtPaymentsByClientId(clientId)
    };

    const report = await this.createDailyReport({
      clientId,
      reportDate: date,
      openingBalance: "0.00", // Should get from previous day's closing
      totalIncome: totalIncome.toFixed(2),
      totalExpenses: totalExpenses.toFixed(2),
      totalDebts: "0.00", // Calculate active debts
      totalDebtPayments: totalDebtPayments.toFixed(2),
      netProfit: netProfit.toFixed(2),
      vendorCommissions: "0.00", // Calculate vendor commissions
      exchangeRateUsed: "1200.00", // Get current rate
      closingBalance: netProfit.toFixed(2),
      totalMovements: reportData.movements.length,
      reportData: JSON.stringify(reportData),
      isAutoGenerated: true
    });

    return report;
  }

  // =======================
  // REAL-TIME STATE
  // =======================

  async getRealTimeCashState(clientId: number): Promise<any> {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    // Get today's sales (income from orders/sales)
    const salesResult = await db
      .select({ total: sum(cashMovements.amountUsd) })
      .from(cashMovements)
      .where(and(
        eq(cashMovements.clientId, clientId),
        gte(cashMovements.createdAt, today),
        lt(cashMovements.createdAt, tomorrow),
        sql`${cashMovements.type} IN ('venta', 'ingreso')`
      ));

    // Get today's expenses
    const expensesResult = await db
      .select({ total: sum(expenses.amountUsd) })
      .from(expenses)
      .where(and(
        eq(expenses.clientId, clientId),
        gte(expenses.expenseDate, today),
        lt(expenses.expenseDate, tomorrow)
      ));

    // Get active debts
    const debtsResult = await db
      .select({ total: sum(customerDebts.remainingAmount) })
      .from(customerDebts)
      .where(and(
        eq(customerDebts.clientId, clientId),
        eq(customerDebts.status, 'vigente')
      ));

    // Get total balance (simplified calculation)
    const dailySalesUsd = parseFloat(salesResult[0]?.total || "0");
    const dailyExpensesUsd = parseFloat(expensesResult[0]?.total || "0");
    const totalActiveDebtsUsd = parseFloat(debtsResult[0]?.total || "0");
    const totalBalanceUsd = dailySalesUsd - dailyExpensesUsd + 1000; // Base amount

    return {
      totalBalanceUsd: totalBalanceUsd.toFixed(2),
      dailySalesUsd: dailySalesUsd.toFixed(2),
      dailyExpensesUsd: dailyExpensesUsd.toFixed(2),
      totalActiveDebtsUsd: totalActiveDebtsUsd.toFixed(2),
      lastUpdated: new Date().toISOString()
    };
  }

  async getTotalDebtsAmount(clientId: number): Promise<number> {
    const result = await db
      .select({ total: sum(customerDebts.remainingAmount) })
      .from(customerDebts)
      .where(and(
        eq(customerDebts.clientId, clientId),
        eq(customerDebts.status, 'vigente')
      ));
    
    return parseFloat(result[0]?.total || "0");
  }

  async getStockValue(clientId: number): Promise<{usd: number, ars: number}> {
    const result = await db
      .select({ total: sum(products.costPrice) })
      .from(products)
      .where(and(
        eq(products.clientId, clientId),
        sql`${products.status} IN ('disponible', 'reservado')`
      ));
    
    const totalUsd = parseFloat(result[0]?.total || "0");
    const exchangeRate = 1200; // Should get from current rate
    
    return {
      usd: totalUsd,
      ars: totalUsd * exchangeRate
    };
  }
}

export const cashStorage = new CashStorage();