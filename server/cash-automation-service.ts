import { storage } from "./storage";
import { cashScheduleStorage } from "./cash-schedule-storage";

export class CashAutomationService {
  private intervalId: NodeJS.Timeout | null = null;
  private isRunning = false;

  // Iniciar el servicio de automatización
  start() {
    if (this.isRunning) {
      console.log('🕐 Cash automation service already running');
      return;
    }

    console.log('🕐 Starting cash automation service...');
    this.isRunning = true;

    // Verificar cada minuto
    this.intervalId = setInterval(async () => {
      await this.checkScheduledOperations();
    }, 60000); // 60 segundos

    console.log('✅ Cash automation service started');
  }

  // Detener el servicio de automatización
  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.isRunning = false;
    console.log('🛑 Cash automation service stopped');
  }

  // Verificar operaciones programadas
  private async checkScheduledOperations() {
    try {
      const timestamp = new Date().toISOString();
      console.log(`🕐 [CASH-AUTO ${timestamp}] Checking scheduled operations...`);

      // Obtener todos los clientes activos
      const clients = await storage.getAllClients();
      const clientIds = clients.map(client => client.id); // Obtener solo los IDs
      console.log(`🕐 [CASH-AUTO] Found ${clients.length} clients to check`);

      // Verificar si debe abrir cada cliente (NUEVA LÓGICA MÚLTIPLE)
      for (const clientId of clientIds) {
        try {
          const openCheck = await cashScheduleStorage.shouldExecuteAutoOperation(clientId, 'open');
          console.log(`🕐 [CASH-AUTO] Client ${clientId} should open: ${openCheck.shouldExecute}`);

          if (openCheck.reason) {
            console.log(`🔍 [CASH-AUTO] Open reason for client ${clientId}: ${openCheck.reason}`);
          }

          if (openCheck.shouldExecute && openCheck.period) {
            console.log(`🌅 [CASH-AUTO] Executing auto open for client ${clientId}, period: ${openCheck.period.periodName}`);
            await this.executeAutoOpen(clientId, openCheck.period);
          }
        } catch (error) {
          console.error(`❌ [CASH-AUTO] Error checking/executing open for client ${clientId}:`, error);
        }
      }

      // Verificar si debe cerrar cada cliente (NUEVA LÓGICA MÚLTIPLE)
      for (const clientId of clientIds) {
        try {
          const closeCheck = await cashScheduleStorage.shouldExecuteAutoOperation(clientId, 'close');
          console.log(`🕐 [CASH-AUTO] Client ${clientId} should close: ${closeCheck.shouldExecute}`);

          if (closeCheck.reason) {
            console.log(`🔍 [CASH-AUTO] Close reason for client ${clientId}: ${closeCheck.reason}`);
          }

          if (closeCheck.shouldExecute && closeCheck.period) {
            console.log(`🌆 [CASH-AUTO] Executing auto close for client ${clientId}, period: ${closeCheck.period.periodName}`);
            await this.executeAutoClose(clientId, closeCheck.period);
          }
        } catch (error) {
          console.error(`❌ [CASH-AUTO] Error checking/executing close for client ${clientId}:`, error);
        }
      }
    } catch (error) {
      console.error('❌ [CASH-AUTO] Error checking scheduled operations:', error);
    }
  }

  // Procesar operaciones programadas para un cliente específico
  private async processClientScheduledOperations(clientId: number) {
    try {
      console.log(`🕐 [CASH-AUTO] Checking operations for client ${clientId}`);

      // Verificar apertura automática
      const shouldOpen = await cashScheduleStorage.shouldExecuteAutoOperation(clientId, 'open');
      console.log(`🕐 [CASH-AUTO] Client ${clientId} should open: ${shouldOpen}`);

      if (shouldOpen) {
        console.log(`🕐 [CASH-AUTO] Executing scheduled AUTO OPEN for client ${clientId}`);
        await this.executeAutoOpen(clientId);
      }

      // Verificar cierre automático
      const shouldClose = await cashScheduleStorage.shouldExecuteAutoOperation(clientId, 'close');
      console.log(`🕐 [CASH-AUTO] Client ${clientId} should close: ${shouldClose}`);

      if (shouldClose) {
        console.log(`🕐 [CASH-AUTO] Executing scheduled AUTO CLOSE for client ${clientId}`);
        await this.executeAutoClose(clientId);
      }
    } catch (error) {
      console.error(`❌ [CASH-AUTO] Error processing operations for client ${clientId}:`, error);
    }
  }

  // Ejecutar apertura automática
  async executeAutoOpen(clientId: number, period?: any) {
    try {
      console.log(`🕐 Executing auto-open for client ${clientId}`);

      // Verificar si ya hay una caja abierta PARA HOY
      const today = new Date().toISOString().split('T')[0];
      const todaysCashRegister = await storage.getCashRegisterByDate(clientId, today);

      if (todaysCashRegister && todaysCashRegister.isOpen) {
        console.log(`⚠️ Cash register already open for today ${today} - client ${clientId}`);

        await cashScheduleStorage.logAutoOperation({
          clientId,
          operationType: 'auto_open',
          cashRegisterId: todaysCashRegister.id,
          scheduledTime: new Date(),
          status: 'success', // CAMBIAR: Ya abierta es exitoso, no omitido
          notes: `Cash register already open for ${today} - Operation successful. ID: ${todaysCashRegister.id}`,
        });

        console.log(`✅ [AUTO-OPEN] Successfully logged operation for already open cash register - Client ${clientId}`);
        return;
      }

      // Si existe caja de hoy pero está cerrada, reabrirla
      if (todaysCashRegister && !todaysCashRegister.isOpen) {
        console.log(`🔄 Reopening today's cash register for client ${clientId}`);

        const reopened = await storage.updateCashRegister(todaysCashRegister.id, {
          isOpen: true,
          reopenedAt: new Date(),
        });

        await cashScheduleStorage.logAutoOperation({
          clientId,
          operationType: 'auto_open',
          cashRegisterId: todaysCashRegister.id,
          scheduledTime: new Date(),
          status: 'success',
          notes: `Cash register reopened for ${today} - ID: ${todaysCashRegister.id}`,
        });

        console.log(`✅ [AUTO-OPEN] Successfully logged reopening operation - Client ${clientId}`);

        console.log(`✅ Auto-open (reopened) completed for client ${clientId}`);
        return;
      }

      // Crear nueva caja con valores iniciales 0
      const newCashRegister = await storage.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd: "0.00",
        initialArs: "0.00",
        initialUsdt: "0.00",
        currentUsd: "0.00",
        currentArs: "0.00",
        currentUsdt: "0.00",
        dailySales: "0.00",
        totalExpenses: "0.00",
        dailyGlobalExchangeRate: "1200.00",
        isOpen: true,
        isActive: true,
      });

      console.log(`💰 [AUTO-OPEN] Caja creada exitosamente ID: ${newCashRegister.id} para cliente ${clientId}`);
      console.log(`💰 [AUTO-OPEN] Estado de caja: isOpen=${newCashRegister.isOpen}, isActive=${newCashRegister.isActive}`);

      // Verificar que la caja se creó correctamente
      const verifyRegister = await storage.getCurrentCashRegister(clientId);
      if (verifyRegister && verifyRegister.isOpen) {
        console.log(`✅ [AUTO-OPEN] VERIFICACIÓN EXITOSA: Caja abierta y activa para cliente ${clientId}`);
      } else {
        console.log(`❌ [AUTO-OPEN] ERROR DE VERIFICACIÓN: La caja no se muestra como abierta para cliente ${clientId}`);
      }

      await cashScheduleStorage.logAutoOperation({
        clientId,
        operationType: 'auto_open',
        cashRegisterId: newCashRegister.id,
        scheduledTime: new Date(),
        status: 'success',
        notes: `Cash register opened automatically - ID: ${newCashRegister.id}, isOpen: ${newCashRegister.isOpen}`,
      });

      console.log(`✅ [AUTO-OPEN] Successfully logged new cash register creation - Client ${clientId}, Register ID: ${newCashRegister.id}`);

      console.log(`✅ Auto-open completed for client ${clientId}`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error(`❌ Error in auto-open for client ${clientId}:`, error);
      console.error(`❌ Error stack:`, error instanceof Error ? error.stack : 'No stack trace');

      await cashScheduleStorage.logAutoOperation({
        clientId,
        operationType: 'auto_open',
        status: 'failed',
        errorMessage,
        notes: `Auto-open failed: ${errorMessage}`,
      });

      throw error; // Re-throw to ensure error is visible in logs
    }
  }

  // Ejecutar cierre automático con generación de reporte
  async executeAutoClose(clientId: number, period?: any) {
    try {
      console.log(`🕐 Executing auto-close for client ${clientId}`);

      // Verificar caja de hoy específicamente
      const today = new Date().toISOString().split('T')[0];
      const todaysCashRegister = await storage.getCashRegisterByDate(clientId, today);

      if (!todaysCashRegister) {
        console.log(`⚠️ No cash register found for today ${today} - client ${clientId}`);

        await cashScheduleStorage.logAutoOperation({
          clientId,
          operationType: 'auto_close',
          scheduledTime: new Date(),
          status: 'skipped',
          notes: `No cash register found for ${today} - Nothing to close`,
        });

        console.log(`✅ [AUTO-CLOSE] Successfully logged skipped operation (no register) - Client ${clientId}`);
        return;
      }

      if (!todaysCashRegister.isOpen) {
        console.log(`⚠️ Cash register already closed for today ${today} - client ${clientId}`);

        await cashScheduleStorage.logAutoOperation({
          clientId,
          operationType: 'auto_close',
          cashRegisterId: todaysCashRegister.id,
          scheduledTime: new Date(),
          status: 'success', // CAMBIAR: Ya cerrada es exitoso, no omitido
          notes: `Cash register already closed for ${today} - Operation successful`,
        });

        console.log(`✅ [AUTO-CLOSE] Successfully logged operation for already closed register - Client ${clientId}`);
        return;
      }

      const currentCashRegister = todaysCashRegister;

      // Cerrar la caja
      await storage.updateCashRegister(currentCashRegister.id, {
        isOpen: false,
        closedAt: new Date(),
      });

      console.log(`📊 Generating automatic daily report for client ${clientId}`);

      // Generar reporte automático del día
      const reportDate = new Date();
      const report = await this.generateComprehensiveReport(clientId, reportDate);

      await cashScheduleStorage.logAutoOperation({
        clientId,
        operationType: 'auto_close',
        cashRegisterId: currentCashRegister.id,
        scheduledTime: new Date(),
        reportId: report?.id,
        status: 'success',
        notes: 'Cash register closed automatically with comprehensive report generated',
      });

      console.log(`✅ [AUTO-CLOSE] Successfully logged closing operation with report - Client ${clientId}, Register ID: ${currentCashRegister.id}`);

      console.log(`✅ Auto-close with report completed for client ${clientId}`);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error(`❌ Error in auto-close for client ${clientId}:`, error);
      console.error(`❌ Error details:`, {
        message: errorMessage,
        stack: error instanceof Error ? error.stack : 'No stack trace',
        clientId,
        timestamp: new Date().toISOString()
      });

      await cashScheduleStorage.logAutoOperation({
        clientId,
        operationType: 'auto_close',
        status: 'failed',
        errorMessage,
        notes: `Auto-close failed: ${errorMessage}`,
      });

      throw error; // Re-throw to ensure error is visible in logs
    }
  }

  // Generar reporte comprensivo con TODA la información incluyendo vendedores
  private async generateComprehensiveReport(clientId: number, reportDate: Date) {
    try {
      const startOfDay = new Date(reportDate);
      startOfDay.setHours(0, 0, 0, 0);

      const endOfDay = new Date(reportDate);
      endOfDay.setHours(23, 59, 59, 999);

      // Obtener TODOS los datos del día
      const [
        orders,
        payments,
        expenses,
        cashMovements,
        vendors,
        products,
        customers,
        debtPayments
      ] = await Promise.all([
        storage.getOrdersByDateRange(clientId, startOfDay, endOfDay),
        storage.getPaymentsByDateRange(clientId, startOfDay, endOfDay),
        storage.getExpensesByDateRange(clientId, startOfDay, endOfDay),
        storage.getCashMovementsByDateRange(clientId, startOfDay, endOfDay),
        storage.getVendorsByClientId(clientId),
        storage.getProductsByClientId(clientId),
        storage.getCustomersByClientId(clientId),
        storage.getDebtPaymentsByDateRange(clientId, startOfDay, endOfDay)
      ]);

      // Calcular estadísticas por vendedor COMPLETAS
      const vendorStats = this.calculateVendorStatistics(orders, payments, vendors, expenses);

      // Calcular totales financieros
      const totalIncome = payments.reduce((sum, p) => sum + parseFloat(p.amountUsd || "0"), 0);
      const totalExpenses = expenses.reduce((sum, e) => sum + parseFloat(e.amountUsd || "0"), 0);
      const totalDebtPayments = debtPayments.reduce((sum, dp) => sum + parseFloat(dp.amountUsd || "0"), 0);
      const netProfit = totalIncome - totalExpenses;
      const totalVendorCommissions = vendorStats.reduce((sum, v) => sum + parseFloat(v.commission), 0);

      // Crear estructura de datos COMPLETA para el reporte
      const comprehensiveReportData = {
        metadata: {
          reportType: 'automatic_daily_close',
          generatedAt: new Date().toISOString(),
          reportDate: reportDate.toISOString().split('T')[0],
          clientId,
        },
        financialSummary: {
          totalIncome: totalIncome.toFixed(2),
          totalExpenses: totalExpenses.toFixed(2),
          totalDebtPayments: totalDebtPayments.toFixed(2),
          netProfit: netProfit.toFixed(2),
          totalVendorCommissions: totalVendorCommissions.toFixed(2),
        },
        transactionDetails: {
          orders: orders.map(order => ({
            id: order.id,
            orderNumber: order.orderNumber,
            customerName: order.customerName,
            vendorName: order.vendorName,
            totalUsd: order.totalUsd,
            status: order.status,
            paymentStatus: order.paymentStatus,
            createdAt: order.createdAt,
          })),
          payments: payments.map(payment => ({
            id: payment.id,
            orderId: payment.orderId,
            paymentMethod: payment.paymentMethod,
            amount: payment.amount,
            amountUsd: payment.amountUsd,
            exchangeRate: payment.exchangeRate,
            createdAt: payment.createdAt,
          })),
          expenses: expenses.map(expense => ({
            id: expense.id,
            description: expense.description,
            category: expense.category,
            amount: expense.amount,
            amountUsd: expense.amountUsd,
            paymentMethod: expense.paymentMethod,
            provider: expense.provider,
            createdAt: expense.createdAt,
          })),
          debtPayments: debtPayments.map(dp => ({
            id: dp.id,
            orderId: dp.orderId,
            customerName: dp.customerName,
            amount: dp.amount,
            amountUsd: dp.amountUsd,
            paymentMethod: dp.paymentMethod,
            createdAt: dp.createdAt,
          })),
        },
        vendorPerformance: vendorStats,
        cashMovements: cashMovements.map(cm => ({
          id: cm.id,
          type: cm.type,
          subtype: cm.subtype,
          amount: cm.amount,
          currency: cm.currency,
          amountUsd: cm.amountUsd,
          description: cm.description,
          vendorName: cm.vendorName,
          customerName: cm.customerName,
          createdAt: cm.createdAt,
        })),
        productActivity: {
          totalProductsSold: orders.reduce((sum, order) => sum + (order.items?.length || 0), 0),
          productsChanged: products.filter(p => {
            const lastUpdate = new Date(p.updatedAt || p.createdAt);
            return lastUpdate >= startOfDay && lastUpdate <= endOfDay;
          }).length,
        },
        counts: {
          totalOrders: orders.length,
          totalPayments: payments.length,
          totalExpenses: expenses.length,
          totalCashMovements: cashMovements.length,
          totalCustomers: customers.length,
          activeVendors: vendorStats.length,
        }
      };

      // Crear el reporte en la base de datos
      const reportDataString = JSON.stringify(comprehensiveReportData, null, 2);

      const report = await storage.createDailyReport({
        clientId,
        reportDate: reportDate,
        totalIncome: totalIncome.toFixed(2),
        totalExpenses: totalExpenses.toFixed(2),
        totalDebts: "0.00", // Se calculará desde las deudas activas
        totalDebtPayments: totalDebtPayments.toFixed(2),
        netProfit: netProfit.toFixed(2),
        vendorCommissions: totalVendorCommissions.toFixed(2),
        exchangeRateUsed: "1200.00",
        reportData: reportDataString,
        isAutoGenerated: true,
        openingBalance: "0.00",
        closingBalance: netProfit.toFixed(2),
        totalMovements: cashMovements.length,
      });

      console.log(`📊 ✅ Comprehensive report generated for client ${clientId}: ${report.id}`);

      // Generar archivo Excel automáticamente
      try {
        await this.generateExcelReport(clientId, reportDate, comprehensiveReportData);
        console.log(`📊 📄 Excel report generated automatically for client ${clientId}`);
      } catch (excelError) {
        console.error('⚠️ Error generating Excel report (but daily report saved):', excelError);
      }

      return report;

    } catch (error) {
      console.error('❌ Error generating comprehensive report:', error);
      throw error;
    }
  }

  // Calcular estadísticas completas por vendedor
  private calculateVendorStatistics(orders: any[], payments: any[], vendors: any[], expenses: any[]) {
    const vendorStats = vendors.map(vendor => {
      const vendorOrders = orders.filter(order => order.vendorId === vendor.id);
      const vendorPayments = payments.filter(payment => 
        vendorOrders.some(order => order.id === payment.orderId)
      );

      const totalSales = vendorOrders.reduce((sum, order) => sum + parseFloat(order.totalUsd || "0"), 0);
      const totalPaymentsReceived = vendorPayments.reduce((sum, payment) => sum + parseFloat(payment.amountUsd || "0"), 0);

      // Calcular comisión basada en el porcentaje del vendedor
      const commissionRate = parseFloat(vendor.commissionPercentage || vendor.commission || "10");
      const estimatedProfit = totalSales * 0.3; // 30% profit margin estimate
      const commission = (estimatedProfit * commissionRate / 100);

      const completedOrders = vendorOrders.filter(order => order.status === 'completado').length;
      const paidOrders = vendorOrders.filter(order => order.paymentStatus === 'pagado').length;

      return {
        vendorId: vendor.id,
        vendorName: vendor.name,
        vendorPhone: vendor.phone || 'N/A',
        commissionRate: commissionRate.toFixed(1),
        totalOrders: vendorOrders.length,
        completedOrders,
        paidOrders,
        totalSales: totalSales.toFixed(2),
        totalPaymentsReceived: totalPaymentsReceived.toFixed(2),
        estimatedProfit: estimatedProfit.toFixed(2),
        commission: commission.toFixed(2),
        averageOrderValue: vendorOrders.length > 0 ? (totalSales / vendorOrders.length).toFixed(2) : "0.00",
        completionRate: vendorOrders.length > 0 ? ((completedOrders / vendorOrders.length) * 100).toFixed(1) : "0.0",
        paymentCollectionRate: vendorOrders.length > 0 ? ((paidOrders / vendorOrders.length) * 100).toFixed(1) : "0.0",
        orderDetails: vendorOrders.map(order => ({
          orderId: order.id,
          orderNumber: order.orderNumber,
          customerName: order.customerName,
          totalUsd: order.totalUsd,
          status: order.status,
          paymentStatus: order.paymentStatus,
          createdAt: order.createdAt,
        })),
      };
    });

    return vendorStats.filter(stats => stats.totalOrders > 0); // Solo vendedores con actividad
  }

  // Generar archivo Excel del reporte
  private async generateExcelReport(clientId: number, reportDate: Date, reportData: any) {
    try {
      // Crear estructura CSV para fácil exportación (Excel-compatible)
      const csvData = [
        // Header del reporte
        ['REPORTE DIARIO AUTOMÁTICO'],
        ['Fecha:', reportDate.toISOString().split('T')[0]],
        ['Cliente ID:', clientId.toString()],
        ['Generado:', new Date().toLocaleString('es-AR')],
        [''],

        // Resumen financiero
        ['RESUMEN FINANCIERO'],
        ['Total Ingresos:', `$${reportData.financialSummary.totalIncome}`],
        ['Total Gastos:', `$${reportData.financialSummary.totalExpenses}`],
        ['Ganancia Neta:', `$${reportData.financialSummary.netProfit}`],
        ['Comisiones Vendedores:', `$${reportData.financialSummary.totalVendorCommissions}`],
        [''],

        // Estadísticas
        ['ESTADÍSTICAS'],
        ['Total Órdenes:', reportData.counts.totalOrders.toString()],
        ['Total Pagos:', reportData.counts.totalPayments.toString()],
        ['Total Gastos:', reportData.counts.totalExpenses.toString()],
        ['Movimientos de Caja:', reportData.counts.totalCashMovements.toString()],
        ['Vendedores Activos:', reportData.counts.activeVendors.toString()],
        [''],

        // Performance de vendedores
        ['PERFORMANCE VENDEDORES'],
        ['Vendedor', 'Ventas', 'Órdenes', 'Comisión', 'Tasa Comisión'],
        ...reportData.vendorPerformance.map((vendor: any) => [
          vendor.vendorName,
          `$${vendor.totalSales}`,
          vendor.totalOrders.toString(),
          `$${vendor.commission}`,
          `${vendor.commissionRate}%`
        ])
      ];

      // Convertir a formato CSV
      const csvContent = csvData.map(row => 
        Array.isArray(row) ? row.join(',') : row
      ).join('\n');

      // Crear nombre de archivo
      const fileName = `reporte_diario_${clientId}_${reportDate.toISOString().split('T')[0]}.csv`;

      // Guardar en base de datos como reporte generado
      const base64Data = Buffer.from(csvContent).toString('base64');

      await storage.createGeneratedReport({
        clientId,
        fileName,
        reportType: 'excel',
        fileData: base64Data,
        generatedAt: new Date(),
        reportDate,
        isAutoGenerated: true
      });

      console.log(`📄 Excel report saved: ${fileName}`);
    } catch (error) {
      console.error('❌ Error generating Excel report:', error);
      throw error;
    }
  }

  // Obtener estado del servicio
  getStatus() {
    return {
      isRunning: this.isRunning,
      uptime: this.isRunning ? 'Active' : 'Stopped',
      lastCheck: new Date().toISOString(),
    };
  }

  // Método público para verificar si está corriendo
  isRunning() {
    return this.isRunning;
  }
}

// Exportar instancia singleton
export const cashAutomationService = new CashAutomationService();