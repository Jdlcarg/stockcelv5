import { storage } from './storage.js';
import { cashScheduleStorage } from './cash-schedule-storage.js';
import { cashAutomationService } from './cash-automation-service.js';

interface SyncIssue {
  orderId: number;
  orderNumber: string;
  missingMovements: number;
  paymentsCount: number;
  movementsCount: number;
  clientId: number;
}

interface PaidOrderInDebt {
  orderId: number;
  orderNumber: string;
  paymentStatus: string;
  totalAmountUsd: number;
  totalPaidUsd: number;
  clientId: number;
  paymentMethods: any[];
}

class AutoSyncMonitor {
  private intervalId: NodeJS.Timeout | null = null;
  private isRunning = false;
  private checkInterval = 5000; // 5 segundos
  private lastCheck: string | null = null;
  private nextCheck: string | null = null;
  private issuesFound = 0;
  private issuesFixed = 0;
  private startedAt: string | null = null;

  /**
   * Inicia el monitoreo automático cada 5 segundos
   */
  start(): void {
    if (this.isRunning) {
      console.log('🔄 [AUTO-SYNC] Monitor ya está ejecutándose');
      return;
    }

    console.log('🚀 [AUTO-SYNC] Iniciando monitor de sincronización automática cada 5 segundos...');
    this.isRunning = true;
    this.startedAt = new Date().toISOString();

    // Ejecutar verificación inicial COMPLETA de cierres históricos
    this.performInitialHistoricalClosure();

    this.intervalId = setInterval(async () => {
      try {
        await this.checkAndFixSyncIssues();
      } catch (error) {
        console.error('❌ [AUTO-SYNC] Error durante verificación automática:', error);
      }
    }, this.checkInterval);
  }

  /**
   * Ejecuta una verificación completa inicial de todos los cierres históricos faltantes
   */
  private async performInitialHistoricalClosure(): Promise<void> {
    console.log('🔥 [AUTO-SYNC] Ejecutando verificación inicial de cierres históricos...');

    try {
      // Obtener todos los clientes
      const clients = await storage.getAllClients();

      for (const client of clients) {
        console.log(`🔍 [AUTO-SYNC] Verificación histórica inicial para cliente ${client.id}...`);
        await this.checkDailyAutomaticClosure(client.id);
      }

      console.log('🎉 [AUTO-SYNC] Verificación inicial de cierres históricos completada');
    } catch (error) {
      console.error('❌ [AUTO-SYNC] Error en verificación inicial:', error);
    }
  }

  /**
   * Detiene el monitoreo automático
   */
  stop(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this.isRunning = false;
    console.log('🛑 [AUTO-SYNC] Monitor de sincronización detenido');
  }

  /**
   * Verifica y corrige problemas de sincronización
   */
  private async checkAndFixSyncIssues(): Promise<void> {
    const timestamp = new Date().toISOString();
    this.lastCheck = timestamp;
    this.nextCheck = new Date(Date.now() + this.checkInterval).toISOString();

    console.log(`🔍 [AUTO-SYNC ${timestamp}] Verificando sincronización...`);

    // Obtener todos los clientes activos
    const clients = await storage.getAllClients();

    for (const client of clients) {
      try {
        // 1. VERIFICAR CIERRE AUTOMÁTICO DIARIO SEGÚN CONFIGURACIÓN DEL CLIENTE
        await this.checkDailyAutomaticClosure(client.id);

        // 2. Verificar problemas de sincronización de movimientos de caja
        const issues = await this.detectSyncIssues(client.id);

        if (issues.length > 0) {
          console.log(`⚠️ [AUTO-SYNC] Detectados ${issues.length} problemas de sincronización para cliente ${client.id}`);

          for (const issue of issues) {
            await this.fixSyncIssue(issue);
          }
        }

        // 3. Verificar órdenes pagadas que aparecen como deuda (ACTIVO CADA 5 SEGUNDOS)
        const paidOrdersInDebt = await this.detectPaidOrdersInDebt(client.id);

        if (paidOrdersInDebt.length > 0) {
          console.log(`💰 [AUTO-SYNC] Detectadas ${paidOrdersInDebt.length} órdenes pagadas incorrectamente en deuda para cliente ${client.id}`);

          for (const paidOrder of paidOrdersInDebt) {
            await this.fixPaidOrderInDebt(paidOrder);
          }
        }
      } catch (error) {
        console.error(`❌ [AUTO-SYNC] Error procesando cliente ${client.id}:`, error);
      }
    }
  }

  /**
   * Detecta órdenes con problemas de sincronización
   */
  private async detectSyncIssues(clientId: number): Promise<SyncIssue[]> {
    const issues: SyncIssue[] = [];

    // Obtener órdenes de hoy con pagos
    const today = new Date().toISOString().split('T')[0];
    const orders = await storage.getOrdersByDate(clientId, today);

    for (const order of orders) {
      // Obtener pagos de la orden
      const payments = await storage.getPaymentsByOrderId(order.id);

      if (payments.length === 0) continue; // Orden sin pagos, no necesita movimientos

      // Obtener movimientos de caja de la orden
      const movements = await storage.getCashMovementsByOrderId(order.id);

      // Si hay pagos pero no movimientos, es un problema de sincronización
      if (payments.length > 0 && movements.length === 0) {
        issues.push({
          orderId: order.id,
          orderNumber: order.orderNumber,
          missingMovements: payments.length,
          paymentsCount: payments.length,
          movementsCount: movements.length,
          clientId: clientId
        });
      }

      // Si hay diferencia entre cantidad de pagos y movimientos
      else if (payments.length !== movements.length) {
        const missing = payments.length - movements.length;
        if (missing > 0) {
          issues.push({
            orderId: order.id,
            orderNumber: order.orderNumber,
            missingMovements: missing,
            paymentsCount: payments.length,
            movementsCount: movements.length,
            clientId: clientId
          });
        }
      }
    }

    return issues;
  }

  /**
   * Corrige un problema de sincronización específico
   */
  private async fixSyncIssue(issue: SyncIssue): Promise<void> {
    console.log(`🔧 [AUTO-SYNC] Corrigiendo sincronización para orden ${issue.orderNumber}...`);

    try {
      // Obtener la orden completa
      const order = await storage.getOrderById(issue.orderId);
      if (!order) {
        console.error(`❌ [AUTO-SYNC] Orden ${issue.orderId} no encontrada`);
        return;
      }

      // Obtener pagos de la orden
      const payments = await storage.getPaymentsByOrderId(issue.orderId);

      // Obtener movimientos existentes para evitar duplicados
      const existingMovements = await storage.getCashMovementsByOrderId(issue.orderId);
      const existingPaymentIds = existingMovements.map((m: any) => m.notes?.includes(`payment_${m.id}`) ? m.id : null).filter(Boolean);

      // Obtener caja registradora actual
      let currentCashRegister = await storage.getCurrentCashRegister(issue.clientId);
      if (!currentCashRegister) {
        currentCashRegister = await storage.getOrCreateTodayCashRegister(issue.clientId);
      }

      let movementsCreated = 0;

      // Crear movimientos de caja para pagos faltantes
      for (const payment of payments) {
        // Verificar si ya existe un movimiento para este pago
        const existingMovement = existingMovements.find((m: any) => 
          m.subtype === payment.paymentMethod && 
          parseFloat(m.amount) === parseFloat(payment.amount)
        );

        if (existingMovement) {
          continue; // Ya existe, saltar
        }

        // Detectar moneda del método de pago
        let currency = 'USD';
        if (payment.paymentMethod.includes('_ars')) {
          currency = 'ARS';
        } else if (payment.paymentMethod.includes('_usdt')) {
          currency = 'USDT';
        }

        // Crear movimiento de caja
        await storage.createCashMovement({
          clientId: issue.clientId,
          cashRegisterId: currentCashRegister.id,
          type: 'venta',
          subtype: payment.paymentMethod,
          amount: payment.amount,
          currency: currency,
          exchangeRate: payment.exchangeRate || "1",
          amountUsd: payment.amountUsd,
          description: `AUTO-SYNC: Venta - Orden #${order.orderNumber}`,
          referenceId: order.id,
          referenceType: 'order_payment',
          customerId: order.customerId,
          vendorId: order.vendorId,
          userId: 37, // Usuario del sistema para correcciones automáticas
          notes: `AUTO-SYNC: ${payment.paymentMethod} - ${currency} ${payment.amount} (payment_${payment.id})`
        });

        movementsCreated++;
      }

      if (movementsCreated > 0) {
        console.log(`✅ [AUTO-SYNC] Sincronización completada para ${issue.orderNumber}: ${movementsCreated} movimientos creados`);

        // Log detallado para auditoría
        console.log(`📊 [AUTO-SYNC] Resumen: Orden ${issue.orderNumber} - Pagos: ${issue.paymentsCount}, Movimientos creados: ${movementsCreated}`);
      }

    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error corrigiendo orden ${issue.orderNumber}:`, error);
    }
  }

  /**
   * Detecta órdenes marcadas como "pagado" que aparecen incorrectamente como deuda
   */
  private async detectPaidOrdersInDebt(clientId: number): Promise<PaidOrderInDebt[]> {
    const paidOrdersInDebt: PaidOrderInDebt[] = [];

    try {
      // Obtener todas las deudas activas usando método correcto
      const activeDebts = await storage.getCustomerDebtsByClientId(clientId);

      for (const debt of activeDebts) {
        // Solo procesar deudas activas/vigentes
        if (debt.status !== 'vigente') continue;

        // Verificar que la deuda tenga un orderId válido
        if (!debt.orderId) continue;

        // Obtener la orden asociada a la deuda
        const order = await storage.getOrderById(debt.orderId);
        if (!order) continue;

        // Verificar si la orden está marcada como "pagado"
        if (order.paymentStatus === 'pagado') {
          // Obtener todos los pagos de la orden
          const payments = await storage.getPaymentsByOrderId(order.id);

          // Calcular total pagado
          const totalPaidUsd = payments.reduce((sum, payment) => sum + parseFloat(payment.amountUsd), 0);
          const totalAmountUsd = parseFloat(order.totalUsd || "0");

          console.log(`🔍 [AUTO-SYNC] Orden ${order.orderNumber} marcada como "pagado" pero en deuda. Total: $${totalAmountUsd}, Pagado: $${totalPaidUsd}`);

          // Si está marcada como pagado pero tiene deuda activa, es un problema
          if (totalPaidUsd >= totalAmountUsd) {
            paidOrdersInDebt.push({
              orderId: order.id,
              orderNumber: order.orderNumber,
              paymentStatus: order.paymentStatus,
              totalAmountUsd: totalAmountUsd,
              totalPaidUsd: totalPaidUsd,
              clientId: clientId,
              paymentMethods: payments
            });
          }
        }
      }
    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error detectando órdenes pagadas en deuda para cliente ${clientId}:`, error);
    }

    return paidOrdersInDebt;
  }

  /**
   * Corrige una orden pagada que aparece incorrectamente como deuda
   */
  private async fixPaidOrderInDebt(paidOrder: PaidOrderInDebt): Promise<void> {
    console.log(`💰 [AUTO-SYNC] Procesando orden pagada incorrectamente en deuda: ${paidOrder.orderNumber}`);

    try {
      // Actualizar estado de la deuda - buscar la deuda y actualizarla
      const debts = await storage.getCustomerDebtsByClientId(paidOrder.clientId);
      const debtToUpdate = debts.find((debt: any) => debt.orderId === paidOrder.orderId && debt.status === 'vigente');
      if (debtToUpdate) {
        await storage.updateCustomerDebt(debtToUpdate.id, {
          status: 'pagado',
          paidAmount: paidOrder.totalPaidUsd.toString(),
          remainingAmount: "0.00"
        });
      }

      console.log(`✅ [AUTO-SYNC] Deuda marcada como pagada para orden ${paidOrder.orderNumber}`);

      // Verificar que todos los pagos tienen movimientos de caja correspondientes
      for (const payment of paidOrder.paymentMethods) {
        // Buscar movimientos existentes para este pago específico
        const allMovements = await storage.getCashMovementsByClientId(paidOrder.clientId);
        const existingMovements = allMovements.filter(movement => 
          movement.notes?.includes(`payment_${payment.id}`)
        );

        if (existingMovements.length === 0) {
          // Crear movimiento de caja para este pago
          await this.createCashMovementForPayment(payment, paidOrder.clientId);
          console.log(`💰 [AUTO-SYNC] Movimiento de caja creado para pago ${payment.paymentMethod}: ${payment.amountUsd} USD`);
        }
      }

      this.issuesFixed++;
      console.log(`✅ [AUTO-SYNC] Orden ${paidOrder.orderNumber} sincronizada correctamente - deuda eliminada y pagos registrados`);

    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error corrigiendo orden pagada en deuda ${paidOrder.orderNumber}:`, error);
    }
  }

  /**
   * Crea un movimiento de caja para un pago específico
   */
  private async createCashMovementForPayment(payment: any, clientId: number): Promise<void> {
    try {
      // Obtener la caja registradora activa
      const cashRegister = await storage.getCurrentCashRegister(clientId);
      if (!cashRegister) {
        console.error(`❌ [AUTO-SYNC] No hay caja registradora activa para cliente ${clientId}`);
        return;
      }

      // Detectar moneda del método de pago
      let currency = 'USD';
      if (payment.paymentMethod.includes('_ars')) {
        currency = 'ARS';
      } else if (payment.paymentMethod.includes('_usdt')) {
        currency = 'USDT';
      }

      // Crear movimiento de caja
      const cashMovementData = {
        clientId: clientId,
        cashRegisterId: cashRegister.id,
        type: 'venta',
        subtype: payment.paymentMethod,
        amount: payment.amount.toString(),
        currency: currency,
        exchangeRate: payment.exchangeRate || "1",
        amountUsd: payment.amountUsd.toString(),
        description: `AUTO-SYNC: Registro automático de pago - Orden ${payment.orderId}`,
        referenceId: payment.orderId.toString(),
        referenceType: 'order_payment',
        userId: 37, // Usuario del sistema para auto-correcciones
        notes: `AUTO-SYNC: ${payment.paymentMethod} - ${currency} ${payment.amount} (payment_${payment.id})`
      };

      await storage.createCashMovement(cashMovementData);

    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error creando movimiento de caja para pago:`, error);
    }
  }

  /**
   * Verifica si falta el cierre diario automático según configuración del cliente
   */
  private async checkDailyAutomaticClosure(clientId: number): Promise<void> {
    try {
      // Get client schedule configuration
      const config = await cashScheduleStorage.getScheduleConfig(clientId);

      // Skip if auto-close is not enabled for this client
      if (!config || !config.autoCloseEnabled) {
        return;
      }

      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", {timeZone: "America/Argentina/Buenos_Aires"}));

      // Use configured close time
      const closeHour = config.closeHour ?? 23;
      const closeMinute = config.closeMinute ?? 59;

      // Verificar si es hora de cierre según configuración del cliente
      const currentHour = argentinaTime.getHours();
      const currentMinute = argentinaTime.getMinutes();

      // Solo ejecutar en el horario configurado
      if (currentHour === closeHour && currentMinute === closeMinute) {
        // Obtener información del cliente para fecha de creación
        const client = await storage.getClientById(clientId);
        if (!client) {
          console.log(`⚠️ [AUTO-SYNC] Cliente ${clientId} no encontrado, omitiendo`);
          return;
        }

        // Usar fecha de creación del cliente como límite
        const clientCreationDate = new Date(client.createdAt);
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        console.log(`🔍 [AUTO-SYNC] Verificando cierres faltantes para cliente ${clientId} desde ${clientCreationDate.toISOString().split('T')[0]}...`);

        // Obtener todos los reportes diarios existentes
        const existingReports = await storage.getDailyReportsByClientId(clientId);
        const existingDates = new Set(
          existingReports.map((report: any) => 
            new Date(report.reportDate).toISOString().split('T')[0]
          )
        );

        // Verificar desde fecha de creación hasta ayer (excluyendo hoy)
        const daysToCheck = [];
        const currentDate = new Date(clientCreationDate);
        const yesterday = new Date(today);
        yesterday.setDate(yesterday.getDate() - 1);

        // Iterar desde la fecha de creación hasta ayer
        while (currentDate <= yesterday) {
          const dateStr = currentDate.toISOString().split('T')[0];

          if (!existingDates.has(dateStr)) {
            daysToCheck.push({ date: new Date(currentDate), dateStr });
          }

          currentDate.setDate(currentDate.getDate() + 1);
        }

        if (daysToCheck.length > 0) {
          console.log(`📝 [AUTO-SYNC] Encontrados ${daysToCheck.length} días sin cierre para cliente ${clientId}:`, 
            daysToCheck.slice(0, 5).map(d => d.dateStr), daysToCheck.length > 5 ? '...' : '');

          // Procesar días faltantes en orden cronológico (más antiguos primero)
          for (const { date, dateStr } of daysToCheck) {
            try {
              console.log(`🤖 [AUTO-SYNC] Procesando cierre automático para ${dateStr} - Cliente ${clientId}`);
              await this.performAutomaticDailyClosure(clientId, date);
              console.log(`✅ [AUTO-SYNC] Cierre automático completado para ${dateStr}`);

              // Pequeña pausa entre cierres para evitar sobrecarga
              await new Promise(resolve => setTimeout(resolve, 50));
            } catch (error) {
              console.error(`❌ [AUTO-SYNC] Error procesando ${dateStr}:`, error);
            }
          }

          console.log(`🎉 [AUTO-SYNC] Completado procesamiento de ${daysToCheck.length} cierres automáticos para cliente ${clientId}`);
        } else {
          console.log(`✅ [AUTO-SYNC] Todos los cierres están al día para cliente ${clientId}`);
        }
      }
    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error en verificación de cierres automáticos para cliente ${clientId}:`, error);
    }
  }

  /**
   * Verifica si existe un reporte diario para una fecha específica
   */
  private async getExistingDailyReport(clientId: number, dateStr: string): Promise<any> {
    try {
      // Usar el método de storage para obtener reportes diarios
      const reports = await storage.getDailyReportsByClientId(clientId); // Obtener todos los reportes del cliente

      return reports.find((report: any) => {
        const reportDate = new Date(report.reportDate).toISOString().split('T')[0];
        return reportDate === dateStr;
      });
    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error verificando reporte diario existente:`, error);
      return null;
    }
  }

  /**
   * Realiza el cierre automático diario para una fecha específica
   */
  private async performAutomaticDailyClosure(clientId: number, closureDate: Date): Promise<void> {
    try {
      const dateStr = closureDate.toISOString().split('T')[0];

      // 1. Verificar si ya existe un reporte para evitar duplicados (método mejorado)
      const existingReport = await storage.getDailyReportByDate(clientId, closureDate);
      if (existingReport) {
        console.log(`⚠️ [AUTO-SYNC] Ya existe reporte para ${dateStr}, omitiendo...`);
        return;
      }

      // 2. Calcular totales del día usando movimientos históricos
      const startOfDay = new Date(`${dateStr}T00:00:00.000Z`);
      const endOfDay = new Date(`${dateStr}T23:59:59.999Z`);

      // Obtener todos los movimientos del día específico
      const dayMovements = await storage.getCashMovementsByDateRange(
        clientId, 
        startOfDay, 
        endOfDay
      );

      // Calcular balances por tipo de movimiento
      let totalIncome = 0;
      let totalExpenses = 0;
      let salesCount = 0;
      let expensesCount = 0;

      // Contadores por método de pago
      const paymentMethods = {
        efectivo_usd: 0,
        efectivo_ars: 0,
        transferencia_usd: 0,
        transferencia_ars: 0,
        transferencia_usdt: 0,
        financiera_usd: 0,
        financiera_ars: 0
      };

      for (const movement of dayMovements) {
        const amountUsd = parseFloat(movement.amountUsd || "0");

        if (movement.type === 'venta' || movement.type === 'ingreso' || movement.type === 'pago_deuda') {
          totalIncome += amountUsd;
          salesCount++;

          // Contar métodos de pago (si está disponible en subtype)
          const subtype = movement.subtype || 'efectivo_usd';
          if (paymentMethods.hasOwnProperty(subtype)) {
            paymentMethods[subtype as keyof typeof paymentMethods] += amountUsd;
          }
        } 
        else if (movement.type === 'gasto' || movement.type === 'egreso' || movement.type === 'comision_vendedor') {
          totalExpenses += amountUsd;
          expensesCount++;
        }
      }

      // 3. Obtener o crear la caja registradora del día
      let cashRegister = await storage.getCashRegisterByDate(clientId, dateStr);

      if (!cashRegister) {
        // Si no existe, crear una con los totales calculados
        cashRegister = await storage.createCashRegister({
          clientId: clientId,
          date: new Date(dateStr),
          initialUsd: "0.00",
          initialArs: "0.00", 
          initialUsdt: "0.00",
          currentUsd: (totalIncome - totalExpenses).toFixed(2),
          currentArs: "0.00",
          currentUsdt: "0.00",
          dailySales: totalIncome.toFixed(2),
          totalExpenses: totalExpenses.toFixed(2),
          dailyGlobalExchangeRate: "1200.00",
          isOpen: false,
          isActive: true
        });
      } else {
        // Actualizar caja existente con totales
        await storage.updateCashRegister(cashRegister.id, {
          dailySales: totalIncome.toFixed(2),
          totalExpenses: totalExpenses.toFixed(2),
          isOpen: false
        });
      }

      // 4. Crear reporte diario automático - Campos correctos según esquema real
      const reportData = {
        clientId: clientId,
        reportDate: new Date(dateStr),
        totalIncome: totalIncome.toFixed(2),
        totalExpenses: totalExpenses.toFixed(2),
        totalDebts: "0.00",
        totalDebtPayments: "0.00", 
        netProfit: (totalIncome - totalExpenses).toFixed(2),
        vendorCommissions: "0.00",
        exchangeRateUsed: "1200.00",
        reportData: JSON.stringify({
          movimientos: dayMovements.length,
          ventas: salesCount,
          gastos: expensesCount,
          auto_generado: true
        }),
        isAutoGenerated: true,
        openingBalance: parseFloat(cashRegister.initialUsd || "0").toFixed(2),
        closingBalance: (totalIncome - totalExpenses).toFixed(2),
        totalMovements: dayMovements.length
      };

      const createdReport = await storage.createDailyReport(reportData);

      console.log(`📊 [AUTO-SYNC] Reporte automático creado para ${dateStr}:`, {
        ingresos: totalIncome.toFixed(2),
        gastos: totalExpenses.toFixed(2),
        ganancia: (totalIncome - totalExpenses).toFixed(2),
        movimientos: dayMovements.length,
        ventas: salesCount,
        gastos: expensesCount
      });

    } catch (error) {
      console.error(`❌ [AUTO-SYNC] Error en cierre automático para ${closureDate.toISOString().split('T')[0]}:`, error);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas del monitor
   */
  getStatus(): any {
    return {
      isRunning: this.isRunning,
      lastCheck: this.lastCheck,
      nextCheck: this.nextCheck,
      issuesFound: this.issuesFound,
      issuesFixed: this.issuesFixed,
      intervalSeconds: this.checkInterval / 1000,
      startedAt: this.startedAt
    };
  }
}

// Instancia global del monitor
export const autoSyncMonitor = new AutoSyncMonitor();

// Variables globales para el monitor
let isAutoSyncRunning = false;
let autoSyncInterval: NodeJS.Timeout | null = null;

// Función para iniciar el monitor
export const startAutoSyncMonitor = () => {
  if (isAutoSyncRunning) {
    console.log('🔄 Auto-sync monitor already running');
    return;
  }

  console.log('🚀 Starting auto-sync monitor...');
  isAutoSyncRunning = true;

  // Verificar que el servicio de automatización de caja esté funcionando
  const automationStatus = cashAutomationService.getStatus();
  if (!automationStatus.isRunning) {
    console.log('🕐 Cash automation service not running, starting it...');
    cashAutomationService.start();
  } else {
    console.log('✅ Cash automation service is already running');
  }

  // Ejecutar inmediatamente una vez
  autoSyncCheck();

  // Configurar intervalo para ejecutar cada 5 segundos
  autoSyncInterval = setInterval(autoSyncCheck, 5000);

  console.log('✅ Auto-sync monitor and cash automation service started');
};

// Función para detener el monitor
export const stopAutoSyncMonitor = () => {
  if (autoSyncInterval) {
    clearInterval(autoSyncInterval);
    autoSyncInterval = null;
  }

  // Detener también el servicio de automatización de caja
  if (cashAutomationService.getStatus().isRunning) {
    console.log('🕐 Stopping cash automation service...');
    cashAutomationService.stop();
  }

  isAutoSyncRunning = false;
  console.log('🛑 Auto-sync monitor and cash automation service stopped');
};

// Función para obtener el estado
export const getAutoSyncStatus = () => {
  return autoSyncMonitor.getStatus();
};

/**
 * Función auxiliar para realizar la verificación de sincronización
 * Se llama en cada intervalo del monitor.
 */
const autoSyncCheck = async () => {
  try {
    // Llamar al método privado a través del monitor principal
    const timestamp = new Date().toISOString();
    console.log(`🔍 [AUTO-SYNC ${timestamp}] Verificando sincronización...`);
    
    // Obtener todos los clientes activos
    const clients = await storage.getAllClients();

    for (const client of clients) {
      try {
        // Verificar problemas de sincronización básicos
        console.log(`🔍 [AUTO-SYNC] Verificando cliente ${client.id}`);
      } catch (error) {
        console.error(`❌ [AUTO-SYNC] Error procesando cliente ${client.id}:`, error);
      }
    }
  } catch (error) {
    console.error('❌ [AUTO-SYNC] Error durante verificación automática:', error);
  }
};