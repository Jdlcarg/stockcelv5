import { useState, useEffect, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { useAuth } from "@/lib/auth";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import Footer from "@/components/layout/footer";
import RealTimeClock from "@/components/layout/real-time-clock";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { 
  DollarSign, 
  TrendingUp, 
  TrendingDown, 
  Users, 
  ShoppingCart, 
  FileText,
  PlusCircle,
  Filter,
  Download,
  Search,
  Calendar,
  AlertCircle,
  RefreshCw,
  Eye,
  Calculator,
  CreditCard,
  Wallet,
  Trophy,
  Target,
  UserCheck,
  Award,
  ChevronDown,
  Settings
} from "lucide-react";

// Interfaces
interface CashRegister {
  id: number;
  clientId: number;
  date: string;
  initialUsd: string;
  initialArs: string;
  initialUsdt: string;
  currentUsd: string;
  currentArs: string;
  currentUsdt: string;
  dailySales: string;
  totalDebts: string;
  totalExpenses: string;
  dailyGlobalExchangeRate: string;
  isOpen: boolean;
  isActive: boolean;
  closedAt?: string;
  autoClosedAt?: string;
  createdAt: string;
}

interface CashMovement {
  id: number;
  type: string;
  subtype: string;
  amount: string;
  currency: string;
  exchangeRate?: string;
  amountUsd: string;
  description: string;
  customerName?: string;
  vendorName?: string;
  userName: string;
  createdAt: string;
}

interface Expense {
  id: number;
  category: string;
  description: string;
  amount: string;
  currency: string;
  amountUsd: string;
  paymentMethod: string;
  receiptNumber?: string;
  notes?: string;
  expenseDate: string;
  userName: string;
}

interface CustomerDebt {
  id: number;
  customerName: string;
  debtAmount: string;
  paidAmount: string;
  remainingAmount: string;
  currency: string;
  status: string;
  dueDate?: string;
  createdAt: string;
}

interface VendorProfit {
  vendorId: number;
  vendorName: string;
  totalSales: string;
  totalRevenue: string;
  netProfit: string;
  profitMargin: string;
  orderCount: number;
  avgOrderValue: string;
  commission: string;
  commissionRate: string;
  period: string;
}

interface VendorRanking {
  vendorId: number;
  vendorName: string;
  totalSales: string;
  netProfit: string;
  profitMargin: string;
  orderCount: number;
  avgOrderValue: string;
  ranking: number;
  performanceScore: number;
}

interface VendorPerformanceSummary {
  totalVendors: number;
  totalSales: string;
  totalProfit: string;
  avgProfitMargin: string;
  topPerformer: string;
  period: string;
  dateRange: {
    from: string;
    to: string;
  };
}

export default function CashAdvanced() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Estados de filtros
  const [filterType, setFilterType] = useState<string>("all");
  const [filterDateFrom, setFilterDateFrom] = useState("");
  const [filterDateTo, setFilterDateTo] = useState("");
  const [filterPaymentMethod, setFilterPaymentMethod] = useState("all");
  const [filterCurrency, setFilterCurrency] = useState("all");
  const [filterCustomer, setFilterCustomer] = useState("");
  const [filterVendor, setFilterVendor] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [timeToClose, setTimeToClose] = useState("");

  // Estados para rendimiento de vendedores
  const [selectedPeriod, setSelectedPeriod] = useState<string>("month");
  const [dailyReportFilter, setDailyReportFilter] = useState<string>("week");
  const [selectedVendor, setSelectedVendor] = useState<string | null>(null);
  const [expandedSales, setExpandedSales] = useState<Set<number>>(new Set());

  // Estados de modales
  const [showExpenseModal, setShowExpenseModal] = useState(false);
  const [showDebtPaymentModal, setShowDebtPaymentModal] = useState(false);
  const [showIncomeModal, setShowIncomeModal] = useState(false);
  const [showHistoryModal, setShowHistoryModal] = useState(false);

  // Estados de formularios
  const [expenseForm, setExpenseForm] = useState({
    category: "",
    description: "",
    amount: "",
    currency: "USD",
    paymentMethod: "efectivo_usd",
    receiptNumber: "",
    notes: ""
  });

  const [debtPaymentForm, setDebtPaymentForm] = useState({
    debtId: "",
    amount: "",
    currency: "USD",
    paymentMethod: "efectivo_usd",
    notes: ""
  });

  const [incomeForm, setIncomeForm] = useState({
    description: "",
    amount: "",
    currency: "USD",
    paymentMethod: "efectivo_usd",
    category: "otros",
    notes: ""
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Obtener estado de caja actual (siempre activa)
  // Obtener estado de caja actual
  const { data: cashRegister, isLoading } = useQuery({
    queryKey: ["/api/cash-register/current", user?.clientId],
    queryFn: async () => {
      console.log(`🔍 [FRONTEND] Calling /api/cash-register/current for clientId: ${user?.clientId}`);
      const response = await fetch(`/api/cash-register/current?clientId=${user?.clientId}`);
      console.log(`🔍 [FRONTEND] Response status: ${response.status}`);
      
      if (!response.ok) {
        console.log(`❌ [FRONTEND] Response not OK: ${response.status}`);
        if (response.status === 404) {
          console.log(`❌ [FRONTEND] 404 - No cash register found`);
          return null; // No hay caja abierta
        }
        throw new Error("Error al obtener estado de caja");
      }
      
      const data = await response.json();
      console.log(`✅ [FRONTEND] Cash register data received:`, {
        id: data?.id,
        isOpen: data?.isOpen,
        date: data?.date,
        clientId: data?.clientId
      });
      
      return data;
    },
    enabled: !!user?.clientId,
    refetchInterval: 30000, // Actualizar cada 30 segundos
    staleTime: 0, // Siempre considerar datos como stale para forzar refetch
  });

  // Obtener movimientos de caja
  const { data: cashMovements } = useQuery({
    queryKey: ['/api/cash-movements', user?.clientId, filterType, filterDateFrom, filterDateTo, filterCustomer, filterVendor, searchQuery, filterPaymentMethod],
    queryFn: async () => {
      let url = `/api/cash-movements?clientId=${user?.clientId}`;
      if (filterType !== "all") url += `&type=${filterType}`;
      if (filterDateFrom) url += `&dateFrom=${filterDateFrom}`;
      if (filterDateTo) url += `&dateTo=${filterDateTo}`;
      if (filterCustomer) url += `&customer=${encodeURIComponent(filterCustomer)}`;
      if (filterVendor) url += `&vendor=${encodeURIComponent(filterVendor)}`;
      if (searchQuery) url += `&search=${encodeURIComponent(searchQuery)}`;
      if (filterPaymentMethod !== "all") url += `&paymentMethod=${encodeURIComponent(filterPaymentMethod)}`;

      const response = await fetch(url);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Obtener gastos
  const { data: expenses } = useQuery({
    queryKey: ['/api/expenses', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/expenses?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });



  // Obtener estado financiero en tiempo real
  const { data: realTimeState } = useQuery({
    queryKey: ['/api/cash-register/real-time-state', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-register/real-time-state?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: false, // Actualizar solo cuando sea necesario
  });

  // Obtener configuración de horarios de caja (MISMA API QUE /cash-schedule)
  const { data: cashScheduleConfig, isLoading: loadingScheduleConfig } = useQuery({
    queryKey: ['/api/cash-schedule/config', user?.clientId],
    queryFn: async () => {
      console.log(`🔍 [FRONTEND] Calling /api/cash-schedule/config for clientId: ${user?.clientId}`);
      const response = await fetch(`/api/cash-schedule/config?clientId=${user?.clientId}`);
      console.log(`🔍 [FRONTEND] Schedule config response status: ${response.status}`);
      
      if (!response.ok) {
        console.log(`❌ [FRONTEND] Schedule config failed: ${response.status}`);
        throw new Error('Failed to fetch schedule configuration');
      }
      
      const data = await response.json();
      console.log(`📋 [FRONTEND] Schedule config received:`, {
        openHour: data.openHour,
        openMinute: data.openMinute,
        closeHour: data.closeHour,
        closeMinute: data.closeMinute,
        autoOpenEnabled: data.autoOpenEnabled,
        autoCloseEnabled: data.autoCloseEnabled
      });
      
      return data;
    },
    enabled: !!user?.clientId,
    refetchInterval: 5000, // Actualizar cada 5 segundos para sincronización
  });

  // Obtener programación automática de caja (para estado actual)
  const { data: cashSchedule } = useQuery({
    queryKey: ['/api/cash-register/schedule', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-register/schedule?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 30000, // Actualizar cada 30 segundos
  });

  // Obtener historial de reportes diarios con filtros
  const { data: dailyReports } = useQuery({
    queryKey: ['/api/daily-reports', user?.clientId, dailyReportFilter],
    queryFn: async () => {
      let url = `/api/daily-reports?clientId=${user?.clientId}`;
      if (dailyReportFilter !== "all") {
        url += `&filter=${dailyReportFilter}`;
      }
      console.log(`🔍 Consultando reportes diarios con filtro: ${dailyReportFilter}, URL: ${url}`);
      const response = await fetch(url);
      const data = await response.json();
      console.log(`📊 Reportes obtenidos: ${data.length} registros con filtro: ${dailyReportFilter}`);
      return data;
    },
    enabled: !!user?.clientId,
  });

  // Obtener reportes Excel generados automáticamente
  const { data: generatedReports } = useQuery({
    queryKey: ['/api/generated-reports', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/generated-reports?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });



  // Estado de notificación automática
  const [autoNotification, setAutoNotification] = useState<string>("");

  // Queries para rendimiento de vendedores
  const { data: vendorPerformanceSummary } = useQuery<VendorPerformanceSummary>({
    queryKey: ['/api/vendor-performance/summary', user?.clientId, selectedPeriod],
    queryFn: async () => {
      const response = await fetch(`/api/vendor-performance/summary?clientId=${user?.clientId}&period=${selectedPeriod}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: vendorRankings } = useQuery<VendorRanking[]>({
    queryKey: ['/api/vendor-performance/ranking', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/vendor-performance/ranking?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: vendorProfits } = useQuery<VendorProfit[]>({
    queryKey: ['/api/vendor-performance/profits', user?.clientId, selectedVendor],
    queryFn: async () => {
      let url = `/api/vendor-performance/profits?clientId=${user?.clientId}`;
      if (selectedVendor) url += `&vendorId=${selectedVendor}`;
      const response = await fetch(url);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Mutación para crear gasto
  const createExpenseMutation = useMutation({
    mutationFn: async (expenseData: any) => {
      return apiRequest('POST', '/api/expenses', {
        ...expenseData,
        clientId: user?.clientId,
        userId: user?.id,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/expenses'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register'] });
      setShowExpenseModal(false);
      setExpenseForm({
        category: "",
        description: "",
        amount: "",
        currency: "USD",
        paymentMethod: "efectivo_usd",
        receiptNumber: "",
        notes: ""
      });
      toast({
        title: "Éxito",
        description: "Gasto registrado correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo registrar el gasto",
        variant: "destructive",
      });
    },
  });

  // Mutación para pago de deuda
  const createDebtPaymentMutation = useMutation({
    mutationFn: async (paymentData: any) => {
      return apiRequest('POST', '/api/debt-payments', {
        ...paymentData,
        clientId: user?.clientId,
        userId: user?.id,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/customer-debts'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register'] });
      setShowDebtPaymentModal(false);
      setDebtPaymentForm({
        debtId: "",
        amount: "",
        currency: "USD",
        paymentMethod: "efectivo_usd",
        notes: ""
      });
      toast({
        title: "Éxito",
        description: "Pago de deuda registrado correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo registrar el pago",
        variant: "destructive",
      });
    },
  });

  // Mutación para registrar ingreso manual
  const createIncomeMutation = useMutation({
    mutationFn: async (incomeData: any) => {
      return apiRequest('POST', '/api/cash-movements', {
        ...incomeData,
        type: 'ingreso',
        clientId: user?.clientId,
        userId: user?.id,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/cash-movements'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register'] });
      setShowIncomeModal(false);
      setIncomeForm({
        description: "",
        amount: "",
        currency: "USD",
        paymentMethod: "efectivo_usd",
        category: "otros",
        notes: ""
      });
      toast({
        title: "Éxito",
        description: "Ingreso registrado correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo registrar el ingreso",
        variant: "destructive",
      });
    },
  });



  // Función para calcular cuenta regresiva
  const calculateTimeToClose = () => {
    const now = new Date();
    const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

    // Obtener horario de cierre desde la configuración
    let closeHour = 23;
    let closeMinute = 59;

    if (cashScheduleConfig && cashScheduleConfig.autoCloseEnabled) {
      closeHour = cashScheduleConfig.closeHour || 23;
      closeMinute = cashScheduleConfig.closeMinute || 59;
    }

    // Crear fecha de cierre para hoy según configuración
    const closeTime = new Date(argentinaTime);
    closeTime.setHours(closeHour, closeMinute, 0, 0);

    // Si ya pasó la hora de cierre, el cierre será mañana
    if (argentinaTime > closeTime) {
      closeTime.setDate(closeTime.getDate() + 1);
    }

    const timeDiff = closeTime.getTime() - argentinaTime.getTime();

    if (timeDiff <= 0) {
      return "Cerrando...";
    }

    const hours = Math.floor(timeDiff / (1000 * 60 * 60));
    const minutes = Math.floor((timeDiff % (1000 * 60 * 60)) / (1000 * 60));
    const seconds = Math.floor((timeDiff % (1000 * 60)) / 1000);

    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
  };

  // Efecto para actualizar cuenta regresiva cada segundo
  useEffect(() => {
    const updateCountdown = () => {
      setTimeToClose(calculateTimeToClose());
    };

    updateCountdown(); // Actualizar inmediatamente
    const interval = setInterval(updateCountdown, 1000);

    return () => clearInterval(interval);
  }, [cashScheduleConfig]); // Recalcular cuando cambie la configuración de horarios

  const categoriesExpenses = [
    { value: "alquiler", label: "Alquiler" },
    { value: "servicios", label: "Servicios" },
    { value: "compras", label: "Compras" },
    { value: "sueldos", label: "Sueldos" },
    { value: "marketing", label: "Marketing" },
    { value: "mantenimiento", label: "Mantenimiento" },
    { value: "impuestos", label: "Impuestos" },
    { value: "otros", label: "Otros" }
  ];

  const paymentMethods = [
    { value: "efectivo_ars", label: "Efectivo ARS" },
    { value: "efectivo_usd", label: "Efectivo USD" },
    { value: "transferencia_ars", label: "Transferencia ARS" },
    { value: "transferencia_usd", label: "Transferencia USD" },
    { value: "transferencia_usdt", label: "Transferencia USDT" },
    { value: "financiera_usd", label: "Financiera USD→ARS" },
    { value: "financiera_ars", label: "Financiera ARS→USD" }
  ];

  const movementTypes = [
    { value: "all", label: "Todos los movimientos" },
    { value: "ingreso", label: "Ingresos" },
    { value: "egreso", label: "Egresos" },
    { value: "venta", label: "Ventas" },
    { value: "gasto", label: "Gastos" },
    { value: "pago_deuda", label: "Pagos de Deuda" },
    { value: "comision_vendedor", label: "Comisiones" }
  ];

  // Filtered movements with proper currency filtering
  const filteredMovements = useMemo(() => {
    if (!cashMovements) return [];

    return cashMovements.filter((movement: CashMovement) => {
      // Filter by currency if not "all"
      if (filterCurrency !== "all" && movement.currency !== filterCurrency) {
        return false;
      }

      // Filter by payment method if not "all"
      if (filterPaymentMethod !== "all") {
        // Match payment method with currency
        const expectedCurrency = filterPaymentMethod.includes("usd") ? "USD" : 
                                filterPaymentMethod.includes("ars") ? "ARS" : 
                                filterPaymentMethod.includes("usdt") ? "USDT" : null;
        if (expectedCurrency && movement.currency !== expectedCurrency) {
          return false;
        }
      }

      return true;
    });
  }, [cashMovements, filterCurrency, filterPaymentMethod]);

  const activeFilters = {
    type: filterType !== 'all' ? filterType : null,
    dateFrom: filterDateFrom || null,
    dateTo: filterDateTo || null,
    customer: filterCustomer || null,
    vendor: filterVendor || null,
    search: searchQuery || null,
    currency: filterCurrency !== 'all' ? filterCurrency : null,
    paymentMethod: filterPaymentMethod !== 'all' ? filterPaymentMethod : null
  };

  // Export functions
  const generatePDFReport = async () => {
    try {
      // Use existing cash movements data
      const allMovements = cashMovements || [];

      const reportData = {
        date: new Date().toLocaleDateString('es-ES'),
        time: new Date().toLocaleTimeString('es-ES'),
        cashState: realTimeState,
        movements: allMovements || [],
        filters: activeFilters,
        dailyHistory: dailyReports?.slice(0, 7) || [], // Last 7 days
        profitData: vendorProfits ? {
          cajaPagosVendedores: vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0).toFixed(2),
          cajaGananciasNetas: (vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0) - vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0)).toFixed(2),
          cajaCostosVendidos: vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0).toFixed(2),
          cajaCostosProducto: vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalCost || '0'), 0).toFixed(2)
        } : null
      };

    const printWindow = window.open('', '_blank');
    if (printWindow) {
      printWindow.document.write(`
        <!DOCTYPE html>
        <html>
        <head>
          <title>Reporte de Caja - ${reportData.date}</title>
          <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #333; padding-bottom: 10px; }
            .summary { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-bottom: 30px; }
            .summary-card { border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
            .summary-card h3 { margin: 0 0 10px 0; color: #333; }
            .amount { font-size: 1.5em; font-weight: bold; color: #2563eb; }
            .movements { margin-top: 20px; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f5f5f5; }
            .positive { color: #16a34a; }
            .negative { color: #dc2626; }
            @media print { body { margin: 0; } }
          </style>
        </head>
        <body>
          <div class="header">
            <h1>Sistema de Cajas - Reporte Detallado</h1>
            <p>Fecha: ${reportData.date} | Hora: ${reportData.time}</p>
          </div>

          <div class="summary">
            <div class="summary-card">
              <h3>Saldo Total</h3>
              <div class="amount">$${realTimeState?.totalBalanceUsd || '0.00'}</div>
              <p>Estado en tiempo real</p>
            </div>
            <div class="summary-card">
              <h3>Ventas del Día</h3>
              <div class="amount positive">$${realTimeState?.dailySalesUsd || '0.00'}</div>
              <p>Ingresos por ventas</p>
            </div>
            <div class="summary-card">
              <h3>Gastos del Día</h3>
              <div class="amount negative">$${realTimeState?.dailyExpensesUsd || '0.00'}</div>
              <p>Gastos registrados</p>
            </div>
            <div class="summary-card">
              <h3>Deudas Activas</h3>
              <div class="amount">$${realTimeState?.totalActiveDebtsUsd || '0.00'}</div>
              <p>Pendientes de cobro</p>
            </div>
            ${reportData.profitData ? `
          </div>

          <div class="summary" style="margin-top: 30px;">
            <h2 style="text-align: center; margin-bottom: 20px; color: #333;">📊 Análisis de Ganancias y Costos</h2>
            <div class="summary-card">
              <h3>Caja Pagos Vendedores</h3>
              <div class="amount" style="color: #e91e63;">$${reportData.profitData.cajaPagosVendedores}</div>
              <p>Comisiones vendedores (%)</p>
            </div>
            <div class="summary-card">
              <h3>Caja Ganancias Netas</h3>
              <div class="amount" style="color: #10b981;">$${reportData.profitData.cajaGananciasNetas}</div>
              <p>Ganancia del admin por la venta</p>
            </div>
            <div class="summary-card">
              <h3>Caja Costos Vendidos</h3>
              <div class="amount" style="color: #f43f5e;">$${reportData.profitData.cajaCostosVendidos}</div>
              <p>Costo total de productos vendidos</p>
            </div>
            <div class="summary-card">
              <h3>Caja Costos Producto</h3>
              <div class="amount" style="color: #64748b;">$${reportData.profitData.cajaCostosProducto}</div>
              <p>Costo base de productos</p>
            </div>
            ` : ''}
          </div>

          ${reportData.dailyHistory?.length > 0 ? `
          <div class="daily-history" style="margin-bottom: 30px;">
            <h2>Historial Diario (Últimos 7 días)</h2>
            <table>
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Balance Apertura</th>
                  <th>Ingresos</th>
                  <th>Gastos</th>
                  <th>Balance Cierre</th>
                  <th>Ganancia Neta</th>
                  <th>Movimientos</th>
                </tr>
              </thead>
              <tbody>
                ${reportData.dailyHistory.map(report => `
                  <tr>
                    <td>${new Date(report.reportDate).toLocaleDateString('es-ES')}</td>
                    <td class="amount">$${report.openingBalance}</td>
                    <td class="positive">$${report.totalIncome}</td>
                    <td class="negative">$${report.totalExpenses}</td>
                    <td class="amount">$${report.closingBalance}</td>
                    <td class="${parseFloat(report.netProfit) >= 0 ? 'positive' : 'negative'}">$${report.netProfit}</td>
                    <td>${report.totalMovements}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
          ` : ''}

          <div class="movements">
            <h2>Todos los Movimientos de Caja (${reportData.movements?.length || 0} registros)</h2>
            <table>
              <thead>
                <tr>
                  <th>Fecha</th>
                  <th>Tipo</th>
                  <th>Descripción</th>
                  <th>Método</th>
                  <th>Moneda</th>
                  <th>Monto Original</th>
                  <th>Monto (USD)</th>
                  <th>Cliente</th>
                  <th>Vendedor</th>
                </tr>
              </thead>
              <tbody>
                ${reportData.movements?.slice(0, 100).map((movement: any) => `
                  <tr>
                    <td>${new Date(movement.createdAt).toLocaleDateString('es-ES')}</td>
                    <td>${movement.type}</td>
                    <td>${movement.description || '-'}</td>
                    <td>${movement.subtype || '-'}</td>
                    <td>${movement.currency}</td>
                    <td>$${movement.amount}</td>
                    <td class="${parseFloat(movement.amountUsd) >= 0 ? 'positive' : 'negative'}">
                      $${Math.abs(parseFloat(movement.amountUsd)).toFixed(2)}
                    </td>
                    <td>${movement.customerName || '-'}</td>
                    <td>${movement.userName || '-'}</td>
                  </tr>
                `).join('') || '<tr><td colspan="9">No hay movimientos para mostrar</td></tr>'}
              </tbody>
            </table>
          </div>

          <div style="margin-top: 30px; text-align: center; color: #666; font-size: 0.9em;">
            <p>Reporte generado automáticamente por StockCel</p>
            <p>www.softwarepar.com</p>
          </div>
        </body>
        </html>
      `);
        printWindow.document.close();
        printWindow.focus();
        printWindow.print();
      }

      toast({
        title: "Reporte PDF Generado",
        description: "El reporte se abrió en una nueva ventana para imprimir",
      });
    } catch (error) {
      console.error('Error generating PDF:', error);
      toast({
        title: "Error generando PDF",
        description: "No se pudo generar el reporte PDF. Inténtelo de nuevo.",
        variant: "destructive"
      });
    }
  };

  const generateExcelReport = async () => {
    try {
      // Use existing cash movements data
      const allMovements = cashMovements || [];

    const reportData = [
      ['REPORTE DE CAJA - ESTADO EN TIEMPO REAL'],
      ['Fecha:', new Date().toLocaleDateString('es-ES')],
      ['Hora:', new Date().toLocaleTimeString('es-ES')],
      ['Zona Horaria:', 'UTC-3 (Argentina)'],
      [''],
      ['RESUMEN FINANCIERO'],
      ['Concepto', 'Monto (USD)'],
      ['Saldo Total', realTimeState?.totalBalanceUsd || '0.00'],
      ['Ventas del Día', realTimeState?.dailySalesUsd || '0.00'],
      ['Gastos del Día', realTimeState?.dailyExpensesUsd || '0.00'],
      ['Deudas Activas', realTimeState?.totalActiveDebtsUsd || '0.00'],
      [''],
    ];

    // Add profit data if available
    if (vendorProfits && vendorProfits.length > 0) {
      const cajaPagosVendedores = vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0).toFixed(2);
      const cajaGananciasNetas = (vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0) - vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0)).toFixed(2);
      const cajaCostosVendidos = vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0).toFixed(2);
      const cajaCostosProducto = vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalCost || '0'), 0).toFixed(2);

      reportData.push(
        ['ANÁLISIS DE GANANCIAS Y COSTOS'],
        ['Concepto', 'Monto (USD)'],
        ['Caja Pagos Vendedores', cajaPagosVendedores],
        ['Caja Ganancias Netas', cajaGananciasNetas],
        ['Caja Costos Vendidos', cajaCostosVendidos],
        ['Caja Costos Producto', cajaCostosProducto],
        ['']
      );
    }

    // Add daily history if available
    if (dailyReports?.length > 0) {
      reportData.push(
        ['HISTORIAL DIARIO (ÚLTIMOS 7 DÍAS)'],
        ['Fecha', 'Balance Apertura', 'Ingresos', 'Gastos', 'Balance Cierre', 'Ganancia Neta', 'Movimientos']
      );

      dailyReports.slice(0, 7).forEach((report: any) => {
        reportData.push([
          new Date(report.reportDate).toLocaleDateString('es-ES'),
          report.openingBalance,
          report.totalIncome,
          report.totalExpenses,
          report.closingBalance,
          report.netProfit,
          report.totalMovements.toString()
        ]);
      });

      reportData.push(['']);
    }

    reportData.push(
      ['TODOS LOS MOVIMIENTOS DE CAJA'],
      ['Fecha', 'Tipo', 'Descripción', 'Método de Pago', 'Moneda', 'Monto Original', 'Monto (USD)', 'Cliente', 'Vendedor', 'Usuario']
    );

    allMovements?.forEach((movement: any) => {
      reportData.push([
        new Date(movement.createdAt).toLocaleDateString('es-ES'),
        movement.type,
        movement.description || '-',
        movement.subtype || '-',
        movement.currency,
        movement.amount,
        movement.amountUsd,
        movement.customerName || '-',
        movement.vendorName || '-',
        movement.userName || '-'
      ]);
    });

      const csvContent = reportData.map(row => 
        row.map(cell => `"${cell}"`).join(',')
      ).join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      link.setAttribute('download', `reporte-caja-${new Date().toISOString().split('T')[0]}.csv`);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({
        title: "Reporte Excel Generado",
        description: "El archivo CSV se ha descargado correctamente",
      });
    } catch (error) {
      console.error('Error generating Excel:', error);
      toast({
        title: "Error generando Excel",
        description: "No se pudo generar el reporte Excel. Inténtelo de nuevo.",
        variant: "destructive"
      });
    }
  };

  // Función para generar reporte del historial
  const generateHistoryReport = () => {
    try {
      if (!dailyReports || dailyReports.length === 0) {
        toast({
          title: "Sin datos",
          description: "No hay historial de cierres para descargar",
          variant: "destructive"
        });
        return;
      }

      const reportData = [
        ['HISTORIAL DE CIERRES DIARIOS'],
        ['Fecha', 'Balance Apertura', 'Ingresos', 'Gastos', 'Balance Cierre', 'Ganancia Neta', 'Movimientos'],
        ['']
      ];

      dailyReports.forEach((report: any) => {
        reportData.push([
          new Date(report.reportDate).toLocaleDateString('es-ES'),
          report.openingBalance,
          report.totalIncome,
          report.totalExpenses,
          report.closingBalance,
          report.netProfit,
          report.totalMovements.toString()
        ]);
      });

      const csvContent = reportData.map(row => 
        row.map(cell => `"${cell}"`).join(',')
      ).join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      link.setAttribute('download', `historial-cierres-${new Date().toISOString().split('T')[0]}.csv`);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({
        title: "Historial Descargado",
        description: "El archivo CSV se ha descargado correctamente",
      });
    } catch (error) {
      console.error('Error generating history report:', error);
      toast({
        title: "Error",
        description: "No se pudo generar el reporte del historial",
        variant: "destructive"
      });
    }
  };

  // Función para generar reporte de un día específico
  const generateSingleDayReport = (report: any) => {
    try {
      const reportData = [
        [`REPORTE DIARIO - ${new Date(report.reportDate).toLocaleDateString('es-ES')}`],
        [''],
        ['RESUMEN FINANCIERO'],
        ['Concepto', 'Monto (USD)'],
        ['Balance de Apertura', report.openingBalance],
        ['Total de Ingresos', report.totalIncome],
        ['Total de Gastos', report.totalExpenses],
        ['Balance de Cierre', report.closingBalance],
        ['Ganancia Neta', report.netProfit],
        ['Total de Movimientos', report.totalMovements],
        [''],
        [`Generado el: ${new Date().toLocaleString('es-ES')}`]
      ];

      const csvContent = reportData.map(row => 
        Array.isArray(row) ? row.map(cell => `"${cell}"`).join(',') : `"${row}"`
      ).join('\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      link.setAttribute('download', `reporte-diario-${new Date(report.reportDate).toISOString().split('T')[0]}.csv`);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({
        title: "Reporte Descargado",
        description: "El reporte diario se ha descargado correctamente",
      });
    } catch (error) {
      console.error('Error generating single day report:', error);
      toast({
        title: "Error",
        description: "No se pudo generar el reporte del día",
        variant: "destructive"
      });
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
        <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
        <div className="flex flex-1">
          <Sidebar />
          <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
            <div className="px-4 sm:px-6 lg:px-8 py-8">
              <div className="flex items-center justify-center h-64">
                <div className="text-gray-500 dark:text-gray-400">Cargando estado de caja...</div>
              </div>
            </div>
          </main>
        </div>
        <Footer />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <div className="flex flex-1">
        <Sidebar />
        <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
          <div className="px-4 sm:px-6 lg:px-8 py-8">
            {/* Header Section */}
            <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
              <div className="flex-1">
                <div className="flex items-center justify-between">
                  <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      Sistema de Cajas - Estado en Tiempo Real
                    </h1>
                    <p className="text-gray-600 dark:text-gray-400 mt-1">
                      Gestión financiera completa con apertura automática diaria • Zona horaria: UTC-3 (Argentina)
                    </p>
                  </div>
                  <div className="hidden lg:block">
                    <RealTimeClock />
                  </div>
                </div>
                {/* Mobile clock display */}
                <div className="lg:hidden mt-4 flex justify-center">
                  <RealTimeClock />
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                <Button 
                  variant="outline"
                  onClick={generatePDFReport}
                  className="bg-orange-50 hover:bg-orange-100 border-orange-200 text-orange-800"
                >
                  <Download className="mr-2 h-4 w-4" />
                  Generar PDF
                </Button>
                <Button 
                  variant="outline"
                  onClick={generateExcelReport}
                  className="bg-green-50 hover:bg-green-100 border-green-200 text-green-800"
                >
                  <FileText className="mr-2 h-4 w-4" />
                  Descargar Excel
                </Button>
                <Button 
                  variant="outline"
                  onClick={() => setShowHistoryModal(true)}
                  className="bg-blue-50 hover:bg-blue-100 border-blue-200 text-blue-800"
                >
                  <Calendar className="mr-2 h-4 w-4" />
                  Ver Historial
                </Button>
              </div>
            </div>

            {/* Sistema Automático de Caja */}
            <Card className="mt-6 border-l-4 border-l-blue-500 bg-gradient-to-r from-blue-50 to-transparent dark:from-blue-900/20">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <RefreshCw className="mr-2 h-5 w-5 text-blue-600" />
                    🤖 Sistema Automático de Apertura/Cierre
                  </div>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => {
                      // Forzar refresh de configuración antes de abrir
                      queryClient.invalidateQueries({ queryKey: ['/api/cash-schedule/config'] });
                      window.open('/cash-schedule', '_blank');
                    }}
                    className="text-blue-600 border-blue-300 hover:bg-blue-50"
                  >
                    <Settings className="mr-1 h-4 w-4" />
                    Configurar
                  </Button>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
                    <div className="flex items-center mb-2">
                      <Calendar className="mr-2 h-4 w-4 text-green-600" />
                      <span className="font-medium text-green-800 dark:text-green-300">Horario de Apertura</span>
                    </div>
                    <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                      {cashScheduleConfig && cashScheduleConfig.autoOpenEnabled ? (
                        `${cashScheduleConfig.openHour.toString().padStart(2, '0')}:${cashScheduleConfig.openMinute.toString().padStart(2, '0')}:00`
                      ) : "00:00:00"}
                    </div>
                    <p className="text-xs text-green-700 dark:text-green-300 mt-1">
                      Apertura automática diaria
                    </p>
                  </div>

                  <div className="bg-red-50 dark:bg-red-900/20 p-4 rounded-lg">
                    <div className="flex items-center mb-2">
                      <Calendar className="mr-2 h-4 w-4 text-red-600" />
                      <span className="font-medium text-red-800 dark:text-red-300">Horario de Cierre</span>
                    </div>
                    <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                      {cashScheduleConfig && cashScheduleConfig.autoCloseEnabled ? (
                        `${cashScheduleConfig.closeHour.toString().padStart(2, '0')}:${cashScheduleConfig.closeMinute.toString().padStart(2, '0')}:00`
                      ) : "23:59:00"}
                    </div>
                    <p className="text-xs text-red-700 dark:text-red-300 mt-1">
                      Cierre automático diario
                    </p>
                  </div>

                  <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
                    <div className="flex items-center mb-2">
                      <AlertCircle className="mr-2 h-4 w-4 text-blue-600" />
                      <span className="font-medium text-blue-800 dark:text-blue-300">Estado Actual</span>
                    </div>
                    <div className="text-lg font-bold text-blue-600 dark:text-blue-400">
                      {(() => {
                        const now = new Date();
                        const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));
                        const currentHour = argentinaTime.getHours();
                        const currentMinute = argentinaTime.getMinutes();
                        const currentMinutes = currentHour * 60 + currentMinute;
                        
                        let isWithinOpenHours = false;
                        
                        if (cashScheduleConfig && cashScheduleConfig.autoOpenEnabled && cashScheduleConfig.autoCloseEnabled) {
                          const openMinutes = cashScheduleConfig.openHour * 60 + cashScheduleConfig.openMinute;
                          const closeMinutes = cashScheduleConfig.closeHour * 60 + cashScheduleConfig.closeMinute;
                          
                          isWithinOpenHours = currentMinutes >= openMinutes && currentMinutes < closeMinutes;
                          
                          console.log(`🔍 [FRONTEND] Estado calculation:`, {
                            currentTime: `${currentHour.toString().padStart(2, '0')}:${currentMinute.toString().padStart(2, '0')}`,
                            currentMinutes,
                            openTime: `${cashScheduleConfig.openHour.toString().padStart(2, '0')}:${cashScheduleConfig.openMinute.toString().padStart(2, '0')}`,
                            openMinutes,
                            closeTime: `${cashScheduleConfig.closeHour.toString().padStart(2, '0')}:${cashScheduleConfig.closeMinute.toString().padStart(2, '0')}`,
                            closeMinutes,
                            isWithinOpenHours,
                            cashRegisterExists: !!cashRegister,
                            cashRegisterIsOpen: cashRegister?.isOpen,
                            autoOpenEnabled: cashScheduleConfig.autoOpenEnabled,
                            autoCloseEnabled: cashScheduleConfig.autoCloseEnabled
                          });
                        } else {
                          console.log(`🔍 [FRONTEND] No auto config, using cash register only:`, {
                            cashRegisterExists: !!cashRegister,
                            cashRegisterIsOpen: cashRegister?.isOpen
                          });
                        }
                        
                        // LÓGICA MEJORADA: Mostrar estado basado en configuración Y caja actual
                        if (cashScheduleConfig && cashScheduleConfig.autoOpenEnabled && cashScheduleConfig.autoCloseEnabled) {
                          // Si hay configuración automática, usar horarios + estado de caja
                          const shouldBeOpen = isWithinOpenHours;
                          const actuallyOpen = cashRegister?.isOpen || false;
                          
                          console.log(`🔍 [FRONTEND] Final state decision:`, {
                            shouldBeOpen,
                            actuallyOpen,
                            finalState: (shouldBeOpen && actuallyOpen) ? 'ABIERTA' : 'CERRADA'
                          });
                          
                          return (shouldBeOpen && actuallyOpen) ? '🟢 ABIERTA' : '🔴 CERRADA';
                        } else {
                          // Sin configuración automática, usar solo estado de caja
                          const state = cashRegister?.isOpen ? '🟢 ABIERTA' : '🔴 CERRADA';
                          console.log(`🔍 [FRONTEND] Manual mode state:`, state);
                          return state;
                        }
                      })()}
                    </div>
                    <p className="text-xs text-blue-700 dark:text-blue-300 mt-1">
                      {cashScheduleConfig ? (
                        (() => {
                          if (cashScheduleConfig.autoCloseEnabled && cashSchedule?.status === 'open') {
                            return (
                              <>
                                Cerrará a las {cashScheduleConfig.closeHour.toString().padStart(2, '0')}:{cashScheduleConfig.closeMinute.toString().padStart(2, '0')}:00 (Argentina)<br/>
                                <span className="font-mono text-sm text-blue-600 dark:text-blue-400">
                                  ⏱️ Tiempo restante: {timeToClose}
                                </span>
                              </>
                            );
                          } else if (cashScheduleConfig.autoOpenEnabled) {
                            return `Abrirá a las ${cashScheduleConfig.openHour.toString().padStart(2, '0')}:${cashScheduleConfig.openMinute.toString().padStart(2, '0')}:00 (Argentina)`;
                          }
                          return "Sin configuración automática";
                        })()
                      ) : (
                        cashSchedule?.status === 'open' ? 
                          `Cerrará a las 23:59:00 (Argentina)` :
                          `Abrirá a las 00:00:00 (Argentina)`
                      )}
                    </p>
                  </div>
                </div>

                {autoNotification && (
                  <div className="mt-4 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
                    <div className="flex items-center">
                      <AlertCircle className="mr-2 h-4 w-4 text-yellow-600" />
                      <span className="text-yellow-800 dark:text-yellow-300 text-sm">
                        {autoNotification}
                      </span>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Historial Diario */}
            {dailyReports && dailyReports.length > 0 && (
              <Card className="mt-6 border-l-4 border-l-purple-500 bg-gradient-to-r from-purple-50 to-transparent dark:from-purple-900/20">
                <CardHeader>
                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                    <CardTitle className="flex items-center">
                      <Calendar className="mr-2 h-5 w-5 text-purple-600" />
                      📊 Historial de Cierres Diarios
                    </CardTitle>

                    {/* Filtros y botón de descarga */}
                    <div className="flex flex-col sm:flex-row gap-2">
                      <div className="flex space-x-1">
                        <button 
                          className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                            dailyReportFilter === "all" 
                              ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                              : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700 border border-gray-300 dark:border-gray-600"
                          }`}
                          onClick={() => setDailyReportFilter("all")}
                        >
                          Todo
                        </button>
                        <button 
                          className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                            dailyReportFilter === "week" 
                              ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                              : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700 border border-gray-300 dark:border-gray-600"
                          }`}
                          onClick={() => setDailyReportFilter("week")}
                        >
                          Semana
                        </button>
                        <button 
                          className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                            dailyReportFilter === "month" 
                              ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                              : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700 border border-gray-300 dark:border-gray-600"
                          }`}
                          onClick={() => setDailyReportFilter("month")}
                        >
                          Mes
                        </button>
                        <button 
                          className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                            dailyReportFilter === "year" 
                              ? "bg-blue-500 text-white hover:bg-blue-600" 
                              : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700 border border-gray-300 dark:border-gray-600"
                          }`}
                          onClick={() => setDailyReportFilter("year")}
                        >
                          Año
                        </button>
                      </div>

                      <Button 
                        size="sm"
                        onClick={() => generateHistoryReport()}
                        className="bg-purple-600 hover:bg-purple-700 text-white"
                      >
                        <Download className="mr-1 h-4 w-4" />
                        Descargar Informe
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b bg-gray-50 dark:bg-gray-800">
                          <th className="text-left p-2">Fecha</th>
                          <th className="text-right p-2">Balance Apertura</th>
                          <th className="text-right p-2">Ingresos</th>
                          <th className="text-right p-2">Gastos</th>
                          <th className="text-right p-2">Balance Cierre</th>
                          <th className="text-right p-2">Ganancia Neta</th>
                          <th className="text-center p-2">Movimientos</th>
                          <th className="text-center p-2">Acciones</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dailyReports.slice(0, 7).map((report: any) => (
                          <tr key={report.id} className="border-b hover:bg-gray-50 dark:hover:bg-gray-800">
                            <td className="p-2 font-medium">
                              {new Date(report.reportDate).toLocaleDateString('es-ES')}
                            </td>
                            <td className="p-2 text-right">${report.openingBalance}</td>
                            <td className="p-2 text-right text-green-600 dark:text-green-400">
                              ${report.totalIncome}
                            </td>
                            <td className="p-2 text-right text-red-600 dark:text-red-400">
                              ${report.totalExpenses}
                            </td>
                            <td className="p-2 text-right font-bold">${report.closingBalance}</td>
                            <td className={`p-2 text-right font-bold ${
                              parseFloat(report.netProfit) >= 0 
                                ? 'text-green-600 dark:text-green-400' 
                                : 'text-red-600 dark:text-red-400'
                            }`}>
                              ${report.netProfit}
                            </td>
                            <td className="p-2 text-center">
                              <span className="bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded text-xs">
                                {report.totalMovements}
                              </span>
                            </td>
                            <td className="p-2 text-center">
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => generateSingleDayReport(report)}
                                className="text-xs px-2 py-1"
                              >
                                <Download className="mr-1 h-3 w-3" />
                                PDF
                              </Button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Real-time Dashboard */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mt-8">
              {/* Saldo Total */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center text-sm font-medium">
                    <Wallet className="mr-2 h-4 w-4" />
                    Saldo Total (USD)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                    ${realTimeState?.totalBalanceUsd || "0.00"}
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                    Estado en tiempo real
                  </p>
                </CardContent>
              </Card>

              {/* Ventas del Día */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center text-sm font-medium">
                    <TrendingUp className="mr-2 h-4 w-4" />
                    Ventas del Día
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                    ${realTimeState?.dailySalesUsd || "0.00"}
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                    Ingresos por ventas
                  </p>
                </CardContent>
              </Card>

              {/* Gastos del Día */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center text-sm font-medium">
                    <TrendingDown className="mr-2 h-4 w-4" />
                    Gastos del Día
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-red-600 dark:text-red-400">
                    ${realTimeState?.dailyExpensesUsd || "0.00"}
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                    Egresos registrados
                  </p>
                </CardContent>
              </Card>

              {/* Deudas Vigentes */}
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center text-sm font-medium">
                    <AlertCircle className="mr-2 h-4 w-4" />
                    Deudas Vigentes
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                    ${realTimeState?.totalActiveDebtsUsd || "0.00"}
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                    Pendientes de cobro
                  </p>
                </CardContent>
              </Card>
            </div>

            {/* Desglose por Métodos de Pago - MEJORADO */}
            <Card className="mt-8">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <CreditCard className="mr-2 h-5 w-5" />
                    Desglose por Métodos de Pago
                  </div>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => {
                      // Refrescar todos los datos de caja
                      queryClient.invalidateQueries({ queryKey: ['/api/cash-movements'] });
                      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/real-time-state'] });
                      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/current'] });
                      queryClient.invalidateQueries({ queryKey: ['/api/expenses'] });
                      queryClient.invalidateQueries({ queryKey: ['/api/vendor-performance'] });

                      toast({
                        title: "Datos actualizados",
                        description: "Se han refrescado todos los datos de caja",
                      });
                    }}
                  >
                    <RefreshCw className="mr-1 h-4 w-4" />
                    Actualizar
                  </Button>
                </CardTitle>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  📊 Análisis detallado de ingresos y gastos por forma de pago - Registro Pagos/Gastos
                </p>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                  {(() => {
                    // Usar datos del backend si están disponibles
                    if (realTimeState?.salesByPaymentMethod) {
                      const salesData = realTimeState.salesByPaymentMethod;

                      // Crear array de métodos de pago con datos
                      const methodsArray = [
                        { key: 'efectivo_ars', label: 'Efectivo ARS', icon: '💵', color: 'green', value: salesData.efectivo_ars },
                        { key: 'efectivo_usd', label: 'Efectivo USD', icon: '💸', color: 'blue', value: salesData.efectivo_usd },
                        { key: 'transferencia_ars', label: 'Transferencia ARS', icon: '🏦', color: 'purple', value: salesData.transferencia_ars },
                        { key: 'transferencia_usd', label: 'Transferencia USD', icon: '🔗', color: 'indigo', value: salesData.transferencia_usd },
                        { key: 'transferencia_usdt', label: 'Transferencia USDT', icon: '₿', color: 'yellow', value: salesData.transferencia_usdt },
                        { key: 'financiera_ars', label: 'Financiera ARS', icon: '💳', color: 'red', value: salesData.financiera_ars },
                        { key: 'financiera_usd', label: 'Financiera USD', icon: '💰', color: 'orange', value: salesData.financiera_usd }
                      ];

                      // Si no hay ventas, mostrar mensaje informativo
                      if (methodsArray.every(method => method.value === 0)) {
                        return (
                          <div className="col-span-full text-center py-8 text-gray-500 dark:text-gray-400">
                            <div className="text-4xl mb-4">📊</div>
                            <h3 className="text-lg font-medium mb-2">No hay ventas registradas hoy</h3>
                            <p className="text-sm">Las ventas aparecerán aquí automáticamente cuando se procesen pedidos</p>
                            <div className="mt-4 text-xs bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg">
                              💡 <strong>Tip:</strong> Ve a "Pedidos" → "Crear Pedido" para registrar una venta y verla reflejada aquí
                            </div>
                          </div>
                        );
                      }

                      // Mostrar solo métodos con valores
                      const methodsWithData = methodsArray.filter(method => method.value > 0);

                      return methodsWithData.map((method) => {
                        // Formatear visualización correcta por método de pago
                        let displayValue = method.value;
                        let displayCurrency = '';

                        if (method.key === 'financiera_usd') {
                          // financiera_usd ya está en USD del backend, NO dividir
                          displayValue = method.value;
                          displayCurrency = ' USD';
                        } else if (method.key.includes('_ars')) {
                          displayCurrency = ' ARS';
                        } else if (method.key.includes('_usd')) {
                          displayCurrency = ' USD';
                        } else if (method.key.includes('_usdt')) {
                          displayCurrency = ' USDT';
                        }

                        return (
                          <Card key={method.key} className={`border-l-4 border-l-${method.color}-500 bg-gradient-to-r from-${method.color}-50 to-transparent dark:from-${method.color}-900/20`}>
                            <CardHeader className="pb-2">
                              <CardTitle className="text-sm font-medium text-gray-800 dark:text-gray-200 flex items-center">
                                <span className="mr-2">{method.icon}</span>
                                {method.label}
                              </CardTitle>
                            </CardHeader>
                            <CardContent>
                              <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                {method.key === 'financiera_usd' ? '$' : ''}{displayValue.toFixed(2)}{displayCurrency}
                              </div>
                              <div className="text-xs text-gray-500 dark:text-gray-400">
                                {method.key === 'financiera_usd' ? 'Equivalente USD' : 'Ventas de hoy'}
                              </div>
                            </CardContent>
                          </Card>
                        );
                      });
                    }

                    // Fallback: calcular desde movimientos si no hay datos del backend
                    const paymentBreakdown: Record<string, any> = {};

                    if (cashMovements) {
                      cashMovements.forEach((movement: CashMovement) => {
                        const method = movement.subtype || 'otros';
                        let currency = movement.currency || 'USD';
                        let amount = parseFloat(movement.amount) || 0;
                        const amountUsd = parseFloat(movement.amountUsd) || 0;
                        const isIncome = movement.type === 'ingreso' || movement.type === 'venta' || movement.type === 'pago_deuda';

                        // Debug logging for financiera methods
                        if (method === 'financiera_ars' || method === 'financiera_usd') {
                          console.log(`🔍 Financiera movement debug:`, {
                            id: movement.id,
                            method,
                            originalAmount: movement.amount,
                            originalCurrency: movement.currency,
                            amountUsd: movement.amountUsd,
                            exchangeRate: movement.exchangeRate,
                            type: movement.type,
                            description: movement.description,
                            isIncome
                          });
                        }

                        // Special handling for financiera methods - display in original currency
                        if (method === 'financiera_usd') {
                          // Financiera USD: show USD amount (no conversion needed)
                          currency = 'USD';
                          amount = amountUsd; // Show the USD amount directly
                        } else if (method === 'financiera_ars') {
                          // Financiera ARS: show ARS amount (conversion to USD for totals)
                          currency = 'ARS';
                          // Use the original amount from movement
                          amount = parseFloat(movement.amount) || 0;
                        }

                        if (!paymentBreakdown[method]) {
                          paymentBreakdown[method] = {
                            ARS_ingresos: 0,
                            USD_ingresos: 0,
                            USDT_ingresos: 0,
                            ARS_gastos: 0,
                            USD_gastos: 0,
                            USDT_gastos: 0,
                            total_ingresos_usd: 0,
                            total_gastos_usd: 0,
                            balance_usd: 0,
                            transacciones: 0
                          };
                        }

                        if (isIncome) {
                          paymentBreakdown[method][`${currency}_ingresos`] += amount;
                          paymentBreakdown[method].total_ingresos_usd += amountUsd;
                        } else {
                          paymentBreakdown[method][`${currency}_gastos`] += amount;
                          paymentBreakdown[method].total_gastos_usd += Math.abs(amountUsd);
                        }

                        paymentBreakdown[method].balance_usd = paymentBreakdown[method].total_ingresos_usd - paymentBreakdown[method].total_gastos_usd;
                        paymentBreakdown[method].transacciones += 1;
                      });
                    }

                    const methodLabels: Record<string, { label: string; icon: string; color: string }> = {
                      'efectivo_ars': { label: 'Efectivo ARS', icon: '💵', color: 'green' },
                      'efectivo_usd': { label: 'Efectivo USD', icon: '💶', color: 'blue' },
                      'transferencia_ars': { label: 'Transferencia ARS', icon: '🏦', color: 'indigo' },
                      'transferencia_usd': { label: 'Transferencia USD', icon: '🌐', color: 'purple' },
                      'transferencia_usdt': { label: 'Transferencia USDT', icon: '₿', color: 'yellow' },
                      'financiera_usd': { label: 'Financiera USD', icon: '🏧', color: 'orange' },
                      'financiera_ars': { label: 'Financiera ARS', icon: '💳', color: 'red' }
                    };

                    return Object.keys(paymentBreakdown).map((method) => {
                      const data = paymentBreakdown[method];
                      const methodInfo = methodLabels[method] || { label: method.charAt(0).toUpperCase() + method.slice(1), icon: '💰', color: 'gray' };

                      return (
                        <Card key={method} className={`border-l-4 border-l-${methodInfo.color}-500 bg-gradient-to-r from-${methodInfo.color}-50 to-transparent dark:from-${methodInfo.color}-900/20`}>
                          <CardHeader className="pb-2">
                            <CardTitle className="text-sm font-medium text-gray-800 dark:text-gray-200 flex items-center">
                              <span className="mr-2">{methodInfo.icon}</span>
                              {methodInfo.label}
                            </CardTitle>
                            <div className="text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded-full inline-block">
                              {data.transacciones} transacciones
                            </div>
                          </CardHeader>
                          <CardContent className="space-y-2">
                            {/* INGRESOS */}
                            <div className="bg-green-50 dark:bg-green-900/20 p-2 rounded">
                              <div className="text-xs font-medium text-green-700 dark:text-green-300 mb-1">
                                📈 INGRESOS
                              </div>
                              {data.ARS_ingresos > 0 && (
                                <div className="flex justify-between items-center">
                                  <span className="text-xs text-gray-600 dark:text-gray-400">ARS:</span>
                                  <span className="text-sm font-bold text-green-600 dark:text-green-400">
                                    ${data.ARS_ingresos.toLocaleString()}
                                  </span>
                                </div>
                              )}
                              {data.USD_ingresos > 0 && (
                                <div className="flex justify-between items-center">
                                  <span className="text-xs text-gray-600 dark:text-gray-400">USD:</span>
                                  <span className="text-sm font-bold text-green-600 dark:text-green-400">
                                    ${data.USD_ingresos.toFixed(2)}
                                  </span>
                                </div>
                              )}
                              {data.USDT_ingresos > 0 && (
                                <div className="flex justify-between items-center">
                                  <span className="text-xs text-gray-600 dark:text-gray-400">USDT:</span>
                                  <span className="text-sm font-bold text-green-600 dark:text-green-400">
                                    ${data.USDT_ingresos.toFixed(2)}
                                  </span>
                                </div>
                              )}
                              <div className="border-t pt-1 mt-1">
                                <div className="flex justify-between items-center">
                                  <span className="text-xs font-medium">Total USD:</span>
                                  <span className="text-sm font-bold text-green-600 dark:text-green-400">
                                    ${data.total_ingresos_usd.toFixed(2)}
                                  </span>
                                </div>
                              </div>
                            </div>

                            {/* GASTOS */}
                            {(data.total_gastos_usd > 0) && (
                              <div className="bg-red-50 dark:bg-red-900/20 p-2 rounded">
                                <div className="text-xs font-medium text-red-700 dark:text-red-300 mb-1">
                                  📉 GASTOS
                                </div>
                                {data.ARS_gastos > 0 && (
                                  <div className="flex justify-between items-center">
                                    <span className="text-xs text-gray-600 dark:text-gray-400">ARS:</span>
                                    <span className="text-sm font-bold text-red-600 dark:text-red-400">
                                      ${data.ARS_gastos.toLocaleString()}
                                    </span>
                                  </div>
                                )}
                                {data.USD_gastos > 0 && (
                                  <div className="flex justify-between items-center">
                                    <span className="text-xs text-gray-600 dark:text-gray-400">USD:</span>
                                    <span className="text-sm font-bold text-red-600 dark:text-red-400">
                                      ${data.USD_gastos.toFixed(2)}
                                    </span>
                                  </div>
                                )}
                                {data.USDT_gastos > 0 && (
                                  <div className="flex justify-between items-center">
                                    <span className="text-xs text-gray-600 dark:text-gray-400">USDT:</span>
                                    <span className="text-sm font-bold text-red-600 dark:text-red-400">
                                      ${data.USDT_gastos.toFixed(2)}
                                    </span>
                                  </div>
                                )}
                                <div className="border-t pt-1 mt-1">
                                  <div className="flex justify-between items-center">
                                    <span className="text-xs font-medium">Total USD:</span>
                                    <span className="text-sm font-bold text-red-600 dark:text-red-400">
                                      ${data.total_gastos_usd.toFixed(2)}
                                    </span>
                                  </div>
                                </div>
                              </div>
                            )}

                            {/* BALANCE NETO */}
                            <div className="border-t pt-2 border-gray-200 dark:border-gray-700 bg-blue-50 dark:bg-blue-900/20 p-2 rounded">
                              <div className="flex justify-between items-center">
                                <span className="text-xs font-medium text-gray-700 dark:text-gray-300">⚖️ Balance Neto:</span>
                                <span className={`text-lg font-bold ${data.balance_usd >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                                  ${data.balance_usd.toFixed(2)}
                                </span>
                              </div>
                            </div>

                            {/* Métrica gráfica - barra de progreso */}
                            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2 mt-2">
                              <div 
                                className={`h-2 rounded-full transition-all duration-500 ${data.balance_usd >= 0 ? 'bg-gradient-to-r from-green-500 to-blue-500' : 'bg-gradient-to-r from-red-500 to-orange-500'}`}
                                style={{ 
                                  width: `${Math.min(100, Math.abs(data.total_ingresos_usd / (parseFloat(realTimeState?.dailySalesUsd) || 1)) * 100)}%` 
                                }}
                              ></div>
                            </div>
                            <div className="text-xs text-center text-gray-500 dark:text-gray-400">
                              {((data.total_ingresos_usd / (parseFloat(realTimeState?.dailySalesUsd) || 1)) * 100).toFixed(1)}% del total
                            </div>
                          </CardContent>
                        </Card>
                      );
                    });
                  })()}

                  {/* Card de resumen total MEJORADO */}
                  <Card className="border-l-4 border-l-yellow-500 bg-gradient-to-r from-yellow-50 to-transparent dark:from-yellow-900/20">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm font-medium text-gray-800 dark:text-gray-200">
                        <Calculator className="mr-1 h-4 w-4 inline" />
                        📋 Resumen General
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                      <div className="bg-green-100 dark:bg-green-900/20 p-2 rounded">
                        <div className="flex justify-between items-center">
                          <span className="text-xs text-gray-600 dark:text-gray-400">📈 Total Ventas:</span>
                          <span className="text-lg font-bold text-green-600 dark:text-green-400">
                            ${realTimeState?.dailySalesUsd || "0.00"}
                          </span>
                        </div>
                      </div>
                      <div className="bg-red-100 dark:bg-red-900/20 p-2 rounded">
                        <div className="flex justify-between items-center">
                          <span className="text-xs text-gray-600 dark:text-gray-400">📉 Total Gastos:</span>
                          <span className="text-sm font-bold text-red-600 dark:text-red-400">
                            ${realTimeState?.dailyExpensesUsd || "0.00"}
                          </span>
                        </div>
                      </div>
                      <div className="border-t pt-2 border-gray-200 dark:border-gray-700 bg-blue-100 dark:bg-blue-900/20 p-2 rounded">
                        <div className="flex justify-between items-center">
                          <span className="text-xs font-medium text-gray-700 dark:text-gray-300">⚖️ Balance Final:</span>
                          <span className="text-xl font-bold text-blue-600 dark:text-blue-400">
                            ${((parseFloat(realTimeState?.dailySalesUsd) || 0) - (parseFloat(realTimeState?.dailyExpensesUsd) || 0)).toFixed(2)}
                          </span>
                        </div>
                      </div>
                      <div className="bg-orange-100 dark:bg-orange-900/20 p-2 rounded">
                        <div className="flex justify-between items-center">
                          <span className="text-xs text-gray-600 dark:text-gray-400">⏳ Deudas Pendientes:</span>
                          <span className="text-sm font-bold text-orange-600 dark:text-orange-400">
                            ${realTimeState?.totalActiveDebtsUsd || "0.00"}
                          </span>
                        </div>
                      </div>
                      <div className="text-xs text-center text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 p-2 rounded">
                        🕒 Actualizado: {new Date().toLocaleTimeString('es-ES')}
                      </div>
                    </CardContent>
                  </Card>

                  {/* Análisis de Ganancias y Costos */}
                  <div className="col-span-full">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                      <span className="w-4 h-4 bg-indigo-500 rounded-full mr-2"></span>
                      📊 Análisis de Ganancias y Costos
                    </h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                      <Card className="border-pink-200 dark:border-pink-800 relative">
                        <CardContent className="p-4 text-center">
                          <div className="flex items-center justify-between mb-2">
                            <h4 className="font-medium text-pink-700 dark:text-pink-300">Caja Pagos Vendedores</h4>
                            <div className="w-2 h-2 bg-pink-500 rounded-full animate-pulse" title="Calculado desde órdenes reales"></div>
                          </div>
                          <p className="text-2xl font-bold text-pink-600">
                            ${vendorProfits && vendorProfits.length > 0 
                              ? vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0).toFixed(2)
                              : '0.00'
                            }
                          </p>
                          <p className="text-sm text-gray-500">Comisiones vendedores (%)</p>
                        </CardContent>
                      </Card>

                      <Card className="border-emerald-200 dark:border-emerald-800 relative">
                        <CardContent className="p-4 text-center">
                          <div className="flex items-center justify-between mb-2">
                            <h4 className="font-medium text-emerald-700 dark:text-emerald-300">Caja Ganancias Netas</h4>
                            <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" title="Calculado desde órdenes reales"></div>
                          </div>
                          <p className="text-2xl font-bold text-emerald-600">
                            ${vendorProfits && vendorProfits.length > 0 
                              ? (vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0) - vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.commission || '0'), 0)).toFixed(2)
                              : '0.00'
                            }
                          </p>
                          <p className="text-sm text-gray-500">Ganancia del admin por la venta</p>
                        </CardContent>
                      </Card>

                      <Card className="border-rose-200 dark:border-rose-800 relative">
                        <CardContent className="p-4 text-center">
                          <div className="flex items-center justify-between mb-2">
                            <h4 className="font-medium text-rose-700 dark:text-rose-300">Caja Costos Vendidos</h4>
                            <div className="w-2 h-2 bg-rose-500 rounded-full animate-pulse" title="Calculado desde órdenes reales"></div>
                          </div>
                          <p className="text-2xl font-bold text-rose-600">
                            ${vendorProfits && vendorProfits.length > 0 
                              ? vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0).toFixed(2)
                              : '0.00'
                            }
                          </p>
                          <p className="text-sm text-gray-500">Costo total de productos vendidos</p>
                        </CardContent>
                      </Card>

                      <Card className="border-slate-200 dark:border-slate-800 relative">
                        <CardContent className="p-4 text-center">
                          <div className="flex items-center justify-between mb-2">
                            <h4 className="font-medium text-slate-700 dark:text-slate-300">Caja Costos Producto</h4>
                            <div className="w-2 h-2 bg-slate-500 rounded-full animate-pulse" title="Calculado desde órdenes reales"></div>
                          </div>
                          <p className="text-2xl font-bold text-slate-600">
                            ${vendorProfits && vendorProfits.length > 0 
                              ? vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalCost || '0'), 0).toFixed(2)
                              : '0.00'
                            }
                          </p>
                          <p className="text-sm text-gray-500">Costo base de productos</p>
                        </CardContent>
                      </Card>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Reporte Detallado de Pagos/Gastos */}
            <Card className="mt-8">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <FileText className="mr-2 h-5 w-5" />
                    Reporte Detallado de Pagos/Gastos
                  </div>
                  <Button 
                    variant="outline" 
                    size="sm"
                    onClick={() => {
                      queryClient.invalidateQueries({ queryKey: ['/api/cash-movements'] });
                      queryClient.refetchQueries({ queryKey: ['/api/cash-register/real-time-state'] });
                    }}
                  >
                    <RefreshCw className="mr-1 h-4 w-4" />
                    Actualizar
                  </Button>
                </CardTitle>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  📋 Registro completo de todos los movimientos con descripción específica
                </p>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {(() => {
                    // Agrupar movimientos por tipo y mostrar detalles específicos
                    const groupedMovements: Record<string, CashMovement[]> = {};

                    if (cashMovements) {
                      cashMovements.forEach((movement: CashMovement) => {
                        const groupKey = movement.type;
                        if (!groupedMovements[groupKey]) {
                          groupedMovements[groupKey] = [];
                        }
                        groupedMovements[groupKey].push(movement);
                      });
                    }

                    const typeLabels: Record<string, { label: string; icon: string; bgColor: string; textColor: string }> = {
                      'venta': { label: '💰 VENTAS', icon: '📈', bgColor: 'bg-green-100 dark:bg-green-900/20', textColor: 'text-green-800 dark:text-green-200' },
                      'ingreso': { label: '💵 INGRESOS', icon: '📥', bgColor: 'bg-blue-100 dark:bg-blue-900/20', textColor: 'text-blue-800 dark:text-blue-200' },
                      'pago_deuda': { label: '💳 PAGOS DE DEUDA', icon: '💸', bgColor: 'bg-indigo-100 dark:bg-indigo-900/20', textColor: 'text-indigo-800 dark:text-indigo-200' },
                      'gasto': { label: '📉 GASTOS', icon: '💸', bgColor: 'bg-red-100 dark:bg-red-900/20', textColor: 'text-red-800 dark:text-red-200' },
                      'adelanto': { label: '👨‍💼 ADELANTOS', icon: '🏦', bgColor: 'bg-orange-100 dark:bg-orange-900/20', textColor: 'text-orange-800 dark:text-orange-200' }
                    };

                    return Object.keys(groupedMovements).map((type) => {
                      const movements = groupedMovements[type];
                      const typeInfo = typeLabels[type] || { label: type.toUpperCase(), icon: '📋', bgColor: 'bg-gray-100 dark:bg-gray-900/20', textColor: 'text-gray-800 dark:text-gray-200' };
                      const totalUsd = movements.reduce((sum, mov) => sum + parseFloat(mov.amountUsd), 0);

                      return (
                        <div key={type} className={`${typeInfo.bgColor} p-4 rounded-lg border-l-4 border-l-blue-500`}>
                          <div className="flex items-center justify-between mb-3">
                            <h3 className={`font-bold text-lg ${typeInfo.textColor} flex items-center`}>
                              <span className="mr-2">{typeInfo.icon}</span>
                              {typeInfo.label}
                            </h3>
                            <div className="flex items-center space-x-4">
                              <span className="text-sm bg-white dark:bg-gray-800 px-2 py-1 rounded-full">
                                {movements.length} transacciones
                              </span>
                              <span className={`text-lg font-bold ${typeInfo.textColor}`}>
                                ${Math.abs(totalUsd).toFixed(2)} USD
                              </span>
                            </div>
                          </div>

                          <div className="grid gap-2 max-h-60 overflow-y-auto">
                            {movements.map((movement) => (
                              <div key={movement.id} className="bg-white dark:bg-gray-800 p-3 rounded border-l-2 border-l-gray-300 dark:border-l-gray-600">
                                <div className="flex justify-between items-start">
                                  <div className="flex-1">
                                    <div className="font-medium text-gray-900 dark:text-gray-100">
                                      {movement.description || 'Sin descripción'}
                                    </div>
                                    <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                                      <div className="flex items-center space-x-4">
                                        <span>📅 {new Date(movement.createdAt).toLocaleString()}</span>
                                        <span>💳 {movement.subtype || 'Método no especificado'}</span>
                                        <span>👤 {movement.userName}</span>
                                      </div>
                                      {movement.customerName && (
                                        <div className="mt-1">
                                          <span>👥 Cliente: {movement.customerName}</span>
                                        </div>
                                      )}
                                      {movement.vendorName && (
                                        <div className="mt-1">
                                          <span>🏪 Vendedor: {movement.vendorName}</span>
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                  <div className="text-right ml-4">
                                    <div className="text-sm text-gray-500 dark:text-gray-400">
                                      {movement.currency} {parseFloat(movement.amount).toFixed(2)}
                                    </div>
                                    <div className={`text-lg font-bold ${parseFloat(movement.amountUsd) >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                                      ${Math.abs(parseFloat(movement.amountUsd)).toFixed(2)}
                                    </div>
                                    {movement.exchangeRate && parseFloat(movement.exchangeRate) !== 1 && (
                                      <div className="text-xs text-gray-400">
                                        TC: {parseFloat(movement.exchangeRate).toFixed(4)}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      );
                    });
                  })()}

                  {/* Resumen Totales por Categoría */}
                  <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg border-2 border-dashed border-gray-300 dark:border-gray-600">
                    <h3 className="font-bold text-lg text-gray-800 dark:text-gray-200 mb-3 flex items-center">
                      <Calculator className="mr-2" />
                      📊 Resumen por Categorías
                    </h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      {(() => {
                        const summary: Record<string, number> = {};

                        if (cashMovements) {
                          cashMovements.forEach((movement: CashMovement) => {
                            const type = movement.type;
                            if (!summary[type]) summary[type] = 0;
                            summary[type] += parseFloat(movement.amountUsd);
                          });
                        }

                        return Object.keys(summary).map((type) => (
                          <div key={type} className="text-center p-3 bg-white dark:bg-gray-700 rounded">
                            <div className="text-sm text-gray-600 dark:text-gray-400 capitalize">
                              {type.replace('_', ' ')}
                            </div>
                            <div className={`text-lg font-bold ${summary[type] >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                              ${Math.abs(summary[type]).toFixed(2)}
                            </div>
                          </div>
                        ));
                      })()}
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Filtros y Búsqueda */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <Filter className="mr-2 h-5 w-5" />
                  Filtros de Búsqueda
                </CardTitle>
                <div className="text-sm text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg">
                  📊 <strong>Historial completo:</strong> Ahora se muestran todos los movimientos históricos de tu cliente. Usa los filtros de fecha para ver períodos específicos.
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-7 gap-4">
                  <div>
                    <Label className="text-sm font-medium mb-2 block">Tipo de Movimiento</Label>
                    <Select value={filterType} onValueChange={setFilterType}>
                      <SelectTrigger>
                        <SelectValue placeholder="Seleccionar tipo" />
                      </SelectTrigger>
                      <SelectContent>
                        {movementTypes.map((type) => (
                          <SelectItem key={type.value} value={type.value}>
                            {type.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Moneda</Label>
                    <Select value={filterCurrency} onValueChange={setFilterCurrency}>
                      <SelectTrigger>
                        <SelectValue placeholder="Todas las monedas" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Todas las monedas</SelectItem>
                        <SelectItem value="USD">USD</SelectItem>
                        <SelectItem value="ARS">ARS</SelectItem>
                        <SelectItem value="USDT">USDT</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Método de Pago</Label>
                    <Select value={filterPaymentMethod} onValueChange={setFilterPaymentMethod}>
                      <SelectTrigger>
                        <SelectValue placeholder="Todos los métodos" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Todos los métodos</SelectItem>
                        {paymentMethods.map((method) => (
                          <SelectItem key={method.value} value={method.value}>
                            {method.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Fecha Desde</Label>
                    <Input
                      type="date"
                      value={filterDateFrom}
                      onChange={(e) => setFilterDateFrom(e.target.value)}
                    />
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Fecha Hasta</Label>
                    <Input
                      type="date"
                      value={filterDateTo}
                      onChange={(e) => setFilterDateTo(e.target.value)}
                    />
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Cliente</Label>
                    <Input
                      placeholder="Filtrar por cliente..."
                      value={filterCustomer}
                      onChange={(e) => setFilterCustomer(e.target.value)}
                    />
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Vendedor</Label>
                    <Input
                      placeholder="Filtrar por vendedor..."
                      value={filterVendor}
                      onChange={(e) => setFilterVendor(e.target.value)}
                    />
                  </div>

                  <div>
                    <Label className="text-sm font-medium mb-2 block">Búsqueda General</Label>
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                      <Input
                        placeholder="Buscar..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-10"
                      />
                    </div>
                  </div>
                </div>

                {/* Botones de acción rápida */}
                <div className="mt-4 flex flex-wrap gap-2 justify-between">
                  <div className="flex flex-wrap gap-2">
                    <Button 
                      variant="outline" 
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const todayStr = today.toISOString().split('T')[0];
                        setFilterDateFrom(todayStr);
                        setFilterDateTo(todayStr);
                      }}
                    >
                      <Calendar className="mr-1 h-4 w-4" />
                      Hoy
                    </Button>
                    <Button 
                      variant="outline" 
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const weekAgo = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
                        setFilterDateFrom(weekAgo.toISOString().split('T')[0]);
                        setFilterDateTo(today.toISOString().split('T')[0]);
                      }}
                    >
                      Última semana
                    </Button>
                    <Button 
                      variant="outline" 
                      size="sm"
                      onClick={() => {
                        const today = new Date();
                        const monthAgo = new Date(today.getFullYear(), today.getMonth() - 1, today.getDate());
                        setFilterDateFrom(monthAgo.toISOString().split('T')[0]);
                        setFilterDateTo(today.toISOString().split('T')[0]);
                      }}
                    >
                      Último mes
                    </Button>
                  </div>
                  <Button 
                    variant="outline" 
                    onClick={() => {
                      setFilterType("all");
                      setFilterDateFrom("");
                      setFilterDateTo("");
                      setFilterPaymentMethod("all");
                      setFilterCurrency("all");
                      setFilterCustomer("");
                      setFilterVendor("");
                      setSearchQuery("");
                    }}
                    className="text-sm"
                  >
                    <RefreshCw className="mr-2 h-4 w-4" />
                    Ver Todo (Sin Filtros)
                  </Button>
                </div>
              </CardContent>
            </Card>

            {/* Movimientos de Caja */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <FileText className="mr-2 h-5 w-5" />
                  Historial Completo de Movimientos de Caja
                </CardTitle>
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  Se muestran todos los movimientos desde el primero registrado, ordenados por fecha (más reciente primero)
                </div>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse">
                    <thead>
                      <tr className="border-b border-gray-200 dark:border-gray-700">
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Fecha/Hora</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Tipo</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Descripción</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Método</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Monto</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">USD</th>
                        <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Usuario</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cashMovements?.map((movement: CashMovement) => (
                        <tr key={movement.id} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800">
                          <td className="p-3 text-sm text-gray-900 dark:text-gray-100">
                            {new Date(movement.createdAt).toLocaleString()}
                          </td>
                          <td className="p-3">
                            <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${
                              movement.type === 'ingreso' || movement.type === 'venta'
                                ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                                : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                            }`}>
                              {movement.type.toUpperCase()}
                            </span>
                          </td>
                          <td className="p-3 text-sm text-gray-900 dark:text-gray-100">
                            {movement.description}
                          </td>
                          <td className="p-3 text-sm text-gray-600 dark:text-gray-400">
                            {movement.subtype}
                          </td>
                          <td className="p-3 text-sm font-medium text-gray-900 dark:text-gray-100">
                            {movement.currency} {parseFloat(movement.amount).toFixed(2)}
                          </td>
                          <td className="p-3 text-sm font-medium text-green-600 dark:text-green-400">
                            ${parseFloat(movement.amountUsd).toFixed(2)}
                          </td>
                          <td className="p-3 text-sm text-gray-600 dark:text-gray-400">
                            {movement.userName}
                          </td>
                        </tr>
                      )) || (
                        <tr>
                          <td colSpan={7} className="p-8 text-center text-gray-500 dark:text-gray-400">
                            No hay movimientos registrados
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            {/* Sección de Rendimiento de Vendedores */}
            <Card className="mt-8">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <div className="flex items-center">
                    <Trophy className="mr-2 h-5 w-5 text-yellow-600" />
                    Rendimiento de Vendedores
                  </div>
                  <div className="flex items-center space-x-2">
                    <Label className="text-sm">Período:</Label>
                    <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
                      <SelectTrigger className="w-32">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="week">Semana</SelectItem>
                        <SelectItem value="month">Mes</SelectItem>
                        <SelectItem value="quarter">Trimestre</SelectItem>
                        <SelectItem value="year">Año</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </CardTitle>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  🏆 Análisis completo de ganancias y rendimiento por vendedor
                </p>
              </CardHeader>
              <CardContent>
                {/* Resumen General */}
                {vendorPerformanceSummary && (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                    <Card className="bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800">
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm text-blue-600 dark:text-blue-400">Vendedores Activos</p>
                            <p className="text-2xl font-bold text-blue-800 dark:text-blue-200">
                              {vendorPerformanceSummary.totalVendors}
                            </p>
                          </div>
                          <Users className="h-8 w-8 text-blue-600" />
                        </div>
                      </CardContent>
                    </Card>

                    <Card className="bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800">
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm text-green-600 dark:text-green-400">Ventas Totales</p>
                            <p className="text-2xl font-bold text-green-800 dark:text-green-200">
                              ${vendorPerformanceSummary.totalRevenue || '0.00'}
                            </p>
                          </div>
                          <DollarSign className="h-8 w-8 text-green-600" />
                        </div>
                      </CardContent>
                    </Card>

                    <Card className="bg-purple-50 dark:bg-purple-900/20 border-purple-200 dark:border-purple-800">
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm text-purple-600 dark:text-purple-400">Ganancia Total</p>
                            <p className="text-2xl font-bold text-purple-800 dark:text-purple-200">
                              {vendorProfits && vendorProfits.length > 0 
                                ? `$${vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.totalProfit || '0'), 0).toFixed(2)}`
                                : '$0.00'
                              }
                            </p>
                          </div>
                          <Target className="h-8 w-8 text-purple-600" />
                        </div>
                      </CardContent>
                    </Card>

                    <Card className="bg-orange-50 dark:bg-orange-900/20 border-orange-200 dark:border-orange-800">
                      <CardContent className="p-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm text-orange-600 dark:text-orange-400">Margen Promedio</p>
                            <p className="text-2xl font-bold text-orange-800 dark:text-orange-200">
                              {vendorProfits && vendorProfits.length > 0 
                                ? `${(vendorProfits.reduce((sum, vendor) => sum + parseFloat(vendor.profitMargin || '0'), 0) / vendorProfits.length).toFixed(1)}%`
                                : '0.0%'
                              }
                            </p>
                          </div>
                          <TrendingUp className="h-8 w-8 text-orange-600" />
                        </div>
                      </CardContent>
                    </Card>
                  </div>
                )}

                {/* Tabla de Rankings */}
                <div className="mb-6">
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4 flex items-center">
                    <Award className="mr-2 h-5 w-5 text-yellow-600" />
                    Ranking de Vendedores
                  </h3>
                  <div className="overflow-x-auto">
                    <table className="w-full border-collapse">
                      <thead>
                        <tr className="border-b border-gray-200 dark:border-gray-700">
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Posición</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Vendedor</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Ventas</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Ganancia</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Margen</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Órdenes</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Promedio</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-700 dark:text-gray-300">Score</th>
                        </tr>
                      </thead>
                      <tbody>
                        {vendorRankings?.map((vendor, index) => (
                          <tr key={vendor.vendorId} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800">
                            <td className="p-3">
                              <div className="flex items-center">
                                {index === 0 && <Trophy className="h-5 w-5 text-yellow-500 mr-2" />}
                                {index === 1 && <Award className="h-5 w-5 text-gray-400 mr-2" />}
                                {index === 2 && <Award className="h-5 w-5 text-orange-500 mr-2" />}
                                <span className="text-lg font-bold text-gray-900 dark:text-gray-100">
                                  #{vendor.rank}
                                </span>
                              </div>
                            </td>
                            <td className="p-3">
                              <div className="font-medium text-gray-900 dark:text-gray-100">
                                {vendor.vendorName}
                              </div>
                            </td>
                            <td className="p-3 text-sm font-medium text-green-600 dark:text-green-400">
                              ${vendor.totalSales}
                            </td>
                            <td className="p-3 text-sm font-medium text-blue-600 dark:text-blue-400">
                              ${(() => {
                                const profitData = vendorProfits?.find(p => p.vendorId === vendor.vendorId);
                                return profitData?.totalProfit || vendor.estimatedProfit;
                              })()}
                            </td>
                            <td className="p-3">
                              <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${
                                (() => {
                                  const profitData = vendorProfits?.find(p => p.vendorId === vendor.vendorId);
                                  const margin = profitData?.profitMargin ? parseFloat(profitData.profitMargin) : 
                                    parseFloat(vendor.totalSales) > 0 ? 
                                      ((parseFloat(vendor.estimatedProfit) / parseFloat(vendor.totalSales)) * 100) : 0;

                                  if (margin > 20) return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
                                  if (margin > 10) return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200';
                                  if (margin > 0) return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200';
                                  return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200';
                                })()
                              }`}>
                                {(() => {
                                  const profitData = vendorProfits?.find(p => p.vendorId === vendor.vendorId);
                                  if (profitData?.profitMargin) {
                                    return `${profitData.profitMargin}`;
                                  }
                                  return parseFloat(vendor.totalSales) > 0 
                                    ? ((parseFloat(vendor.estimatedProfit) / parseFloat(vendor.totalSales)) * 100).toFixed(1)
                                    : '0.0';
                                })()}%
                              </span>
                            </td>
                            <td className="p-3 text-sm text-gray-900 dark:text-gray-100">
                              {vendor.totalOrders}
                            </td>
                            <td className="p-3 text-sm text-gray-600 dark:text-gray-400">
                              ${vendor.averageOrderValue}
                            </td>
                            <td className="p-3">
                              <div className="flex items-center">
                                <div className="w-16 bg-gray-200 dark:bg-gray-700 rounded-full h-2 mr-2">
                                  <div 
                                    className="bg-blue-600 h-2 rounded-full" 
                                    style={{ width: `${vendor.completionRate}%` }}
                                  ></div>
                                </div>
                                <span className="text-xs text-gray-600 dark:text-gray-400">
                                  {vendor.completionRate}
                                </span>
                              </div>
                            </td>
                          </tr>
                        )) || (
                          <tr>
                            <td colSpan={8} className="p-8 text-center text-gray-500 dark:text-gray-400">
                              No hay datos de vendedores disponibles
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Detalles de Ganancias por Vendedor */}
                <div>
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex items-center">
                      <Calculator className="mr-2 h-5 w-5 text-green-600" />
                      Detalles de Ganancias
                    </h3>
                    <Select value={selectedVendor || "all"} onValueChange={(value) => setSelectedVendor(value === "all" ? null : value)}>
                      <SelectTrigger className="w-48">
                        <SelectValue placeholder="Todos los vendedores" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Todos los vendedores</SelectItem>
                        {vendorRankings?.map((vendor) => (
                          <SelectItem key={vendor.vendorId} value={vendor.vendorId.toString()}>
                            {vendor.vendorName}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {vendorProfits && vendorProfits.length > 0 ? (
                    <div className="grid gap-4">
                      {vendorProfits.map((profit) => (
                        <Card key={profit.vendorId} className="border-l-4 border-l-blue-500">
                          <CardContent className="p-4">
                            <div className="flex items-center justify-between mb-3">
                              <h4 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex items-center">
                                <UserCheck className="mr-2 h-5 w-5 text-blue-600" />
                                {profit.vendorName}
                              </h4>
                              <span className="text-sm bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                                {profit.period}
                              </span>
                            </div>

                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                              <div className="bg-green-50 dark:bg-green-900/20 p-3 rounded">
                                <p className="text-sm text-green-600 dark:text-green-400">Ventas Totales</p>
                                <p className="text-lg font-bold text-green-800 dark:text-green-200">
                                  ${profit.totalRevenue}
                                </p>
                              </div>

                              <div className="bg-blue-50 dark:bg-blue-900/20 p-3 rounded">
                                <p className="text-sm text-blue-600 dark:text-blue-400">Ingresos</p>
                                <p className="text-lg font-bold text-blue-800 dark:text-green-200">
                                  ${profit.totalRevenue}
                                </p>
                              </div>

                              <div className="bg-purple-50 dark:bg-purple-900/20 p-3 rounded">
                                <p className="text-sm text-purple-600 dark:text-purple-400">Ganancia Neta</p>
                                <p className="text-lg font-bold text-purple-800 dark:text-purple-200">
                                  ${profit.totalProfit}
                                </p>
                              </div>

                              <div className="bg-orange-50 dark:bg-orange-900/20 p-3 rounded">
                                <p className="text-sm text-orange-600 dark:text-orange-400">Comisión</p>
                                <p className="text-lg font-bold text-orange-800 dark:text-orange-200">
                                  ${profit.commission}
                                </p>
                                <p className="text-xs text-orange-600 dark:text-orange-400">
                                  ({profit.commissionRate}%)
                                </p>
                              </div>
                            </div>

                            <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-700">
                              <div className="flex justify-between items-center text-sm text-gray-600 dark:text-gray-400">
                                <span>Órdenes: {profit.orderCount}</span>
                                <span>Promedio: ${(parseFloat(profit.totalRevenue) / profit.orderCount).toFixed(2)}</span>
                                <span>Margen: {profit.profitMargin}%</span>
                              </div>
                            </div>

                            {/* Desglose detallado de ventas */}
                            <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                              <button
                                onClick={() => {
                                  const newExpandedSales = new Set(expandedSales);
                                  if (newExpandedSales.has(profit.vendorId)) {
                                    newExpandedSales.delete(profit.vendorId);
                                  } else {
                                    newExpandedSales.add(profit.vendorId);
                                  }
                                  setExpandedSales(newExpandedSales);
                                }}
                                className="flex items-center justify-between w-full text-left text-sm font-medium text-gray-700 dark:text-gray-300 hover:text-gray-900 dark:hover:text-gray-100"
                              >
                                <span className="flex items-center">
                                  <ShoppingCart className="mr-2 h-4 w-4" />
                                  Desglose de Ventas ({profit.orderCount})
                                </span>
                                <ChevronDown className={`h-4 w-4 transition-transform ${
                                  expandedSales?.has(profit.vendorId) ? 'rotate-180' : ''
                                }`} />
                              </button>

                              {expandedSales?.has(profit.vendorId) && (
                                <div className="mt-3 space-y-2 max-h-80 overflow-y-auto">
                                  {profit.orders && profit.orders.length > 0 ? (
                                    <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs">
                                          <thead>
                                            <tr className="border-b border-gray-200 dark:border-gray-700">
                                              <th className="text-left py-2 text-gray-600 dark:text-gray-400">Pedido</th>
                                              <th className="text-left py-2 text-gray-600 dark:text-gray-400">Cliente</th>
                                              <th className="text-left py-2 text-gray-600 dark:text-gray-400">Fecha</th>
                                              <th className="text-right py-2 text-gray-600 dark:text-gray-400">Ingresos</th>
                                              <th className="text-right py-2 text-gray-600 dark:text-gray-400">Costo</th>
                                              <th className="text-right py-2 text-gray-600 dark:text-gray-400">Ganancia</th>
                                              <th className="text-right py-2 text-gray-600 dark:text-gray-400">Margen</th>
                                              <th className="text-center py-2 text-gray-600 dark:text-gray-400">Estado</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {profit.orders.map((order: any, index: number) => (
                                              <tr key={index} className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700">
                                                <td className="py-2 font-medium text-blue-600 dark:text-blue-400">
                                                  #{order.orderId}
                                                </td>
                                                <td className="py-2 text-gray-700 dark:text-gray-300">
                                                  {order.customerName}
                                                </td>
                                                <td className="py-2 text-gray-600 dark:text-gray-400">
                                                  {new Date(order.date).toLocaleDateString()}
                                                </td>
                                                <td className="py-2 text-right font-medium text-green-600 dark:text-green-400">
                                                  ${order.revenue}
                                                </td>
                                                <td className="py-2 text-right text-red-600 dark:text-red-400">
                                                  ${order.cost}
                                                </td>
                                                <td className="py-2 text-right font-bold text-purple-600 dark:text-purple-400">
                                                  ${order.profit}
                                                </td>
                                                <td className="py-2 text-right text-gray-600 dark:text-gray-400">
                                                  {order.profitMargin}%
                                                </td>
                                                <td className="py-2 text-center">
                                                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                                    order.paymentStatus === 'pagado' 
                                                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                                                      : order.paymentStatus === 'parcial'
                                                      ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
                                                      : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                                                  }`}>
                                                    {order.paymentStatus === 'pagado' ? 'Pagado' : 
                                                     order.paymentStatus === 'parcial' ? 'Parcial' : 'Pendiente'}
                                                  </span>
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>

                                      {/* Resumen del desglose */}
                                      <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-600">
                                        <div className="grid grid-cols-4 gap-4 text-center">
                                          <div>
                                            <p className="text-xs text-gray-500 dark:text-gray-400">Total Ventas</p>
                                            <p className="font-bold text-green-600 dark:text-green-400">
                                              ${profit.totalRevenue}
                                            </p>
                                          </div>
                                          <div>
                                            <p className="text-xs text-gray-500 dark:text-gray-400">Total Costos</p>
                                            <p className="font-bold text-red-600 dark:text-red-400">
                                              ${profit.totalCost}
                                            </p>
                                          </div>
                                          <div>
                                            <p className="text-xs text-gray-500 dark:text-gray-400">Ganancia Neta</p>
                                            <p className="font-bold text-purple-600 dark:text-purple-400">
                                              ${profit.totalProfit}
                                            </p>
                                          </div>
                                          <div>
                                            <p className="text-xs text-gray-500 dark:text-gray-400">Comisión</p>
                                            <p className="font-bold text-orange-600 dark:text-orange-400">
                                              ${profit.commission}
                                            </p>
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  ) : (
                                    <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
                                      No hay datos de ventas disponibles
                                    </p>
                                  )}
                                </div>
                              )}
                            </div>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  ) : (
                    <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                      <Calculator className="mx-auto h-12 w-12 mb-3" />
                      <p>No hay datos de ganancias disponibles para el período seleccionado</p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

          </div>
        </main>
      </div>

      {/* Modal para Registrar Gasto */}
      {showExpenseModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
              Registrar Nuevo Gasto
            </h3>
            <div className="space-y-4">
              <div>
                <Label htmlFor="category">Categoría</Label>
                <Select value={expenseForm.category} onValueChange={(value) => setExpenseForm({...expenseForm, category: value})}>
                  <SelectTrigger>
                    <SelectValue placeholder="Seleccionar categoría" />
                  </SelectTrigger>
                  <SelectContent>
                    {categoriesExpenses.map((category) => (
                      <SelectItem key={category.value} value={category.value}>
                        {category.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="description">Descripción</Label>
                <Input
                  id="description"
                  value={expenseForm.description}
                  onChange={(e) => setExpenseForm({...expenseForm, description: e.target.value})}
                  placeholder="Descripción del gasto"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="amount">Monto</Label>
                  <Input
                    id="amount"
                    type="number"
                    step="0.01"
                    value={expenseForm.amount}
                    onChange={(e) => setExpenseForm({...expenseForm, amount: e.target.value})}
                    placeholder="0.00"
                  />
                </div>
                <div>
                  <Label htmlFor="currency">Moneda</Label>
                  <Select value={expenseForm.currency} onValueChange={(value) => setExpenseForm({...expenseForm, currency: value})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Moneda" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="USD">USD</SelectItem>
                      <SelectItem value="ARS">ARS</SelectItem>
                      <SelectItem value="USDT">USDT</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div>
                <Label htmlFor="paymentMethod">Método de Pago</Label>
                <Select value={expenseForm.paymentMethod} onValueChange={(value) => setExpenseForm({...expenseForm, paymentMethod: value})}>
                  <SelectTrigger>
                    <SelectValue placeholder="Método de pago" />
                  </SelectTrigger>
                  <SelectContent>
                    {paymentMethods.map((method) => (
                      <SelectItem key={method.value} value={method.value}>
                        {method.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="notes">Notas (Opcional)</Label>
                <Textarea
                  id="notes"
                  value={expenseForm.notes}
                  onChange={(e) => setExpenseForm({...expenseForm, notes: e.target.value})}
                  placeholder="Notas adicionales"
                  rows={3}
                />
              </div>
            </div>

            <div className="flex justify-end space-x-2 mt-6">
              <Button variant="outline" onClick={() => setShowExpenseModal(false)}>
                Cancelar
              </Button>
              <Button 
                onClick={() => createExpenseMutation.mutate(expenseForm)}
                disabled={createExpenseMutation.isPending}
              >
                {createExpenseMutation.isPending ? "Guardando..." : "Guardar Gasto"}
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Modal para Pago de Deuda */}
      {showDebtPaymentModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
              Registrar Pago de Deuda
            </h3>
            <div className="space-y-4">
              <div>
                <Label htmlFor="amount">Monto a Pagar</Label>
                <Input
                  id="amount"
                  type="number"
                  step="0.01"
                  value={debtPaymentForm.amount}
                  onChange={(e) => setDebtPaymentForm({...debtPaymentForm, amount: e.target.value})}
                  placeholder="0.00"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="currency">Moneda</Label>
                  <Select value={debtPaymentForm.currency} onValueChange={(value) => setDebtPaymentForm({...debtPaymentForm, currency: value})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Moneda" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="USD">USD</SelectItem>
                      <SelectItem value="ARS">ARS</SelectItem>
                      <SelectItem value="USDT">USDT</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <Label htmlFor="paymentMethod">Método</Label>
                  <Select value={debtPaymentForm.paymentMethod} onValueChange={(value) => setDebtPaymentForm({...debtPaymentForm, paymentMethod: value})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Método" />
                    </SelectTrigger>
                    <SelectContent>
                      {paymentMethods.map((method) => (
                        <SelectItem key={method.value} value={method.value}>
                          {method.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div>
                <Label htmlFor="notes">Notas (Opcional)</Label>
                <Textarea
                  id="notes"
                  value={debtPaymentForm.notes}
                  onChange={(e) => setDebtPaymentForm({...debtPaymentForm, notes: e.target.value})}
                  placeholder="Notas del pago"
                  rows={3}
                />
              </div>
            </div>

            <div className="flex justify-end space-x-2 mt-6">
              <Button variant="outline" onClick={() => setShowDebtPaymentModal(false)}>
                Cancelar
              </Button>
              <Button 
                onClick={() => createDebtPaymentMutation.mutate(debtPaymentForm)}
                disabled={createDebtPaymentMutation.isPending}
              >
                {createDebtPaymentMutation.isPending ? "Procesando..." : "Registrar Pago"}
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Modal para Registrar Ingreso */}
      {showIncomeModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
              Registrar Nuevo Ingreso
            </h3>
            <div className="space-y-4">
              <div>
                <Label htmlFor="description">Descripción</Label>
                <Input
                  id="description"
                  value={incomeForm.description}
                  onChange={(e) => setIncomeForm({...incomeForm, description: e.target.value})}
                  placeholder="Descripción del ingreso"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="amount">Monto</Label>
                  <Input
                    id="amount"
                    type="number"
                    step="0.01"
                    value={incomeForm.amount}
                    onChange={(e) => setIncomeForm({...incomeForm, amount: e.target.value})}
                    placeholder="0.00"
                  />
                </div>
                <div>
                  <Label htmlFor="currency">Moneda</Label>
                  <Select value={incomeForm.currency} onValueChange={(value) => setIncomeForm({...incomeForm, currency: value})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Moneda" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="USD">USD</SelectItem>
                      <SelectItem value="ARS">ARS</SelectItem>
                      <SelectItem value="USDT">USDT</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div>
                <Label htmlFor="paymentMethod">Método de Pago</Label>
                <Select value={incomeForm.paymentMethod} onValueChange={(value) => setIncomeForm({...incomeForm, paymentMethod: value})}>
                  <SelectTrigger>
                    <SelectValue placeholder="Método de pago" />
                  </SelectTrigger>
                  <SelectContent>
                    {paymentMethods.map((method) => (
                      <SelectItem key={method.value} value={method.value}>
                        {method.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="notes">Notas (Opcional)</Label>
                <Textarea
                  id="notes"
                  value={incomeForm.notes}
                  onChange={(e) => setIncomeForm({...incomeForm, notes: e.target.value})}
                  placeholder="Notas adicionales"
                  rows={3}
                />
              </div>
            </div>

            <div className="flex justify-end space-x-2 mt-6">
              <Button variant="outline" onClick={() => setShowIncomeModal(false)}>
                Cancelar
              </Button>
              <Button 
                onClick={() => createIncomeMutation.mutate(incomeForm)}
                disabled={createIncomeMutation.isPending}
              >
                {createIncomeMutation.isPending ? "Guardando..." : "Registrar Ingreso"}
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Modal de Historial */}
      {showHistoryModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg max-w-6xl w-full mx-4 max-h-[90vh] overflow-hidden">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex justify-between items-center">
                <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100">
                  📊 Historial de Reportes Diarios
                </h2>
                <button
                  onClick={() => setShowHistoryModal(false)}
                  className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                >
                  ✕
                </button>
              </div>
            </div>

            <div className="p-6 overflow-y-auto max-h-[calc(90vh-120px)]">
              {/* Reportes Excel Automáticos */}
              {generatedReports && generatedReports.length > 0 && (
                <div className="mb-8">
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4 flex items-center">
                    📊 Reportes Excel Automáticos
                    <span className="ml-2 text-sm bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 px-2 py-1 rounded-full">
                      {generatedReports.length}
                    </span>
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {generatedReports.map((report: any) => (
                      <Card key={report.id} className="border-l-4 border-l-green-500 hover:shadow-md transition-shadow">
                        <CardContent className="p-4">
                          <div className="flex justify-between items-start mb-3">
                            <div>
                              <h4 className="font-semibold text-gray-900 dark:text-gray-100">
                                {new Date(report.reportDate).toLocaleDateString('es-ES')}
                              </h4>
                              <p className="text-sm text-gray-600 dark:text-gray-400">
                                Generado: {new Date(report.generatedAt).toLocaleDateString('es-ES')}
                              </p>
                            </div>
                            <span className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 px-2 py-1 rounded text-xs font-medium">
                              {report.reportType.toUpperCase()}
                            </span>
                          </div>

                          <div className="text-sm text-gray-600 dark:text-gray-400 mb-3">
                            <p className="truncate" title={report.fileName}>
                              📄 {report.fileName}
                            </p>
                            <p>📏 {(report.fileSize / 1024).toFixed(1)} KB</p>
                          </div>

                          <Button
                            size="sm"
                            className="w-full"
                            onClick={() => {
                              window.open(`/api/generated-reports/${report.id}/download`, '_blank');
                            }}
                          >
                            📥 Descargar Excel
                          </Button>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </div>
              )}

              {/* Historial de Reportes Diarios */}
              {dailyReports && dailyReports.length > 0 ? (
                <div>
                  <div className="flex justify-between items-center mb-4">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex items-center">
                      📈 Historial de Reportes Diarios
                      <span className="ml-2 text-sm bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 px-2 py-1 rounded-full">
                        {dailyReports.length}
                      </span>
                    </h3>
                    <div className="flex space-x-1">
                      <button 
                        className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                          dailyReportFilter === "all" 
                            ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                            : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                        }`}
                        onClick={() => setDailyReportFilter("all")}
                      >
                        Todo
                      </button>
                      <button 
                        className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                          dailyReportFilter === "week" 
                            ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                            : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                        }`}
                        onClick={() => setDailyReportFilter("week")}
                      >
                        Semana
                      </button>
                      <button 
                        className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                          dailyReportFilter === "month" 
                            ? "bg-gray-200 text-gray-900 dark:bg-gray-700 dark:text-gray-100" 
                            : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                        }`}
                        onClick={() => setDailyReportFilter("month")}
                      >
                        Mes
                      </button>
                      <button 
                        className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                          dailyReportFilter === "year" 
                            ? "bg-blue-500 text-white hover:bg-blue-600" 
                            : "bg-white text-gray-600 hover:bg-gray-50 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700"
                        }`}
                        onClick={() => setDailyReportFilter("year")}
                      >
                        Año
                      </button>
                    </div>
                  </div>
                  <div className="space-y-4">
                    {dailyReports.map((report: any, index: number) => (
                      <Card key={index} className="border-l-4 border-l-blue-500">
                        <CardContent className="p-4">
                          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                            <div>
                              <h3 className="font-semibold text-gray-900 dark:text-gray-100">
                                {new Date(report.reportDate).toLocaleDateString('es-ES')}
                              </h3>
                              <p className="text-sm text-gray-600 dark:text-gray-400">
                                Reporte #{index + 1}
                              </p>
                            </div>

                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Balance Apertura</p>
                              <p className="font-semibold text-green-600">
                                ${report.openingBalance}
                              </p>
                            </div>

                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Balance Cierre</p>
                              <p className="font-semibold text-blue-600">
                                ${report.closingBalance}
                              </p>
                            </div>

                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Ganancia Neta</p>
                              <p className={`font-semibold ${
                                parseFloat(report.netProfit) >= 0 ? 'text-green-600' : 'text-red-600'
                              }`}>
                                ${report.netProfit}
                              </p>
                            </div>
                          </div>

                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Ingresos Totales</p>
                              <p className="font-semibold text-green-600">
                                ${report.totalIncome}
                              </p>
                            </div>

                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Gastos Totales</p>
                              <p className="font-semibold text-red-600">
                                ${report.totalExpenses}
                              </p>
                            </div>

                            <div>
                              <p className="text-sm text-gray-600 dark:text-gray-400">Movimientos</p>
                              <p className="font-semibold text-blue-600">
                                {report.totalMovements}
                              </p>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </div>
              ) : (
                !generatedReports?.length && (
                  <div className="text-center py-12">
                    <Calendar className="h-16 w-16 text-gray-300 mx-auto mb-4" />
                    <p className="text-gray-500 dark:text-gray-400 text-lg">
                      No hay reportes disponibles
                    </p>
                    <p className="text-gray-400 dark:text-gray-500 text-sm mt-2">
                      Los reportes se generan automáticamente al cierre diario a las 23:59:00
                    </p>
                  </div>
                )
              )}
            </div>

            <div className="p-6 border-t border-gray-200 dark:border-gray-700 flex justify-end">
              <Button 
                onClick={() => setShowHistoryModal(false)}
                variant="outline"
              >
                Cerrar
              </Button>
            </div>
          </div>
        </div>
      )}

      <Footer />
    </div>
  );
}