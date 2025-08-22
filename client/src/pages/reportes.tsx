import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/lib/auth";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { FileBarChart, Download, Calendar, TrendingUp, Users, DollarSign, RefreshCw, Printer } from "lucide-react";

export default function Reportes() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [selectedPeriod, setSelectedPeriod] = useState("today");
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();

  // Consulta para obtener los movimientos de caja
  const { data: cashMovements, isLoading: movementsLoading, refetch: refetchMovements } = useQuery({
    queryKey: ['/api/cash-movements', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/cash-movements?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId
  });

  // Consulta para obtener las órdenes y pagos
  const { data: orders, isLoading: ordersLoading } = useQuery({
    queryKey: ['/api/orders', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/orders?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  if (!isAuthenticated) {
    return null;
  }

  // Función para calcular los balances por categoría
  const calculateBalancesByCategory = () => {
    const balances = {
      cajaUsd: 0,
      cajaTransferenciaUsd: 0,
      cajaUsdt: 0,
      cajaFinancieraUsd: 0,
      cajaPesos: 0,
      cajaTransferenciaPesos: 0,
      cajaFinancieraPesos: 0,
      cajaPagosVendedores: 0,
      cajaGananciasNetas: 0,
      cajaCostosVendidos: 0,
      cajaCostosProducto: 0
    };

    const movementStats = {
      totalMovements: 0,
      incomeMovements: 0,
      expenseMovements: 0,
      processedOrders: 0
    };

    if (!cashMovements || !orders) return { balances, movementStats };

    // Filtrar movimientos según el período seleccionado
    const filterByPeriod = (date: string) => {
      const movementDate = new Date(date);
      const today = new Date();
      
      switch (selectedPeriod) {
        case "today":
          return movementDate.toDateString() === today.toDateString();
        case "week":
          const weekAgo = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
          return movementDate >= weekAgo;
        case "month":
          const monthAgo = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000);
          return movementDate >= monthAgo;
        default:
          return true;
      }
    };

    // Procesar movimientos de caja con mejor clasificación
    const filteredMovements = cashMovements.filter((movement: any) => filterByPeriod(movement.createdAt));
    movementStats.totalMovements = filteredMovements.length;

    // Tipo de cambio aproximado USD a ARS (puede ser configurado)
    const USD_TO_ARS_RATE = 1000; // 1 USD = 1000 ARS aproximadamente

    filteredMovements.forEach((movement: any) => {
      const amountUsd = parseFloat(movement.amount_usd || movement.amountUsd || movement.amount || 0);
      const originalAmount = parseFloat(movement.amount || 0);
      const currency = movement.currency || 'USD';
      const isIncome = movement.type === 'ingreso' || movement.type === 'venta' || movement.type === 'pago_deuda';
      
      // Para los balances en ARS, convertir USD a ARS
      let finalAmountARS = 0;
      if (currency === 'ARS') {
        finalAmountARS = isIncome ? originalAmount : -originalAmount;
      } else {
        // Convertir USD a ARS para mostrar en pesos
        finalAmountARS = isIncome ? (amountUsd * USD_TO_ARS_RATE) : -(amountUsd * USD_TO_ARS_RATE);
      }
      
      const finalAmountUSD = isIncome ? amountUsd : -amountUsd;

      if (isIncome) {
        movementStats.incomeMovements++;
      } else {
        movementStats.expenseMovements++;
      }

      // Clasificar según el subtipo del movimiento (que contiene el método de pago)
      const subtype = (movement.subtype || '').toLowerCase();
      
      // Efectivo USD
      if (subtype === 'efectivo_usd') {
        balances.cajaUsd += finalAmountUSD;
      }
      // Transferencia USD
      else if (subtype === 'transferencia_usd') {
        balances.cajaTransferenciaUsd += finalAmountUSD;
      }
      // USDT - incluyendo transferencia_usdt
      else if (subtype === 'usdt' || subtype === 'transferencia_usdt' || movement.currency === 'USDT') {
        balances.cajaUsdt += finalAmountUSD;
      }
      // Financiera USD
      else if (subtype === 'financiera_usd') {
        balances.cajaFinancieraUsd += finalAmountUSD;
      }
      // Efectivo ARS/Pesos - mostrar en ARS
      else if (subtype === 'efectivo_ars') {
        balances.cajaPesos += finalAmountARS;
      }
      // Transferencia ARS/Pesos - mostrar en ARS
      else if (subtype === 'transferencia_ars') {
        balances.cajaTransferenciaPesos += finalAmountARS;
      }
      // Financiera ARS/Pesos - mostrar en ARS
      else if (subtype === 'financiera_ars') {
        balances.cajaFinancieraPesos += finalAmountARS;
      }
    });

    // Procesar órdenes para calcular ganancias y costos con más detalle
    const filteredOrders = orders.filter((order: any) => filterByPeriod(order.createdAt));
    movementStats.processedOrders = filteredOrders.length;

    filteredOrders.forEach((order: any) => {
      const orderItems = order.orderItems || []; // Usar orderItems como viene de la API
      
      // Obtener información del vendedor directamente de la orden (nuevo formato API)
      let vendorCommissionRate = 0.10; // 10% por defecto
      
      // Si existe vendorCommission en la orden, usarlo
      if (order.vendorCommission) {
        vendorCommissionRate = parseFloat(order.vendorCommission) / 100;
      }
      
      let totalCostPrice = 0;
      let totalSalePrice = 0;
      
      orderItems.forEach((item: any) => {
        // Usar costPrice y priceUsd desde los datos de la API
        const costPrice = parseFloat(item.product?.costPrice || 0);
        const salePrice = parseFloat(item.priceUsd || 0);
        totalCostPrice += costPrice;
        totalSalePrice += salePrice;
      });

      const netProfit = totalSalePrice - totalCostPrice;
      
      // Calcular comisión del vendedor en USD
      const vendorCommissionUSD = netProfit * vendorCommissionRate;
      
      // Ganancia del admin = ganancia total - comisión del vendedor
      const adminProfitUSD = netProfit - vendorCommissionUSD;
      
      // Todos los valores en USD para "Análisis de Ganancias y Costos"
      balances.cajaPagosVendedores += vendorCommissionUSD; // Comisión del vendedor en USD
      balances.cajaGananciasNetas += adminProfitUSD; // Ganancia del admin en USD
      balances.cajaCostosProducto += totalCostPrice; // Costo base de productos en USD
    });

    // Corregir: Costos Vendidos = Pagos Vendedores + Ganancias Netas
    balances.cajaCostosVendidos = balances.cajaPagosVendedores + balances.cajaGananciasNetas;

    return { balances, movementStats };
  };

  const { balances, movementStats } = calculateBalancesByCategory();
  const isLoading = movementsLoading || ordersLoading;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50 dark:from-gray-900 dark:to-gray-800 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            
            {/* Header */}
            <div className="mb-8">
              <div className="flex items-center justify-between">
                <div>
                  <div className="flex items-center space-x-3 mb-2">
                    <FileBarChart className="h-8 w-8 text-primary" />
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                      REPORTES FINANCIEROS
                    </h1>
                  </div>
                  <p className="text-gray-600 dark:text-gray-300">
                    Análisis detallado de todas las categorías de caja y métodos de pago
                  </p>
                </div>
                
                <div className="flex items-center space-x-4">
                  <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
                    <SelectTrigger className="w-40">
                      <SelectValue placeholder="Seleccionar período" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="today">Hoy</SelectItem>
                      <SelectItem value="week">Esta Semana</SelectItem>
                      <SelectItem value="month">Este Mes</SelectItem>
                    </SelectContent>
                  </Select>
                  
                  <Button 
                    onClick={() => {
                      refetchMovements();
                      window.location.reload();
                    }}
                    variant="outline"
                    size="icon"
                    disabled={isLoading}
                  >
                    <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
                  </Button>
                </div>
              </div>
            </div>

            {/* Estadísticas de Sincronización */}
            <Card className="mb-6 border-blue-200 dark:border-blue-800">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    <div className="text-center">
                      <p className="text-2xl font-bold text-blue-600">{movementStats.totalMovements}</p>
                      <p className="text-xs text-gray-500">Movimientos Totales</p>
                    </div>
                    <div className="text-center">
                      <p className="text-2xl font-bold text-green-600">{movementStats.incomeMovements}</p>
                      <p className="text-xs text-gray-500">Ingresos</p>
                    </div>
                    <div className="text-center">
                      <p className="text-2xl font-bold text-red-600">{movementStats.expenseMovements}</p>
                      <p className="text-xs text-gray-500">Gastos</p>
                    </div>
                    <div className="text-center">
                      <p className="text-2xl font-bold text-purple-600">{movementStats.processedOrders}</p>
                      <p className="text-xs text-gray-500">Órdenes</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-gray-600 dark:text-gray-300">
                      Período: <span className="font-semibold">
                        {selectedPeriod === 'today' ? 'Hoy' : 
                         selectedPeriod === 'week' ? 'Esta Semana' : 'Este Mes'}
                      </span>
                    </p>
                    <p className="text-xs text-gray-500">
                      Sincronizado con datos reales del sistema
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Reporte Principal */}
            <Card className="mb-8">
              <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                  <DollarSign className="h-6 w-6 text-green-600" />
                  <span>Reporte de Cajas por Moneda y Método</span>
                </CardTitle>
                <CardDescription>
                  Detalle completo de todas las categorías financieras del sistema - Datos sincronizados en tiempo real
                </CardDescription>
              </CardHeader>
              <CardContent>
                {/* Métodos de Pago USD */}
                <div className="mb-8">
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <span className="w-4 h-4 bg-green-500 rounded-full mr-2"></span>
                    💵 Métodos de Pago USD
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <Card className="border-green-200 dark:border-green-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-green-700 dark:text-green-300">Caja USD</h4>
                          <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-green-600">
                          ${balances.cajaUsd.toFixed(2)}
                        </p>
                        <p className="text-sm text-gray-500">Efectivo USD</p>
                      </CardContent>
                    </Card>
                    
                    <Card className="border-blue-200 dark:border-blue-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-blue-700 dark:text-blue-300">Caja Transferencia USD</h4>
                          <div className="w-2 h-2 bg-blue-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-blue-600">
                          ${balances.cajaTransferenciaUsd.toFixed(2)}
                        </p>
                        <p className="text-sm text-gray-500">Transferencias USD</p>
                      </CardContent>
                    </Card>
                    
                    <Card className="border-purple-200 dark:border-purple-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-purple-700 dark:text-purple-300">Caja USDT</h4>
                          <div className="w-2 h-2 bg-purple-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-purple-600">
                          ${balances.cajaUsdt.toFixed(2)}
                        </p>
                        <p className="text-sm text-gray-500">Criptomoneda USDT</p>
                      </CardContent>
                    </Card>
                    
                    <Card className="border-orange-200 dark:border-orange-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-orange-700 dark:text-orange-300">Caja Financiera USD</h4>
                          <div className="w-2 h-2 bg-orange-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-orange-600">
                          ${balances.cajaFinancieraUsd.toFixed(2)}
                        </p>
                        <p className="text-sm text-gray-500">Financiera USD</p>
                      </CardContent>
                    </Card>
                  </div>
                </div>

                {/* Métodos de Pago ARS */}
                <div className="mb-8">
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                    <span className="w-4 h-4 bg-yellow-500 rounded-full mr-2"></span>
                    💰 Métodos de Pago Pesos (ARS)
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <Card className="border-yellow-200 dark:border-yellow-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-yellow-700 dark:text-yellow-300">Caja Pesos</h4>
                          <div className="w-2 h-2 bg-yellow-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-yellow-600">
                          $ARS {balances.cajaPesos.toLocaleString('es-AR', { minimumFractionDigits: 2 })}
                        </p>
                        <p className="text-sm text-gray-500">Efectivo ARS</p>
                      </CardContent>
                    </Card>
                    
                    <Card className="border-cyan-200 dark:border-cyan-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-cyan-700 dark:text-cyan-300">Caja Transferencia Pesos</h4>
                          <div className="w-2 h-2 bg-cyan-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-cyan-600">
                          $ARS {balances.cajaTransferenciaPesos.toLocaleString('es-AR', { minimumFractionDigits: 2 })}
                        </p>
                        <p className="text-sm text-gray-500">Transferencias ARS</p>
                      </CardContent>
                    </Card>
                    
                    <Card className="border-red-200 dark:border-red-800 relative">
                      <CardContent className="p-4 text-center">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-red-700 dark:text-red-300">Caja Financiera Pesos</h4>
                          <div className="w-2 h-2 bg-red-500 rounded-full animate-pulse" title="Sincronizado en tiempo real"></div>
                        </div>
                        <p className="text-2xl font-bold text-red-600">
                          $ARS {balances.cajaFinancieraPesos.toLocaleString('es-AR', { minimumFractionDigits: 2 })}
                        </p>
                        <p className="text-sm text-gray-500">Financiera ARS</p>
                      </CardContent>
                    </Card>
                  </div>
                </div>

                {/* Análisis de Ganancias y Costos */}
                <div className="mb-8">
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
                          ${balances.cajaPagosVendedores.toFixed(2)}
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
                          ${balances.cajaGananciasNetas.toFixed(2)}
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
                          ${balances.cajaCostosVendidos.toFixed(2)}
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
                          ${balances.cajaCostosProducto.toFixed(2)}
                        </p>
                        <p className="text-sm text-gray-500">Costo base de productos</p>
                      </CardContent>
                    </Card>
                  </div>
                </div>

                {/* Botones de Acción */}
                <div className="flex justify-end space-x-4 pt-6 border-t">
                  <Button variant="outline">
                    <Printer className="mr-2 h-4 w-4" />
                    Imprimir Reporte
                  </Button>
                  <Button>
                    <Download className="mr-2 h-4 w-4" />
                    Exportar Excel
                  </Button>
                </div>
              </CardContent>
            </Card>

            {/* Reportes Adicionales Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              
              {/* Reporte de Ventas */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <TrendingUp className="h-5 w-5 text-green-600" />
                    <span>Reporte de Ventas</span>
                  </CardTitle>
                  <CardDescription>
                    Resumen completo de ventas por período
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Ventas por vendedor
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Productos más vendidos
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Análisis de ganancias
                    </div>
                    <Button className="w-full mt-4" disabled>
                      <Download className="mr-2 h-4 w-4" />
                      Generar Reporte
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Reporte de Inventario */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <FileBarChart className="h-5 w-5 text-blue-600" />
                    <span>Reporte de Inventario</span>
                  </CardTitle>
                  <CardDescription>
                    Estado actual del stock y movimientos
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Productos disponibles
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Productos reservados
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Historial de movimientos
                    </div>
                    <Button className="w-full mt-4" disabled>
                      <Download className="mr-2 h-4 w-4" />
                      Generar Reporte
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Reporte Financiero */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <DollarSign className="h-5 w-5 text-yellow-600" />
                    <span>Reporte Financiero</span>
                  </CardTitle>
                  <CardDescription>
                    Análisis financiero y flujo de caja
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Flujo de caja diario
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Ingresos y gastos
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Balance por moneda
                    </div>
                    <Button className="w-full mt-4" disabled>
                      <Download className="mr-2 h-4 w-4" />
                      Generar Reporte
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Reporte de Vendedores */}
              <Card className="hover:shadow-lg transition-shadow">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Users className="h-5 w-5 text-purple-600" />
                    <span>Reporte de Vendedores</span>
                  </CardTitle>
                  <CardDescription>
                    Performance y comisiones por vendedor
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Ranking de vendedores
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Comisiones calculadas
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Metas vs realizadas
                    </div>
                    <Button className="w-full mt-4" disabled>
                      <Download className="mr-2 h-4 w-4" />
                      Generar Reporte
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Reportes Excel Automáticos */}
              <Card className="hover:shadow-lg transition-shadow border-green-200 dark:border-green-800">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Calendar className="h-5 w-5 text-green-600" />
                    <span>Reportes Automáticos</span>
                  </CardTitle>
                  <CardDescription>
                    Reportes Excel generados automáticamente
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Generación diaria automática
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Almacenamiento en base de datos
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">
                      • Acceso desde Cajas Avanzadas
                    </div>
                    <Button 
                      className="w-full mt-4 bg-green-600 hover:bg-green-700" 
                      onClick={() => setLocation("/cash-advanced")}
                    >
                      <Calendar className="mr-2 h-4 w-4" />
                      Ver en Cajas Avanzadas
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Próximamente */}
              <Card className="hover:shadow-lg transition-shadow border-gray-200 dark:border-gray-700 opacity-75">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <FileBarChart className="h-5 w-5 text-gray-400" />
                    <span>Más reportes próximamente</span>
                  </CardTitle>
                  <CardDescription>
                    Nuevos reportes en desarrollo
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div className="text-sm text-gray-400">
                      • Reportes personalizados
                    </div>
                    <div className="text-sm text-gray-400">
                      • Alertas automáticas
                    </div>
                    <div className="text-sm text-gray-400">
                      • Análisis predictivo
                    </div>
                    <Button className="w-full mt-4" disabled>
                      Próximamente
                    </Button>
                  </div>
                </CardContent>
              </Card>

            </div>

            {/* Información adicional */}
            <div className="mt-8 p-6 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
              <div className="flex items-start space-x-3">
                <FileBarChart className="h-6 w-6 text-blue-600 mt-1" />
                <div>
                  <h3 className="font-semibold text-blue-900 dark:text-blue-100 mb-2">
                    📊 Centro de Reportes StockCel
                  </h3>
                  <p className="text-blue-800 dark:text-blue-200 text-sm">
                    Esta sección centraliza todos los reportes del sistema. Actualmente, los reportes Excel automáticos 
                    están funcionando y se generan diariamente durante el cierre de caja. Los demás reportes están 
                    en desarrollo y se implementarán próximamente según las necesidades del negocio.
                  </p>
                </div>
              </div>
            </div>
            
          </div>
        </main>
      </div>
      
      <Footer />
    </div>
  );
}