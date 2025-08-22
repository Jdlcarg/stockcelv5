import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { 
  DollarSign, 
  TrendingUp, 
  TrendingDown, 
  PlusCircle,
  CreditCard,
  Wallet,

  Settings,
  Download,
  FileText,
  RefreshCw
} from "lucide-react";

// Interfaces

interface CashMovement {
  id: number;
  clientId: number;
  userId: number;
  type: string;
  description: string | null;
  amountUsd: string;
  amountArs: string;
  amountUsdt: string;
  paymentMethod: string;
  category: string;
  customerName: string | null;
  vendorName: string | null;
  orderId: number | null;
  createdAt: string;
}

export default function PagosGastos() {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  // Modal states
  const [showExpenseModal, setShowExpenseModal] = useState(false);
  const [showDebtPaymentModal, setShowDebtPaymentModal] = useState(false);
  const [showExchangeRateModal, setShowExchangeRateModal] = useState(false);
  const [currentExchangeMethod, setCurrentExchangeMethod] = useState('');
  
  // Filter states
  const [customerFilter, setCustomerFilter] = useState("todos_clientes");
  const [vendorFilter, setVendorFilter] = useState("todos_vendedores");
  const [paymentMethodFilter, setPaymentMethodFilter] = useState("todos_metodos");
  const [typeFilter, setTypeFilter] = useState("todos");

  // Form states
  const [expenseForm, setExpenseForm] = useState({
    description: "",
    amount: "",
    currency: "USD",
    paymentMethod: "efectivo_usd",
    category: "gastos_operativos",
    customerName: "",
    vendorName: "",
    notes: ""
  });

  const [debtPaymentForm, setDebtPaymentForm] = useState({
    orderId: "",
    customerName: "",
    vendorName: "",
    notes: ""
  });

  // Estados para métodos de pago múltiples
  const [selectedPaymentMethods, setSelectedPaymentMethods] = useState<string[]>([]);
  const [paymentAmounts, setPaymentAmounts] = useState<Record<string, string>>({});
  const [financieraUSDAmount, setFinancieraUSDAmount] = useState('');
  const [financieraARSAmount, setFinancieraARSAmount] = useState('');

  // Exchange rate configuration
  const [exchangeRates, setExchangeRates] = useState({
    efectivo_usd: { usd: 1, ars: 1100 },
    efectivo_ars: { usd: 1100, ars: 1 },
    transferencia_usd: { usd: 1, ars: 1100 },
    transferencia_ars: { usd: 1100, ars: 1 },
    transferencia_usdt: { usd: 1, ars: 1100 },
    financiera_usd: { usd: 1, ars: 1100 },
    financiera_ars: { usd: 1100, ars: 1 }
  });

  // Métodos de pago disponibles como en create-order
  const availablePaymentMethods = [
    { value: 'efectivo_ars', label: 'Efectivo ARS' },
    { value: 'efectivo_usd', label: 'Efectivo USD' },
    { value: 'transferencia_ars', label: 'Transferencia ARS' },
    { value: 'transferencia_usd', label: 'Transferencia USD' },
    { value: 'transferencia_usdt', label: 'Transferencia USDT' },
    { value: 'financiera_usd', label: 'Financiera USD→ARS' },
    { value: 'financiera_ars', label: 'Financiera ARS→USD' }
  ];

  // Tasas de cambio para métodos de pago - Replicando exactamente create-order.tsx
  const [paymentExchangeRates, setPaymentExchangeRates] = useState({
    'efectivo_ars': '1100',
    'efectivo_usd': '1100',
    'transferencia_ars': '1100',
    'transferencia_usd': '1100',
    'transferencia_usdt': '1100',
    'financiera_usd_ars': '1100',
    'financiera_ars_usd': '1050'
  });

  // Función para actualizar tasas de cambio
  const updatePaymentExchangeRate = (key: string, rate: string) => {
    setPaymentExchangeRates(prev => ({
      ...prev,
      [key]: rate
    }));
  };

  // Funciones de cálculo para financiera
  const calculateARSFromUSDFinanciera = (usdAmount: string) => {
    if (!usdAmount || isNaN(parseFloat(usdAmount))) return '0.00';
    const amount = parseFloat(usdAmount);
    const rate = parseFloat(paymentExchangeRates['financiera_usd_ars']) || 1100;
    return (amount * rate).toFixed(2);
  };

  const calculateUSDFromARSFinanciera = (arsAmount: string) => {
    if (!arsAmount || isNaN(parseFloat(arsAmount))) return '0.00';
    const amount = parseFloat(arsAmount);
    const rate = parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050;
    return (amount / rate).toFixed(2);
  };

  // Fetch customers
  const { data: customers = [], isLoading: customersLoading, error: customersError } = useQuery({
    queryKey: ['/api/customers'],
    enabled: !!user?.clientId,
    queryFn: () => fetch('/api/customers', {
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': user?.id?.toString() || '',
        'x-client-id': user?.clientId?.toString() || ''
      }
    }).then(res => res.json())
  });

  // Fetch vendors
  const { data: vendors = [], isLoading: vendorsLoading, error: vendorsError } = useQuery({
    queryKey: ['/api/vendors'],
    enabled: !!user?.clientId,
    queryFn: () => fetch('/api/vendors', {
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': user?.id?.toString() || '',
        'x-client-id': user?.clientId?.toString() || ''
      }
    }).then(res => res.json())
  });

  // Fetch active debts
  const { data: activeDebts = [] } = useQuery({
    queryKey: ['/api/customer-debts/active', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/customer-debts/active?clientId=${user?.clientId}`, {
        headers: {
          'Content-Type': 'application/json',
          'x-user-id': user?.id?.toString() || '',
          'x-client-id': user?.clientId?.toString() || ''
        }
      });
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchInterval: 5000, // Refresh every 5 seconds for real-time sync
  });

  // Fetch all orders for debt payment (will filter by active debts)
  const { data: allOrders = [] } = useQuery({
    queryKey: ['/api/orders', user?.clientId],
    enabled: !!user?.clientId,
    queryFn: () => fetch(`/api/orders?clientId=${user?.clientId}`, {
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': user?.id?.toString() || '',
        'x-client-id': user?.clientId?.toString() || ''
      }
    }).then(res => res.json())
  });

  // Filter orders that have active debts
  const pendingOrders = (() => {
    if (!Array.isArray(allOrders) || !Array.isArray(activeDebts)) return [];
    
    const ordersWithDebts = allOrders.filter(order => 
      activeDebts.some(debt => debt.orderId === order.id)
    );
    
    console.log('All orders received:', allOrders);
    console.log('Active debts:', activeDebts);
    console.log('Orders with active debts (filtered):', ordersWithDebts);
    
    return ordersWithDebts;
  })();

  // Fetch cash movements
  const { data: movements = [], isLoading: loadingMovements } = useQuery({
    queryKey: ['/api/cash-movements'],
    enabled: !!user?.clientId,
    queryFn: () => fetch(`/api/cash-movements?clientId=${user?.clientId}`, {
      headers: {
        'Content-Type': 'application/json',
        'x-user-id': user?.id?.toString() || '',
        'x-client-id': user?.clientId?.toString() || ''
      }
    }).then(res => res.json())
  });

  // Add debugging logs after all variables are declared
  console.log('Pagos/Gastos Debug:', {
    user: user?.email,
    customersCount: Array.isArray(customers) ? customers.length : 0,
    vendorsCount: Array.isArray(vendors) ? vendors.length : 0,
    pendingOrdersCount: Array.isArray(pendingOrders) ? pendingOrders.length : 0,
    movementsCount: Array.isArray(movements) ? movements.length : 0,
    customersLoading,
    vendorsLoading,
    pendingOrdersData: pendingOrders,
    debtPaymentFormMethod: debtPaymentForm.paymentMethod
  });

  // Filter movements based on all filters
  const filteredMovements = (Array.isArray(movements) ? movements : []).filter((movement: any) => {
    const matchesCustomer = customerFilter === "todos_clientes" ||
      (movement.customerName && movement.customerName.toLowerCase().includes(customerFilter.toLowerCase()));
    const matchesVendor = vendorFilter === "todos_vendedores" ||
      (movement.vendorName && movement.vendorName.toLowerCase().includes(vendorFilter.toLowerCase()));
    const matchesPaymentMethod = paymentMethodFilter === "todos_metodos" || movement.subtype === paymentMethodFilter;
    const matchesType = typeFilter === "todos" || movement.type === typeFilter;
    
    return matchesCustomer && matchesVendor && matchesPaymentMethod && matchesType;
  });





  // Mutation for creating expense
  const createExpenseMutation = useMutation({
    mutationFn: async (expenseData: any) => {
      // Get or create current cash register first
      if (!user?.clientId) {
        throw new Error("ID de cliente no disponible");
      }
      
      let cashRegisterResponse;
      try {
        const response = await apiRequest('GET', `/api/cash-register/current?clientId=${user.clientId}`);
        cashRegisterResponse = await response.json();
        console.log('Cash register GET response:', cashRegisterResponse);
      } catch (error) {
        // If no open cash register, create one
        console.log('Creating new cash register');
        const createResponse = await apiRequest('POST', '/api/cash-register/open', {
          clientId: user.clientId,
          initialUsd: 0,
          initialArs: 0,
          initialUsdt: 0
        });
        cashRegisterResponse = await createResponse.json();
        console.log('Cash register POST response:', cashRegisterResponse);
      }
      const cashRegisterId = cashRegisterResponse.id;
      
      if (!cashRegisterId) {
        throw new Error("No se pudo obtener el ID de la caja registradora");
      }
      
      // Validar que el monto no esté vacío
      if (!expenseData.amount || parseFloat(expenseData.amount) <= 0) {
        throw new Error("El monto debe ser mayor a 0");
      }
      
      // Determine currency based on payment method
      let currency = "USD";
      let exchangeRate = 1;
      let amountUsd = parseFloat(expenseData.amount);
      
      if (expenseData.paymentMethod.includes('ars')) {
        currency = "ARS";
        exchangeRate = exchangeRates[expenseData.paymentMethod as keyof typeof exchangeRates]?.usd || 1100;
        amountUsd = parseFloat(expenseData.amount) / exchangeRate; // Convert ARS to USD
      } else if (expenseData.paymentMethod.includes('usdt')) {
        currency = "USDT";
        exchangeRate = exchangeRates[expenseData.paymentMethod as keyof typeof exchangeRates]?.usd || 1;
        amountUsd = parseFloat(expenseData.amount) / exchangeRate; // Convert USDT to USD
      }
      
      const payload = {
        clientId: user.clientId,
        cashRegisterId: cashRegisterId,
        type: 'gasto',
        subtype: expenseData.paymentMethod,
        amount: parseFloat(expenseData.amount).toString(),
        currency: currency,
        exchangeRate: exchangeRate.toString(),
        amountUsd: amountUsd.toString(),
        description: expenseData.description || "Gasto registrado",
        referenceType: 'expense',
        customerId: expenseData.customerName && expenseData.customerName !== "sin_cliente" ? 
          (Array.isArray(customers) ? customers : []).find((c: any) => c.name === expenseData.customerName)?.id : null,
        vendorId: expenseData.vendorName && expenseData.vendorName !== "sin_vendedor" ? 
          (Array.isArray(vendors) ? vendors : []).find((v: any) => v.name === expenseData.vendorName)?.id : null,
        userId: user.id,
        notes: expenseData.notes || null
      };
      return apiRequest('POST', '/api/cash-movements', payload);
    },
    onSuccess: (data, variables) => {
      // Invalidate all related queries for comprehensive sync
      queryClient.invalidateQueries({ queryKey: ['/api/cash-movements'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/current'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/real-time-state'] });
      queryClient.invalidateQueries({ queryKey: ['/api/dashboard/stats'] });
      queryClient.invalidateQueries({ queryKey: ['/api/expenses'] });
      queryClient.invalidateQueries({ queryKey: ['/api/orders'] });
      queryClient.invalidateQueries({ queryKey: ['/api/customer-debts'] });
      
      // Force immediate refetch to update UI across all components
      queryClient.refetchQueries({ queryKey: ['/api/cash-movements'] });
      queryClient.refetchQueries({ queryKey: ['/api/cash-register/real-time-state'] });
      queryClient.refetchQueries({ queryKey: ['/api/orders'] });
      queryClient.refetchQueries({ queryKey: ['/api/dashboard/stats'] });
      
      // Additional delayed sync for complete synchronization
      setTimeout(() => {
        queryClient.refetchQueries({ queryKey: ['/api/cash-register/real-time-state'] });
        queryClient.refetchQueries({ queryKey: ['/api/cash-movements'] });
        queryClient.refetchQueries({ queryKey: ['/api/dashboard/stats'] });
      }, 1000);
      
      setShowExpenseModal(false);
      setExpenseForm({
        description: "",
        amount: "",
        currency: "USD",
        paymentMethod: "efectivo_usd",
        category: "otros",
        customerName: "",
        vendorName: "",
        notes: ""
      });
      // Calculate USD amount for notification based on payment method
      let usdAmount = parseFloat(variables.amount);
      if (variables.paymentMethod.includes('ars')) {
        const exchangeRate = exchangeRates[variables.paymentMethod as keyof typeof exchangeRates]?.usd || 1100;
        usdAmount = parseFloat(variables.amount) / exchangeRate; // Convert ARS to USD
      } else if (variables.paymentMethod.includes('usdt')) {
        const exchangeRate = exchangeRates[variables.paymentMethod as keyof typeof exchangeRates]?.usd || 1;
        usdAmount = parseFloat(variables.amount) / exchangeRate; // Convert USDT to USD
      }
      
      toast({
        title: "✅ Gasto Registrado",
        description: `Gasto de $${usdAmount.toFixed(2)} USD sincronizado automáticamente en toda la plataforma.`,
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

  // Calcular total de todos los métodos de pago seleccionados - CORREGIDO
  const calculateTotalPaymentAmount = () => {
    let total = 0;
    
    selectedPaymentMethods.forEach(method => {
      if (method === 'financiera_usd') {
        total += parseFloat(financieraUSDAmount) || 0;
      } else if (method === 'financiera_ars') {
        const arsAmount = parseFloat(financieraARSAmount) || 0;
        const rate = parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050;
        total += arsAmount / rate; // Convert to USD
      } else {
        const amount = parseFloat(paymentAmounts[method]) || 0;
        if (method === 'efectivo_ars') {
          const rate = parseFloat(paymentExchangeRates['efectivo_ars']) || 1100;
          total += amount / rate; // Convert ARS to USD
        } else if (method === 'transferencia_ars') {
          const rate = parseFloat(paymentExchangeRates['transferencia_ars']) || 1100;
          total += amount / rate; // Convert ARS to USD
        } else if (method === 'transferencia_usdt') {
          const rate = parseFloat(paymentExchangeRates['transferencia_usdt']) || 1100;
          total += amount; // USDT counted as USD for total calculation
        } else {
          // Para métodos USD (efectivo_usd, transferencia_usd)
          total += amount; // Already in USD
        }
      }
    });
    
    return total;
  };

  // Mutation for debt payment using multiple payment methods
  const createDebtPaymentMutation = useMutation({
    mutationFn: async (paymentData: any) => {
      if (!user?.clientId) {
        throw new Error("ID de cliente no disponible");
      }
      
      // Validar que al menos un método de pago esté seleccionado
      if (selectedPaymentMethods.length === 0) {
        throw new Error("Seleccione al menos un método de pago");
      }
      
      // Validar que los montos no estén vacíos
      const totalAmount = calculateTotalPaymentAmount();
      if (totalAmount <= 0) {
        throw new Error("El monto total debe ser mayor a 0");
      }
      
      // Get cash register ID
      let cashRegisterId = 1; // Default fallback
      try {
        const response = await apiRequest('GET', `/api/cash-register/current?clientId=${user.clientId}`);
        const cashRegisterResponse = await response.json();
        cashRegisterId = cashRegisterResponse.id || 1;
      } catch (error) {
        console.log('Using default cash register ID');
      }
      
      // Create debt payments for each payment method
      const results = [];
      
      for (const method of selectedPaymentMethods) {
        let amount = 0;
        let currency = "USD";
        let exchangeRate = 1;
        
        if (method === 'financiera_usd') {
          amount = parseFloat(financieraUSDAmount) || 0;
          currency = "USD";
          exchangeRate = parseFloat(paymentExchangeRates['financiera_usd_ars']) || 1100;
        } else if (method === 'financiera_ars') {
          amount = parseFloat(financieraARSAmount) || 0;
          currency = "ARS";
          exchangeRate = parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050;
        } else {
          amount = parseFloat(paymentAmounts[method]) || 0;
          if (method === 'efectivo_ars') {
            currency = "ARS";
            exchangeRate = parseFloat(paymentExchangeRates['efectivo_ars']) || 1100;
          } else if (method === 'transferencia_ars') {
            currency = "ARS";
            exchangeRate = parseFloat(paymentExchangeRates['transferencia_ars']) || 1100;
          } else if (method === 'transferencia_usdt') {
            currency = "USDT";
            exchangeRate = parseFloat(paymentExchangeRates['transferencia_usdt']) || 1100;
          } else {
            currency = "USD";
          }
        }
        
        if (amount > 0) {
          const payload = {
            clientId: user.clientId,
            cashRegisterId: cashRegisterId,
            orderId: parseInt(paymentData.orderId),
            amount: amount.toString(),
            currency: currency,
            paymentMethod: method,
            exchangeRate: exchangeRate.toString(),
            customerId: paymentData.customerName ? 
              (Array.isArray(customers) ? customers : []).find((c: any) => c.name === paymentData.customerName)?.id : null,
            vendorId: paymentData.vendorName && paymentData.vendorName !== "sin_vendedor" ? 
              (Array.isArray(vendors) ? vendors : []).find((v: any) => v.name === paymentData.vendorName)?.id : null,
            userId: user.id,
            notes: paymentData.notes || null
          };
          
          console.log(`🔄 Sending debt payment payload for ${method}:`, payload);
          
          // CORRECCIÓN: El endpoint /api/debt-payments ya crea el movimiento de caja automáticamente
          // No necesitamos crear movimientos separados aquí
          const result = await apiRequest('POST', '/api/debt-payments', payload);
          results.push(result);
        }
      }
      
      return results;
    },
    onSuccess: (data, variables) => {
      // Invalidate all related queries for comprehensive sync
      queryClient.invalidateQueries({ queryKey: ['/api/cash-movements'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/current'] });
      queryClient.invalidateQueries({ queryKey: ['/api/cash-register/real-time-state'] });
      queryClient.invalidateQueries({ queryKey: ['/api/orders'] });
      queryClient.invalidateQueries({ queryKey: ['/api/dashboard/stats'] });
      queryClient.invalidateQueries({ queryKey: ['/api/customer-debts'] });
      queryClient.invalidateQueries({ queryKey: ['/api/payments'] });
      
      // Force immediate refetch to update UI across all pages  
      queryClient.refetchQueries({ queryKey: ['/api/cash-movements'] });
      queryClient.refetchQueries({ queryKey: ['/api/cash-register/real-time-state'] });
      queryClient.refetchQueries({ queryKey: ['/api/orders'] });
      
      // Invalidate specific order and payment queries for invoice synchronization
      if (variables.orderId) {
        queryClient.invalidateQueries({ queryKey: ['/api/orders', variables.orderId] });
        queryClient.invalidateQueries({ queryKey: ['/api/payments', variables.orderId] });
        queryClient.refetchQueries({ queryKey: ['/api/orders', variables.orderId] });
        queryClient.refetchQueries({ queryKey: ['/api/payments', variables.orderId] });
      }
      
      // Delayed refetch for complete synchronization
      setTimeout(() => {
        queryClient.refetchQueries({ queryKey: ['/api/customer-debts/active'] });
        queryClient.refetchQueries({ queryKey: ['/api/orders'] });
        queryClient.refetchQueries({ queryKey: ['/api/cash-register/real-time-state'] });
      }, 1000);
      setShowDebtPaymentModal(false);
      // Reset all form states
      setDebtPaymentForm({
        orderId: "",
        customerName: "",
        vendorName: "",
        notes: ""
      });
      setSelectedPaymentMethods([]);
      setPaymentAmounts({});
      setFinancieraUSDAmount('');
      setFinancieraARSAmount('');
      
      // Calculate total USD amount for notification
      const totalUsdAmount = calculateTotalPaymentAmount();
      
      toast({
        title: "✅ Pago de Deuda Registrado",
        description: `Pago de $${totalUsdAmount.toFixed(2)} USD aplicado correctamente con múltiples métodos. Sincronizado en toda la plataforma.`,
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

  const paymentMethods = [
    { value: "efectivo_ars", label: "Efectivo ARS" },
    { value: "efectivo_usd", label: "Efectivo USD" },
    { value: "transferencia_ars", label: "Transferencia ARS" },
    { value: "transferencia_usd", label: "Transferencia USD" },
    { value: "transferencia_usdt", label: "Transferencia USDT" },
    { value: "financiera_usd", label: "Financiera USD→ARS" },
    { value: "financiera_ars", label: "Financiera ARS→USD" }
  ];

  const categories = [
    { value: "ventas", label: "Ventas" },
    { value: "alquiler", label: "Alquiler" },
    { value: "servicios", label: "Servicios" },
    { value: "compras", label: "Compras" },
    { value: "sueldos", label: "Sueldos" },
    { value: "marketing", label: "Marketing" },
    { value: "mantenimiento", label: "Mantenimiento" },
    { value: "impuestos", label: "Impuestos" },
    { value: "otros", label: "Otros" }
  ];

  if (loadingMovements) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
        <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
        <div className="flex flex-1">
          <Sidebar />
          <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
            <div className="px-4 sm:px-6 lg:px-8 py-8">
              <div className="flex items-center justify-center h-64">
                <div className="text-gray-500 dark:text-gray-400">Cargando datos de pagos y gastos...</div>
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
              <div>
                <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                  Pagos/Gastos
                </h1>
                <p className="text-gray-600 dark:text-gray-400 mt-1 flex items-center gap-2">
                  Gestión de ingresos, gastos y métodos de pago
                  <span className="flex items-center gap-1 text-green-600 text-sm">
                    <RefreshCw className="h-3 w-3" />
                    Sincronizado con Caja Avanzada
                  </span>
                </p>
              </div>
              
              <div className="flex flex-wrap gap-2">
                <Button 
                  onClick={() => setShowExpenseModal(true)}
                  className="bg-red-600 hover:bg-red-700 text-white"
                >
                  <TrendingDown className="mr-2 h-4 w-4" />
                  Registrar Gasto
                </Button>
                <Button 
                  onClick={() => setShowDebtPaymentModal(true)}
                  className="bg-blue-600 hover:bg-blue-700 text-white"
                >
                  <CreditCard className="mr-2 h-4 w-4" />
                  Pago de Deuda
                </Button>
              </div>
            </div>

            {/* Advanced Filters Section */}
            <div className="mt-8">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <Settings className="mr-2 h-5 w-5" />
                    Filtros Avanzados
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {/* Customer Filter */}
                    <div>
                      <Label className="text-sm font-medium mb-2 block">
                        Filtrar por Cliente
                      </Label>
                      <Select value={customerFilter} onValueChange={setCustomerFilter}>
                        <SelectTrigger>
                          <SelectValue placeholder="Todos los clientes" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="todos_clientes">Todos los clientes</SelectItem>
                          {(Array.isArray(customers) ? customers : []).map((customer: any) => (
                            <SelectItem key={customer.id} value={customer.name}>
                              {customer.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {/* Vendor Filter */}
                    <div>
                      <Label className="text-sm font-medium mb-2 block">
                        Filtrar por Vendedor
                      </Label>
                      <Select value={vendorFilter} onValueChange={setVendorFilter}>
                        <SelectTrigger>
                          <SelectValue placeholder="Todos los vendedores" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="todos_vendedores">Todos los vendedores</SelectItem>
                          {(Array.isArray(vendors) ? vendors : []).map((vendor: any) => (
                            <SelectItem key={vendor.id} value={vendor.name}>
                              {vendor.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {/* Payment Method Filter */}
                    <div>
                      <Label className="text-sm font-medium mb-2 block">
                        Filtrar por Método de Pago
                      </Label>
                      <Select value={paymentMethodFilter} onValueChange={setPaymentMethodFilter}>
                        <SelectTrigger>
                          <SelectValue placeholder="Todos los métodos" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="todos_metodos">Todos los métodos</SelectItem>
                          <SelectItem value="efectivo_ars">Efectivo ARS</SelectItem>
                          <SelectItem value="efectivo_usd">Efectivo USD</SelectItem>
                          <SelectItem value="transferencia_ars">Transferencia ARS</SelectItem>
                          <SelectItem value="transferencia_usd">Transferencia USD</SelectItem>
                          <SelectItem value="transferencia_usdt">Transferencia USDT</SelectItem>
                          <SelectItem value="financiera_usd">Financiera USD→ARS</SelectItem>
                          <SelectItem value="financiera_ars">Financiera ARS→USD</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  
                  {/* Clear Filters Button */}
                  {(customerFilter || vendorFilter || paymentMethodFilter) && (
                    <div className="mt-4 pt-4 border-t">
                      <Button 
                        variant="outline" 
                        onClick={() => {
                          setCustomerFilter("");
                          setVendorFilter("");
                          setPaymentMethodFilter("");
                        }}
                        className="text-sm"
                      >
                        Limpiar Filtros
                      </Button>
                      <span className="ml-3 text-sm text-gray-500 dark:text-gray-400">
                        Mostrando {filteredMovements.length} de {(Array.isArray(movements) ? movements : []).length} movimientos
                      </span>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>

            {/* Deudas Vigentes por Cliente */}
            <div className="mt-8">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <CreditCard className="mr-2 h-5 w-5" />
                    Deudas Vigentes por Cliente
                    <span className="bg-red-100 text-red-800 text-xs font-medium px-2.5 py-0.5 rounded-full ml-2">
                      {(Array.isArray(activeDebts) ? activeDebts : []).length} activas
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {(Array.isArray(activeDebts) ? activeDebts : []).length === 0 ? (
                    <div className="text-center py-8">
                      <CreditCard className="mx-auto h-12 w-12 text-gray-400" />
                      <h3 className="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">
                        No hay deudas vigentes
                      </h3>
                      <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
                        Todos los clientes están al día con sus pagos
                      </p>
                    </div>
                  ) : (
                    <div className="space-y-4">
                      {(Array.isArray(activeDebts) ? activeDebts : []).map((debt: any) => (
                        <Card key={debt.id} className="border-l-4 border-l-red-500">
                          <CardContent className="pt-6">
                            <div className="flex items-center justify-between">
                              <div className="flex-1">
                                <div className="flex items-center space-x-3">
                                  <div className="flex-1">
                                    <p className="text-sm font-medium text-gray-900 dark:text-gray-100">
                                      {debt.customerName}
                                    </p>
                                    <p className="text-sm text-gray-500 dark:text-gray-400">
                                      Orden #{debt.orderId} • {debt.currency}
                                    </p>
                                  </div>
                                  <div className="text-right">
                                    <p className="text-lg font-bold text-red-600 dark:text-red-400">
                                      ${debt.remainingAmount}
                                    </p>
                                    <p className="text-xs text-gray-500 dark:text-gray-400">
                                      de ${debt.debtAmount} total
                                    </p>
                                  </div>
                                </div>
                                <div className="mt-3 flex items-center justify-between">
                                  <div className="flex items-center space-x-4 text-xs text-gray-500 dark:text-gray-400">
                                    <span>
                                      Vence: {new Date(debt.dueDate).toLocaleDateString()}
                                    </span>
                                    <span>
                                      Estado: <span className="text-red-600 font-medium">{debt.status}</span>
                                    </span>
                                  </div>
                                  <Button
                                    size="sm"
                                    onClick={() => {
                                      setDebtPaymentForm({
                                        orderId: debt.orderId.toString(),
                                        customerName: debt.customerName,
                                        vendorName: "",
                                        amount: "",
                                        currency: "USD",
                                        paymentMethod: "efectivo_usd",
                                        notes: ""
                                      });
                                      setShowDebtPaymentModal(true);
                                    }}
                                    className="bg-green-600 hover:bg-green-700 text-white"
                                  >
                                    <DollarSign className="mr-1 h-3 w-3" />
                                    Pagar
                                  </Button>
                                </div>
                                {debt.notes && (
                                  <div className="mt-2 p-2 bg-gray-50 dark:bg-gray-800 rounded text-xs text-gray-600 dark:text-gray-400">
                                    {debt.notes}
                                  </div>
                                )}
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>



            {/* Historial de Pagos y Gastos */}
            <div className="grid grid-cols-1 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <FileText className="mr-2 h-5 w-5" />
                    Historial de Pagos y Gastos
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {/* Advanced Filters */}
                  <div className="mb-6 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                    <h3 className="text-sm font-medium mb-3">Filtros Avanzados</h3>
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                      <div>
                        <Label htmlFor="history-customer-filter">Cliente</Label>
                        <Select value={customerFilter} onValueChange={setCustomerFilter}>
                          <SelectTrigger>
                            <SelectValue placeholder="Todos los clientes" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="todos_clientes">Todos los clientes</SelectItem>
                            {(Array.isArray(customers) ? customers : []).map((customer: any) => (
                              <SelectItem key={customer.id} value={customer.name}>
                                {customer.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="history-vendor-filter">Vendedor</Label>
                        <Select value={vendorFilter} onValueChange={setVendorFilter}>
                          <SelectTrigger>
                            <SelectValue placeholder="Todos los vendedores" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="todos_vendedores">Todos los vendedores</SelectItem>
                            {(Array.isArray(vendors) ? vendors : []).map((vendor: any) => (
                              <SelectItem key={vendor.id} value={vendor.name}>
                                {vendor.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="history-payment-method-filter">Método de Pago</Label>
                        <Select value={paymentMethodFilter} onValueChange={setPaymentMethodFilter}>
                          <SelectTrigger>
                            <SelectValue placeholder="Todos los métodos" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="todos_metodos">Todos los métodos</SelectItem>
                            {paymentMethods.map((method) => (
                              <SelectItem key={method.value} value={method.value}>
                                {method.label}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="history-type-filter">Tipo</Label>
                        <Select value={typeFilter} onValueChange={setTypeFilter}>
                          <SelectTrigger>
                            <SelectValue placeholder="Gastos y Pagos" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="todos">Todos</SelectItem>
                            <SelectItem value="gasto">Solo Gastos</SelectItem>
                            <SelectItem value="pago_deuda">Solo Pagos de Deuda</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                  </div>

                  {/* History Table */}
                  <div className="overflow-x-auto">
                    <table className="w-full border-collapse">
                      <thead>
                        <tr className="border-b border-gray-200 dark:border-gray-700">
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Fecha</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Tipo</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Descripción</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Cliente</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Vendedor</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Método</th>
                          <th className="text-right p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Monto USD</th>
                          <th className="text-left p-3 text-sm font-medium text-gray-600 dark:text-gray-400">Orden</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredMovements.map((movement: any) => (
                          <tr key={movement.id} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800">
                            <td className="p-3 text-sm">
                              {new Date(movement.createdAt).toLocaleDateString('es-ES')}
                            </td>
                            <td className="p-3">
                              <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                                movement.type === 'gasto' 
                                  ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                                  : 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
                              }`}>
                                {movement.type === 'gasto' ? 'Gasto' : 'Pago Deuda'}
                              </span>
                            </td>
                            <td className="p-3 text-sm max-w-xs truncate" title={movement.description}>
                              {movement.description}
                            </td>
                            <td className="p-3 text-sm">
                              {movement.customerName || '-'}
                            </td>
                            <td className="p-3 text-sm">
                              {movement.vendorName || '-'}
                            </td>
                            <td className="p-3 text-sm">
                              {movement.subtype ? movement.subtype.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase()) : '-'}
                            </td>
                            <td className="p-3 text-sm text-right font-medium">
                              ${parseFloat(movement.amountUsd || '0').toFixed(2)}
                            </td>
                            <td className="p-3 text-sm">
                              {movement.referenceId ? `#${movement.referenceId}` : '-'}
                            </td>
                          </tr>
                        ))}
                        {filteredMovements.length === 0 && (
                          <tr>
                            <td colSpan={8} className="p-8 text-center text-gray-500 dark:text-gray-400">
                              No se encontraron registros con los filtros aplicados
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Expense Modal */}
            <Dialog open={showExpenseModal} onOpenChange={setShowExpenseModal}>
              <DialogContent className="max-w-md">
                <DialogHeader>
                  <DialogTitle className="flex items-center">
                    <TrendingDown className="mr-2 h-5 w-5 text-red-600" />
                    Registrar Gasto
                  </DialogTitle>
                </DialogHeader>
                <div className="space-y-4">
                  <div>
                    <Label htmlFor="expense-description">Descripción</Label>
                    <Input
                      id="expense-description"
                      value={expenseForm.description}
                      onChange={(e) => setExpenseForm({...expenseForm, description: e.target.value})}
                      placeholder="Descripción del gasto"
                    />
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="expense-customer">Cliente (Opcional)</Label>
                      <Select value={expenseForm.customerName} onValueChange={(value) => setExpenseForm({...expenseForm, customerName: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar cliente" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="sin_cliente">Sin cliente</SelectItem>
                          {(Array.isArray(customers) ? customers : []).map((customer: any) => (
                            <SelectItem key={customer.id} value={customer.name}>
                              {customer.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="expense-vendor">Vendedor (Opcional)</Label>
                      <Select value={expenseForm.vendorName} onValueChange={(value) => setExpenseForm({...expenseForm, vendorName: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar vendedor" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="sin_vendedor">Sin vendedor</SelectItem>
                          {(Array.isArray(vendors) ? vendors : []).map((vendor: any) => (
                            <SelectItem key={vendor.id} value={vendor.name}>
                              {vendor.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  
                  <div>
                    <Label htmlFor="expense-payment-method">Método de Pago</Label>
                    <Select value={expenseForm.paymentMethod} onValueChange={(value) => setExpenseForm({...expenseForm, paymentMethod: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Seleccionar método" />
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

                  {/* Payment Method Conversion Card for Expenses */}
                  {expenseForm.paymentMethod && (
                    <div className="p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
                      <div className="grid grid-cols-1 gap-3">
                        <div>
                          <Label htmlFor="expense-amount">
                            Monto {expenseForm.paymentMethod.includes('ars') ? 'ARS' : 
                                  expenseForm.paymentMethod.includes('usdt') ? 'USDT' : 'USD'}
                          </Label>
                          <Input
                            id="expense-amount"
                            type="number"
                            step="0.01"
                            value={expenseForm.amount}
                            onChange={(e) => setExpenseForm({...expenseForm, amount: e.target.value})}
                            placeholder="0.00"
                          />
                        </div>
                        
                        {/* Show conversion if not USD */}
                        {expenseForm.paymentMethod.includes('ars') && (
                          <div>
                            <Label>Equivalente USD</Label>
                            <Input
                              value={expenseForm.amount ? 
                                (parseFloat(expenseForm.amount) / (exchangeRates[expenseForm.paymentMethod as keyof typeof exchangeRates]?.usd || 1100)).toFixed(2) : 
                                '0.00'
                              }
                              disabled
                              className="bg-gray-100 dark:bg-gray-700"
                            />
                            <div className="flex justify-between items-center mt-1 text-xs text-gray-600 dark:text-gray-400">
                              <span>TC: ${(exchangeRates[expenseForm.paymentMethod as keyof typeof exchangeRates]?.usd || 1100).toFixed(0)}</span>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-6 px-2 text-xs"
                                onClick={() => {
                                  setCurrentExchangeMethod(expenseForm.paymentMethod);
                                  setShowExchangeRateModal(true);
                                }}
                              >
                                Configurar TC
                              </Button>
                            </div>
                          </div>
                        )}
                        
                        {expenseForm.paymentMethod.includes('usdt') && (
                          <div>
                            <Label>Equivalente USD</Label>
                            <Input
                              value={expenseForm.amount ? 
                                (parseFloat(expenseForm.amount) / (exchangeRates[expenseForm.paymentMethod as keyof typeof exchangeRates]?.usd || 1)).toFixed(2) : 
                                '0.00'
                              }
                              disabled
                              className="bg-gray-100 dark:bg-gray-700"
                            />
                            <div className="flex justify-between items-center mt-1 text-xs text-gray-600 dark:text-gray-400">
                              <span>TC: ${(exchangeRates[expenseForm.paymentMethod as keyof typeof exchangeRates]?.usd || 1).toFixed(4)}</span>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-6 px-2 text-xs"
                                onClick={() => {
                                  setCurrentExchangeMethod(expenseForm.paymentMethod);
                                  setShowExchangeRateModal(true);
                                }}
                              >
                                Configurar TC
                              </Button>
                            </div>
                          </div>
                        )}
                        

                      </div>
                    </div>
                  )}
                  <div>
                    <Label htmlFor="expense-category">Categoría</Label>
                    <Select value={expenseForm.category} onValueChange={(value) => setExpenseForm({...expenseForm, category: value})}>
                      <SelectTrigger>
                        <SelectValue placeholder="Seleccionar categoría" />
                      </SelectTrigger>
                      <SelectContent>
                        {categories.map((category) => (
                          <SelectItem key={category.value} value={category.value}>
                            {category.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label htmlFor="expense-notes">Notas (Opcional)</Label>
                    <Textarea
                      id="expense-notes"
                      value={expenseForm.notes}
                      onChange={(e) => setExpenseForm({...expenseForm, notes: e.target.value})}
                      placeholder="Notas adicionales"
                      rows={3}
                    />
                  </div>
                  <Button 
                    onClick={() => createExpenseMutation.mutate(expenseForm)}
                    disabled={createExpenseMutation.isPending}
                    className="w-full bg-red-600 hover:bg-red-700"
                  >
                    {createExpenseMutation.isPending ? "Registrando..." : "Registrar Gasto"}
                  </Button>
                </div>
              </DialogContent>
            </Dialog>

            {/* Debt Payment Modal */}
            <Dialog open={showDebtPaymentModal} onOpenChange={setShowDebtPaymentModal}>
              <DialogContent className="max-w-5xl max-h-[95vh] overflow-hidden flex flex-col">
                <DialogHeader className="flex-shrink-0 pb-3 border-b border-gray-200 dark:border-gray-700">
                  <DialogTitle className="flex items-center">
                    <CreditCard className="mr-2 h-5 w-5 text-blue-600" />
                    Pago de Deuda
                  </DialogTitle>
                </DialogHeader>
                <div className="flex-1 overflow-y-auto px-1">
                  <div className="space-y-4 py-4">
                  <div>
                    <Label htmlFor="debt-order">Factura/Orden a Pagar</Label>
                    <Select value={debtPaymentForm.orderId} onValueChange={(value) => {
                      const selectedOrder = pendingOrders.find((order: any) => order.id.toString() === value);
                      setDebtPaymentForm({
                        ...debtPaymentForm, 
                        orderId: value,
                        customerName: selectedOrder?.customerName || ""
                      });
                    }}>
                      <SelectTrigger>
                        <SelectValue placeholder="Seleccionar factura pendiente" />
                      </SelectTrigger>
                      <SelectContent>
                        {pendingOrders.map((order: any) => (
                          <SelectItem key={order.id} value={order.id.toString()}>
                            #{order.id} - {order.customerName} - ${order.totalUsd} ({order.paymentStatus})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="debt-customer">Cliente</Label>
                      <Select value={debtPaymentForm.customerName} onValueChange={(value) => setDebtPaymentForm({...debtPaymentForm, customerName: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar cliente" />
                        </SelectTrigger>
                        <SelectContent>
                          {(Array.isArray(customers) ? customers : []).map((customer: any) => (
                            <SelectItem key={customer.id} value={customer.name}>
                              {customer.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label htmlFor="debt-vendor">Vendedor (Opcional)</Label>
                      <Select value={debtPaymentForm.vendorName} onValueChange={(value) => setDebtPaymentForm({...debtPaymentForm, vendorName: value})}>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar vendedor" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="sin_vendedor">Sin vendedor</SelectItem>
                          {(Array.isArray(vendors) ? vendors : []).map((vendor: any) => (
                            <SelectItem key={vendor.id} value={vendor.name}>
                              {vendor.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                    {/* Métodos de Pago Múltiples */}
                    <div>
                      <Label className="text-base font-medium text-gray-900 dark:text-gray-100 mb-3 block">
                        Métodos de Pago:
                      </Label>
                      
                      <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg border border-gray-200 dark:border-gray-700">
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                          {availablePaymentMethods.map((method) => (
                            <div key={method.value} className="border border-gray-300 dark:border-gray-600 rounded-lg p-3 bg-white dark:bg-gray-900">
                            <div className="flex items-center space-x-3 mb-3">
                              <input
                                type="checkbox"
                                id={`debt-payment-${method.value}`}
                                checked={selectedPaymentMethods.includes(method.value)}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setSelectedPaymentMethods(prev => [...prev, method.value]);
                                    setPaymentAmounts(prev => ({ ...prev, [method.value]: '' }));
                                  } else {
                                    setSelectedPaymentMethods(prev => prev.filter(m => m !== method.value));
                                    setPaymentAmounts(prev => {
                                      const newAmounts = { ...prev };
                                      delete newAmounts[method.value];
                                      return newAmounts;
                                    });
                                    if (method.value === 'financiera_usd') {
                                      setFinancieraUSDAmount('');
                                    }
                                    if (method.value === 'financiera_ars') {
                                      setFinancieraARSAmount('');
                                    }
                                  }
                                }}
                                className="rounded h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 dark:border-gray-600"
                              />
                              <label htmlFor={`debt-payment-${method.value}`} className="font-medium text-gray-900 dark:text-gray-100 cursor-pointer">
                                {method.label}
                              </label>
                            </div>
                            
                              {/* Campo de monto para métodos seleccionados */}
                              {selectedPaymentMethods.includes(method.value) && (
                                <div className="mt-2 space-y-2">
                                  {method.value === 'financiera_usd' ? (
                                    // Campos especiales para financiera USD→ARS
                                    <div className="space-y-2">
                                      <div>
                                        <Label className="text-xs text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                        <Input
                                          type="number"
                                          value={financieraUSDAmount}
                                          onChange={(e) => setFinancieraUSDAmount(e.target.value)}
                                          placeholder="0.00"
                                          min="0"
                                          step="0.01"
                                          className="h-8 w-full text-sm"
                                        />
                                      </div>
                                      <div>
                                        <Label className="text-xs text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                        <Input
                                          type="text"
                                          value={`$${calculateARSFromUSDFinanciera(financieraUSDAmount)}`}
                                          readOnly
                                          className="h-8 w-full text-sm bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                        />
                                      </div>
                                      <div className="flex items-center justify-between">
                                        <span className="text-xs text-gray-500 dark:text-gray-400">
                                          TC: ${Math.round(parseFloat(paymentExchangeRates['financiera_usd_ars']) || 1100)}
                                        </span>
                                        <Button
                                          type="button"
                                          variant="outline"
                                          size="sm"
                                          onClick={() => {
                                            const newRate = prompt('Configurar tasa de conversión FINANCIERA USD→ARS:', paymentExchangeRates['financiera_usd_ars']);
                                            if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                              updatePaymentExchangeRate('financiera_usd_ars', newRate);
                                              toast({
                                                title: "Tasa Financiera USD actualizada",
                                                description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                              });
                                            }
                                          }}
                                          className="h-6 px-2 text-xs"
                                        >
                                          TC
                                        </Button>
                                      </div>
                                  </div>
                                  ) : method.value === 'financiera_ars' ? (
                                    // Campos especiales para financiera ARS→USD
                                    <div className="space-y-2">
                                      <div>
                                        <Label className="text-xs text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                        <Input
                                          type="number"
                                          value={financieraARSAmount}
                                          onChange={(e) => setFinancieraARSAmount(e.target.value)}
                                          placeholder="0.00"
                                          min="0"
                                          step="0.01"
                                          className="h-8 w-full text-sm"
                                        />
                                      </div>
                                    <div>
                                      <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                      <Input
                                        type="text"
                                        value={`$${calculateUSDFromARSFinanciera(financieraARSAmount)}`}
                                        readOnly
                                        className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                      />
                                    </div>
                                    <div className="flex items-center justify-between">
                                      <span className="text-xs text-gray-500 dark:text-gray-400">
                                        TC: ${Math.round(parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050)}
                                      </span>
                                      <Button
                                        type="button"
                                        variant="outline"
                                        size="sm"
                                        onClick={() => {
                                          const newRate = prompt('Configurar tasa de conversión FINANCIERA ARS→USD:', paymentExchangeRates['financiera_ars_usd']);
                                          if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                            updatePaymentExchangeRate('financiera_ars_usd', newRate);
                                            toast({
                                              title: "Tasa Financiera ARS actualizada",
                                              description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                            });
                                          }
                                        }}
                                        className="h-7 px-3 text-xs"
                                      >
                                        Configurar TC
                                      </Button>
                                    </div>
                                  </div>
                            ) : method.value === 'efectivo_usd' ? (
                              // Efectivo USD con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['efectivo_usd']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['efectivo_usd']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Efectivo USD→ARS:', paymentExchangeRates['efectivo_usd']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        setPaymentExchangeRates(prev => ({
                                          ...prev,
                                          'efectivo_usd': newRate
                                        }));
                                        toast({
                                          title: "Tasa Efectivo USD actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'efectivo_ars' ? (
                              // Efectivo ARS con conversión a USD
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') / (parseFloat(paymentExchangeRates['efectivo_ars']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['efectivo_ars']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Efectivo ARS→USD:', paymentExchangeRates['efectivo_ars']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        setPaymentExchangeRates(prev => ({
                                          ...prev,
                                          'efectivo_ars': newRate
                                        }));
                                        toast({
                                          title: "Tasa Efectivo ARS actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_usd' ? (
                              // Transferencia USD con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['transferencia_usd']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_usd']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Transferencia USD→ARS:', paymentExchangeRates['transferencia_usd']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        setPaymentExchangeRates(prev => ({
                                          ...prev,
                                          'transferencia_usd': newRate
                                        }));
                                        toast({
                                          title: "Tasa Transferencia USD actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_ars' ? (
                              // Transferencia ARS con conversión a USD
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') / (parseFloat(paymentExchangeRates['transferencia_ars']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_ars']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Transferencia ARS→USD:', paymentExchangeRates['transferencia_ars']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        setPaymentExchangeRates(prev => ({
                                          ...prev,
                                          'transferencia_ars': newRate
                                        }));
                                        toast({
                                          title: "Tasa Transferencia ARS actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_usdt' ? (
                              // Transferencia USDT con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USDT</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['transferencia_usdt']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_usdt'] || '1100'))} (USDT→ARS)
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa USDT→ARS (ej: 1100):', paymentExchangeRates['transferencia_usdt']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        setPaymentExchangeRates(prev => ({
                                          ...prev,
                                          'transferencia_usdt': newRate
                                        }));
                                        toast({
                                          title: "Tasa USDT actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USDT`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : (
                              // Campos estándar para otros métodos
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                              </div>
                            )}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                  <div>
                    <Label htmlFor="debt-notes">Notas (Opcional)</Label>
                    <Textarea
                      id="debt-notes"
                      value={debtPaymentForm.notes}
                      onChange={(e) => setDebtPaymentForm({...debtPaymentForm, notes: e.target.value})}
                      placeholder="Notas adicionales"
                      rows={3}
                    />
                  </div>
                    {/* Resumen de pagos seleccionados */}
                    {selectedPaymentMethods.length > 0 && (
                      <div className="p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                        <h4 className="font-medium text-blue-900 dark:text-blue-100 mb-2 text-sm">
                          Resumen de Pago (Total: ${calculateTotalPaymentAmount().toFixed(2)} USD)
                        </h4>
                      <div className="space-y-1 text-sm text-blue-700 dark:text-blue-300">
                        {selectedPaymentMethods.map(method => {
                          const methodLabel = availablePaymentMethods.find(m => m.value === method)?.label || method;
                          let amount = 0;
                          if (method === 'financiera_usd') {
                            amount = parseFloat(financieraUSDAmount) || 0;
                          } else if (method === 'financiera_ars') {
                            amount = parseFloat(financieraARSAmount) || 0;
                          } else {
                            amount = parseFloat(paymentAmounts[method]) || 0;
                          }
                          
                          if (amount > 0) {
                            return (
                              <div key={method} className="flex justify-between">
                                <span>{methodLabel}:</span>
                                <span>${amount.toFixed(2)} {method.includes('ars') ? 'ARS' : method.includes('usdt') ? 'USDT' : 'USD'}</span>
                              </div>
                            );
                          }
                          return null;
                        })}
                      </div>
                    </div>
                  )}

                    <Button 
                      onClick={() => createDebtPaymentMutation.mutate(debtPaymentForm)}
                      disabled={createDebtPaymentMutation.isPending}
                      className="w-full bg-blue-600 hover:bg-blue-700"
                    >
                      {createDebtPaymentMutation.isPending ? "Registrando..." : "Registrar Pago"}
                    </Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>

            {/* Exchange Rate Configuration Modal */}
            <Dialog open={showExchangeRateModal} onOpenChange={setShowExchangeRateModal}>
              <DialogContent className="max-w-md">
                <DialogHeader>
                  <DialogTitle>Configurar Tipo de Cambio</DialogTitle>
                </DialogHeader>
                <div className="space-y-4">
                  <div>
                    <Label>Método de Pago</Label>
                    <Input 
                      value={currentExchangeMethod}
                      disabled
                      className="bg-gray-100 dark:bg-gray-700"
                    />
                  </div>
                  <div>
                    <Label>Nuevo Tipo de Cambio</Label>
                    <Input 
                      type="number"
                      step="0.0001"
                      placeholder="Ingrese el nuevo TC"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          const newRate = parseFloat((e.target as HTMLInputElement).value);
                          if (newRate > 0) {
                            setExchangeRates(prev => ({
                              ...prev,
                              [currentExchangeMethod]: {
                                ...prev[currentExchangeMethod as keyof typeof prev],
                                usd: currentExchangeMethod.includes('ars') ? newRate : prev[currentExchangeMethod as keyof typeof prev]?.usd,
                                ars: currentExchangeMethod.includes('usd') && !currentExchangeMethod.includes('usdt') ? newRate : prev[currentExchangeMethod as keyof typeof prev]?.ars
                              }
                            }));
                            setShowExchangeRateModal(false);
                            toast({
                              title: "✅ TC Actualizado",
                              description: `Nuevo tipo de cambio configurado: ${newRate}`,
                            });
                          }
                        }
                      }}
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Presiona Enter para confirmar
                    </p>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}